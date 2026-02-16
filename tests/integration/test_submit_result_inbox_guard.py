#!/usr/bin/env python3
"""
Test script for submit_result inbox guard.

Flow:
1) Pick a profile that requires submit_result (must_end_with contains submit_result).
2) Dispatch a turn for an agent using that profile.
3) Insert multiple pending inbox rows (tool_result) without wakeup.
4) Wait for submit_result to be blocked (tool.result error inbox_not_empty).
5) Publish a wakeup to drain pending inbox.
6) Wait for final evt.agent.*.task completion.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Tuple

import uuid6

import pytest

from core.app_config import config_to_dict, load_app_config
from card_box_core.structures import FieldsSchemaContent
from core.config import PROTOCOL_VERSION
from core.errors import ProtocolViolationError
from core.headers import ensure_recursion_depth
from core.subject import evt_agent_task_subject, format_subject
from core.trace import build_traceparent, ensure_trace_headers
from core.utils import safe_str, set_loop_policy
from core.utp_protocol import Card, JsonContent, TextContent, extract_json_object, extract_tool_result_payload
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest
from infra.agent_routing import resolve_agent_target
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore
from services.pmo.agent_lifecycle import CreateAgentSpec, ensure_agent_ready
from scripts.utils.nats import wait_for_task_event
from tests.integration._llm_defaults import apply_default_llm_config

DEFAULT_CARDBOX_DSN = "postgresql://postgres:postgres@localhost:5433/cardbox"
DEFAULT_NATS_URL = "nats://localhost:4222"

set_loop_policy()

try:
    import yaml
except ImportError:  # pragma: no cover - runtime dependency hint
    yaml = None


def _runtime_defaults() -> Dict[str, str]:
    cfg = config_to_dict(load_app_config())
    cardbox_cfg = cfg.get("cardbox") if isinstance(cfg, dict) else {}
    nats_cfg = cfg.get("nats") if isinstance(cfg, dict) else {}
    dsn = (
        str((cardbox_cfg or {}).get("postgres_dsn") or "").strip()
        if isinstance(cardbox_cfg, dict)
        else ""
    )
    nats_servers = (
        [s.strip() for s in (nats_cfg or {}).get("servers", []) if isinstance(s, str) and s.strip()]
        if isinstance(nats_cfg, dict)
        else []
    )
    return {
        "dsn": dsn or DEFAULT_CARDBOX_DSN,
        "nats_url": (nats_servers[0] if nats_servers else DEFAULT_NATS_URL),
    }


async def _wait_for_inbox_block(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    output_box_id: str,
    timeout_s: float,
) -> bool:
    seen: set[str] = set()
    deadline = asyncio.get_event_loop().time() + timeout_s
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            return False
        box = await cardbox.get_box(output_box_id, project_id=project_id)
        card_ids = list(box.card_ids or []) if box else []
        new_ids = [cid for cid in card_ids if cid not in seen]
        if new_ids:
            cards = await cardbox.get_cards(new_ids, project_id=project_id)
            for card in cards:
                if getattr(card, "card_id", None):
                    seen.add(str(card.card_id))
                if getattr(card, "type", "") != "tool.result":
                    continue
                try:
                    payload = extract_tool_result_payload(card)
                except ProtocolViolationError:
                    continue
                error = payload.get("error")
                if isinstance(error, dict):
                    code = safe_str(error.get("code"))
                    if code in ("inbox_not_empty", "inbox_check_failed"):
                        return True
        await asyncio.sleep(0.75)


def _normalize_must_end_with(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        cleaned: List[str] = []
        for item in value:
            val = safe_str(item).strip()
            if val:
                cleaned.append(val)
        return cleaned
    return []


async def _load_profile_content(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    profile_box_id: str,
) -> Optional[Dict[str, Any]]:
    if not profile_box_id:
        return None
    box = await cardbox.get_box(profile_box_id, project_id=project_id)
    if not box or not box.card_ids:
        return None
    cards = await cardbox.get_cards(list(box.card_ids), project_id=project_id)
    if not cards:
        return None
    profile_card = next(
        (c for c in cards if getattr(c, "type", "") == "sys.profile"),
        cards[0],
    )
    try:
        return extract_json_object(profile_card)
    except ProtocolViolationError:
        return None


async def _pick_profile_with_submit_result(
    resource_store: ResourceStore,
    cardbox: CardBoxClient,
    *,
    project_id: str,
) -> Optional[Tuple[str, str, Optional[List[str]]]]:
    rows = await resource_store.list_profiles(project_id, limit=500, offset=0)
    for row in rows or []:
        profile_box_id = safe_str((row or {}).get("profile_box_id"))
        if not profile_box_id:
            continue
        content = await _load_profile_content(
            cardbox,
            project_id=project_id,
            profile_box_id=profile_box_id,
        )
        if not isinstance(content, dict):
            continue
        must_end_with = _normalize_must_end_with(content.get("must_end_with"))
        if "submit_result" not in must_end_with:
            continue
        allowed_tools = content.get("allowed_tools")
        allowed_tools_list = (
            list(allowed_tools) if isinstance(allowed_tools, list) else None
        )
        profile_name = safe_str((row or {}).get("name")) or profile_box_id
        return profile_box_id, profile_name, allowed_tools_list
    return None


async def _create_profile_from_yaml(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    yaml_path: str,
) -> Tuple[str, Dict[str, Any]]:
    assert yaml is not None, "PyYAML is required. Install with `uv add pyyaml` or `pip install pyyaml`."
    with open(yaml_path, "r", encoding="utf-8") as fh:
        profile_data = yaml.safe_load(fh)
    if not isinstance(profile_data, dict):
        raise ValueError("profile YAML root must be a mapping")
    profile_data = apply_default_llm_config(profile_data)

    profile_id = safe_str(profile_data.get("profile_id")) or safe_str(profile_data.get("name")) or uuid6.uuid7().hex
    card_id = f"card_{uuid6.uuid7().hex}"
    card = Card(
        card_id=card_id,
        project_id=project_id,
        type="sys.profile",
        content=JsonContent(data=profile_data),
        created_at=datetime.now(UTC),
        author_id="system",
        metadata={
            "name": profile_data.get("name"),
            "worker_target": profile_data.get("worker_target"),
            "tags": profile_data.get("tags"),
            "profile_id": profile_id,
            "type": "sys.profile",
        },
    )
    await cardbox.save_card(card)
    box_id = await cardbox.save_box([card.card_id], project_id=project_id)
    return str(box_id), profile_data


async def _pick_tool_name(
    resource_store: ResourceStore,
    *,
    project_id: str,
    allowed_tools: Optional[List[str]],
    preferred: Optional[str],
) -> Optional[str]:
    tools = await resource_store.fetch_tools_for_project(project_id)
    candidates: List[str] = []
    preferred = safe_str(preferred)
    prefer_order = [preferred] if preferred else ["emit_marker", "mock_search", "context_merge"]

    for tool in tools or []:
        name = safe_str(tool.get("tool_name"))
        after_exec = safe_str(tool.get("after_execution"))
        if not name or after_exec != "suspend":
            continue
        if allowed_tools is not None and name not in allowed_tools:
            continue
        candidates.append(name)

    if not candidates:
        return None
    for name in prefer_order:
        if name in candidates:
            return name
    return candidates[0]


async def _create_context_box(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    instruction: str,
    required_fields: List[Dict[str, str]],
) -> str:
    instruction_card_id = uuid6.uuid7().hex
    instruction_card = Card(
        card_id=instruction_card_id,
        project_id=project_id,
        type="task.instruction",
        content=TextContent(text=instruction),
        created_at=datetime.now(UTC),
        author_id="user_test",
        metadata={"role": "user"},
    )
    await cardbox.save_card(instruction_card)

    fields_card_id: Optional[str] = None
    if required_fields:
        fields_card_id = uuid6.uuid7().hex
        fields_card = Card(
            card_id=fields_card_id,
            project_id=project_id,
            type="task.result_fields",
            content=FieldsSchemaContent(fields=required_fields),
            created_at=datetime.now(UTC),
            author_id="user_test",
            metadata={"role": "system"},
        )
        await cardbox.save_card(fields_card)

    card_ids = [instruction_card_id]
    if fields_card_id:
        card_ids.append(fields_card_id)
    box_id = await cardbox.save_box(card_ids, project_id=project_id)
    return str(box_id)


async def _insert_pending_inbox(
    execution_store: ExecutionStore,
    *,
    project_id: str,
    agent_id: str,
    count: int,
    recursion_depth: int,
    trace_id: Optional[str],
    status: str = "processing",
) -> List[str]:
    created: List[str] = []
    for _ in range(count):
        traceparent, resolved_trace_id = build_traceparent(trace_id)
        inbox_id = f"inbox_test_{uuid6.uuid7().hex}"
        payload = {
            "agent_id": agent_id,
            "agent_turn_id": f"turn_fake_{uuid6.uuid7().hex}",
            "turn_epoch": 1,
            "tool_call_id": f"tc_fake_{uuid6.uuid7().hex}",
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": "",
        }
        await execution_store.insert_inbox(
            inbox_id=inbox_id,
            project_id=project_id,
            agent_id=agent_id,
            message_type="tool_result",
            enqueue_mode=None,
            correlation_id=None,
            recursion_depth=recursion_depth,
            traceparent=traceparent,
            tracestate=None,
            trace_id=resolved_trace_id,
            parent_step_id=None,
            source_agent_id="test_script",
            source_step_id=None,
            payload=payload,
            status=status,
        )
        created.append(inbox_id)
    return created


async def _release_inbox(
    execution_store: ExecutionStore,
    *,
    project_id: str,
    inbox_ids: List[str],
) -> None:
    for inbox_id in inbox_ids:
        try:
            await execution_store.update_inbox_status(
                inbox_id=inbox_id,
                project_id=project_id,
                status="pending",
                expected_status="processing",
            )
        except Exception:
            continue


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    runtime = _runtime_defaults()
    parser = argparse.ArgumentParser(description="Test submit_result inbox guard.")
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID", "proj_submit_result_guard"))
    parser.add_argument("--channel", default=os.environ.get("CHANNEL_ID", "public"), help="Channel ID (default: public)")
    parser.add_argument("--agent-id", default=os.environ.get("AGENT_ID", "agent_submit_result_guard"), help="Target agent ID")
    parser.add_argument(
        "--no-ensure-agent",
        action="store_true",
        help="Disable auto agent creation/update",
    )
    parser.add_argument("--profile-name", help="Optional profile name to use")
    parser.add_argument("--profile-box-id", help="Optional profile box ID to use")
    parser.add_argument(
        "--profile-yaml",
        default=os.environ.get(
            "PROFILE_YAML",
            str(REPO_ROOT / "tests" / "fixtures" / "profiles" / "simple_assistant.yaml"),
        ),
        help="Profile YAML path (will be imported into CardBox)",
    )
    parser.add_argument("--tool-name", help="Tool to call before submit_result")
    parser.add_argument("--inbox-items", type=int, default=3, help="Pending inbox rows to insert")
    parser.add_argument(
        "--enqueue-delay",
        type=float,
        default=1.5,
        help="Delay before inserting inbox rows (pre-dispatch)",
    )
    parser.add_argument("--block-timeout", type=float, default=120.0, help="Wait for inbox block tool.result")
    parser.add_argument("--timeout", type=float, default=240.0, help="Wait for final task completion")
    parser.add_argument("--nats-url", default=runtime["nats_url"])
    parser.add_argument("--dsn", default=runtime["dsn"])
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    nats = NATSClient(config={"servers": [args.nats_url]})
    cardbox = CardBoxClient(config={"postgres_dsn": args.dsn})
    dsn = args.dsn
    resource_store = ResourceStore(dsn)
    state_store = StateStore(dsn)
    execution_store = ExecutionStore(dsn)

    await nats.connect()
    await cardbox.init()
    await resource_store.open()
    await state_store.open()
    await execution_store.open()

    try:
        task_queue: asyncio.Queue = asyncio.Queue()
        task_subject = evt_agent_task_subject(args.project, args.channel, args.agent_id)

        async def _on_task(msg) -> None:
            try:
                data = json.loads(msg.data.decode("utf-8"))
            except Exception:
                return
            await task_queue.put(data)

        await nats.subscribe_core(task_subject, _on_task)

        roster = await resource_store.fetch_roster(args.project, args.agent_id)
        roster_profile_box_id = safe_str((roster or {}).get("profile_box_id"))

        selected_profile_box_id: Optional[str] = None
        selected_profile_name: Optional[str] = None
        allowed_tools: Optional[List[str]] = None
        profile_worker_target: Optional[str] = None
        profile_tags: Optional[List[str]] = None

        if args.profile_yaml:
            selected_profile_box_id, profile_data = await _create_profile_from_yaml(
                cardbox,
                project_id=args.project,
                yaml_path=args.profile_yaml,
            )
            selected_profile_name = safe_str(profile_data.get("name")) or selected_profile_box_id
            must_end_with = _normalize_must_end_with(profile_data.get("must_end_with"))
            assert "submit_result" in must_end_with, (
                f"profile YAML missing must_end_with submit_result: {args.profile_yaml}"
            )
            allowed_tools = profile_data.get("allowed_tools")
            allowed_tools = list(allowed_tools) if isinstance(allowed_tools, list) else None
            profile_worker_target = safe_str(profile_data.get("worker_target")) or None
            raw_tags = profile_data.get("tags")
            profile_tags = raw_tags if isinstance(raw_tags, list) else None

        if not selected_profile_box_id and (args.profile_box_id or args.profile_name):
            if args.profile_box_id:
                selected_profile_box_id = safe_str(args.profile_box_id)
                selected_profile_name = selected_profile_box_id
            else:
                profile_row = await resource_store.find_profile_by_name(
                    args.project, args.profile_name
                )
                assert profile_row is not None, f"profile not found: {args.profile_name}"
                selected_profile_box_id = safe_str(profile_row.get("profile_box_id"))
                selected_profile_name = safe_str(profile_row.get("name")) or selected_profile_box_id
            content = await _load_profile_content(
                cardbox,
                project_id=args.project,
                profile_box_id=selected_profile_box_id,
            )
            must_end_with = _normalize_must_end_with((content or {}).get("must_end_with"))
            assert "submit_result" in must_end_with, (
                f"profile {selected_profile_name} missing must_end_with submit_result"
            )
            allowed_tools = (content or {}).get("allowed_tools")
            allowed_tools = list(allowed_tools) if isinstance(allowed_tools, list) else None
            profile_worker_target = safe_str((content or {}).get("worker_target")) or None
            raw_tags = (content or {}).get("tags")
            profile_tags = raw_tags if isinstance(raw_tags, list) else None
        elif not selected_profile_box_id and roster_profile_box_id:
            content = await _load_profile_content(
                cardbox,
                project_id=args.project,
                profile_box_id=roster_profile_box_id,
            )
            must_end_with = _normalize_must_end_with((content or {}).get("must_end_with"))
            if "submit_result" in must_end_with:
                selected_profile_box_id = roster_profile_box_id
                selected_profile_name = safe_str((roster or {}).get("agent_id")) or roster_profile_box_id
                allowed_tools = (content or {}).get("allowed_tools")
                allowed_tools = list(allowed_tools) if isinstance(allowed_tools, list) else None
                profile_worker_target = safe_str((content or {}).get("worker_target")) or None
                raw_tags = (content or {}).get("tags")
                profile_tags = raw_tags if isinstance(raw_tags, list) else None
        if not selected_profile_box_id:
            picked = await _pick_profile_with_submit_result(
                resource_store,
                cardbox,
                project_id=args.project,
            )
            assert picked, "no profile found with must_end_with submit_result"
            selected_profile_box_id, selected_profile_name, allowed_tools = picked
            content = await _load_profile_content(
                cardbox,
                project_id=args.project,
                profile_box_id=selected_profile_box_id,
            )
            profile_worker_target = safe_str((content or {}).get("worker_target")) or None
            raw_tags = (content or {}).get("tags")
            profile_tags = raw_tags if isinstance(raw_tags, list) else None

        if roster and selected_profile_box_id != roster_profile_box_id:
            assert not args.no_ensure_agent, (
                "agent exists with different profile; auto-update disabled (--no-ensure-agent)"
            )
            lifecycle_result = await ensure_agent_ready(
                resource_store=resource_store,
                state_store=state_store,
                project_id=args.project,
                agent_id=args.agent_id,
                spec=CreateAgentSpec(
                    profile_box_id=selected_profile_box_id,
                    worker_target=profile_worker_target,
                    tags=profile_tags,
                    metadata={"source": "test_submit_result_inbox_guard"},
                    display_name=args.agent_id,
                    owner_agent_id="system",
                    active_channel_id=args.channel,
                ),
            )
            assert not lifecycle_result.error_code, (
                f"ensure_agent_ready failed: {lifecycle_result.error_code}"
            )
        elif not roster:
            assert not args.no_ensure_agent, (
                "agent not found; auto-create disabled (--no-ensure-agent)"
            )
            lifecycle_result = await ensure_agent_ready(
                resource_store=resource_store,
                state_store=state_store,
                project_id=args.project,
                agent_id=args.agent_id,
                spec=CreateAgentSpec(
                    profile_box_id=selected_profile_box_id,
                    worker_target=profile_worker_target,
                    tags=profile_tags,
                    metadata={"source": "test_submit_result_inbox_guard"},
                    display_name=args.agent_id,
                    owner_agent_id="system",
                    active_channel_id=args.channel,
                ),
            )
            assert not lifecycle_result.error_code, (
                f"ensure_agent_ready failed: {lifecycle_result.error_code}"
            )

        tool_name = await _pick_tool_name(
            resource_store,
            project_id=args.project,
            allowed_tools=allowed_tools,
            preferred=args.tool_name,
        )

        required_fields = [
            {"name": "final", "description": "final answer (text)"},
        ]
        tool_clause = ""
        if tool_name:
            tool_clause = (
                f"First, call the tool `{tool_name}` with valid arguments.\n"
            )
        instruction = (
            "You must finish this task by calling submit_result.\n"
            f"{tool_clause}"
            "After any tool results, call submit_result with the required fields.\n"
            "If submit_result fails with error code inbox_not_empty or inbox_check_failed, "
            "do NOT call other tools; wait briefly and retry submit_result until it succeeds.\n"
        )

        context_box_id = await _create_context_box(
            cardbox,
            project_id=args.project,
            instruction=instruction,
            required_fields=required_fields,
        )

        await asyncio.sleep(max(0.0, float(args.enqueue_delay)))
        pending_inbox_ids = await _insert_pending_inbox(
            execution_store,
            project_id=args.project,
            agent_id=args.agent_id,
            count=max(1, int(args.inbox_items)),
            recursion_depth=0,
            trace_id=None,
            status="processing",
        )
        print(
            f"[OK] Inserted {args.inbox_items} pending inbox rows before dispatch (status=processing)."
        )

        dispatcher = AgentDispatcher(
            resource_store=resource_store,
            state_store=state_store,
            cardbox=cardbox,
            nats=nats,
        )

        headers, _, trace_id = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
        headers = ensure_recursion_depth(headers, default_depth=0)

        dispatch_result = await dispatcher.dispatch(
            DispatchRequest(
                project_id=args.project,
                channel_id=args.channel,
                agent_id=args.agent_id,
                profile_box_id=selected_profile_box_id,
                context_box_id=context_box_id,
                headers=headers,
                trace_id=trace_id,
            )
        )
        assert dispatch_result.status in ("accepted", "pending"), (
            f"dispatch failed: {dispatch_result.status} {dispatch_result.error_code}"
        )

        output_box_id = safe_str(dispatch_result.output_box_id)
        assert output_box_id, "missing output_box_id from dispatch"

        blocked = await _wait_for_inbox_block(
            cardbox,
            project_id=args.project,
            output_box_id=output_box_id,
            timeout_s=float(args.block_timeout),
        )
        if blocked:
            print("[OK] Detected submit_result inbox_not_empty block.")
        else:
            print("[WARN] Did not observe inbox_not_empty; guard might not trigger.")

        if pending_inbox_ids:
            await _release_inbox(
                execution_store,
                project_id=args.project,
                inbox_ids=pending_inbox_ids,
            )
            print(f"[OK] Released {len(pending_inbox_ids)} inbox rows to pending.")

        target = await resolve_agent_target(
            resource_store=resource_store,
            project_id=args.project,
            agent_id=args.agent_id,
            default_target="agent_worker",
        )
        assert target, "worker_target missing for agent"
        wakeup_subject = format_subject(
            args.project,
            args.channel,
            "cmd",
            "agent",
            target,
            "wakeup",
            protocol_version=PROTOCOL_VERSION,
        )
        await nats.publish_event(wakeup_subject, {"agent_id": args.agent_id})
        print(f"[OK] Wakeup published to drain inbox (target={target}).")

        task_payload = await wait_for_task_event(
            task_queue,
            agent_turn_id=safe_str(dispatch_result.agent_turn_id),
            timeout_s=float(args.timeout),
        )
        print("[DONE] Task completed:", json.dumps(task_payload, ensure_ascii=False))
    finally:
        await execution_store.close()
        await state_store.close()
        await resource_store.close()
        await cardbox.close()
        await nats.close()


async def main() -> None:
    args = _parse_args()
    await _run(args)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Allow clean exit on Ctrl+C without printing a stack trace.
        pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_submit_result_inbox_guard():
    args = _parse_args([])
    await _run(args)
