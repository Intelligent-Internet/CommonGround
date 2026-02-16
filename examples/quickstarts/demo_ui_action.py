#!/usr/bin/env python3
"""
Demo: UI action scenarios (chat | busy).

Modes:
- chat: UI action -> delegate_async -> chat agent task
- busy: send two UI actions back-to-back and verify busy behavior
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import httpx
import uuid6

from core.config import PROTOCOL_VERSION
from core.headers import ensure_recursion_depth
from core.time_utils import to_iso, utc_now
from core.trace import ensure_trace_headers
from infra.nats_client import NATSClient
from scripts.utils.api import create_project, upload_profile_yaml, upsert_agent
from scripts.utils.config import load_toml_config

REPO_ROOT = Path(__file__).resolve().parents[2]


async def _subscribe_event(
    nats: NATSClient,
    *,
    subject: str,
    queue_name: str,
    event_queue: asyncio.Queue,
) -> None:
    async def _on_event(_subject: str, data: Dict[str, Any], headers: Dict[str, str]) -> None:
        await event_queue.put((data, headers))

    await nats.subscribe_cmd(
        subject,
        queue_name,
        _on_event,
        durable_name=queue_name,
        deliver_policy="new",
    )


async def _wait_for_event(
    event_queue: asyncio.Queue,
    *,
    timeout_s: float,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    return await asyncio.wait_for(event_queue.get(), timeout=timeout_s)


async def _fetch_card(
    client: httpx.AsyncClient,
    *,
    project_id: str,
    card_id: str,
) -> Dict[str, Any]:
    resp = await client.get(f"/projects/{project_id}/cards/{card_id}")
    if resp.status_code >= 400:
        raise RuntimeError(f"fetch card failed: {resp.status_code} {resp.text}")
    return resp.json()


async def _run_chat(
    *,
    args: argparse.Namespace,
    nats: NATSClient,
    client: httpx.AsyncClient,
) -> int:
    ack_queue: asyncio.Queue = asyncio.Queue()
    task_queue: asyncio.Queue = asyncio.Queue()
    suffix = uuid6.uuid7().hex[:8]

    subject_ack = f"cg.{PROTOCOL_VERSION}.{args.project}.{args.channel}.evt.sys.ui.action_ack"
    subject_task = f"cg.{PROTOCOL_VERSION}.{args.project}.{args.channel}.evt.agent.{args.chat_agent_id}.task"

    await _subscribe_event(
        nats,
        subject=subject_ack,
        queue_name=f"demo_ui_chat_ack_{suffix}",
        event_queue=ack_queue,
    )
    await _subscribe_event(
        nats,
        subject=subject_task,
        queue_name=f"demo_ui_chat_task_{suffix}",
        event_queue=task_queue,
    )

    action_id = uuid6.uuid7().hex
    subject_action = f"cg.{PROTOCOL_VERSION}.{args.project}.{args.channel}.cmd.sys.ui.action"
    payload = {
        "action_id": action_id,
        "agent_id": args.ui_agent_id,
        "tool_name": "delegate_async",
        "args": {
            "target_strategy": "reuse",
            "target_ref": args.chat_agent_id,
            "instruction": args.message,
        },
        "metadata": {
            "source": "demo_ui_action",
            "client_ts": to_iso(utc_now()),
        },
    }
    headers, _, _ = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
    headers = ensure_recursion_depth(headers, default_depth=0)
    await nats.publish_event(subject_action, payload, headers=headers)

    start = time.monotonic()
    ack_payload, _ = await _wait_for_event(ack_queue, timeout_s=args.timeout)
    print("[ack]", ack_payload)

    ack_status = str(ack_payload.get("status") or "")
    if ack_status not in ("accepted", "pending", "done"):
        return 1

    elapsed = time.monotonic() - start
    remaining = max(1.0, args.timeout - elapsed)
    task_payload, _ = await _wait_for_event(task_queue, timeout_s=remaining)
    print("[task]", task_payload)

    if args.fetch_deliverable:
        deliverable_id = str(task_payload.get("deliverable_card_id") or "")
        if deliverable_id:
            card = await _fetch_card(client, project_id=args.project, card_id=deliverable_id)
            content = card.get("content") or {}
            print("[deliverable]", content)

    return 0


async def _run_busy(
    *,
    args: argparse.Namespace,
    nats: NATSClient,
) -> int:
    ack_queue: asyncio.Queue = asyncio.Queue()
    suffix = uuid6.uuid7().hex[:8]
    subject_ack = f"cg.{PROTOCOL_VERSION}.{args.project}.{args.channel}.evt.sys.ui.action_ack"
    await _subscribe_event(
        nats,
        subject=subject_ack,
        queue_name=f"demo_ui_busy_ack_{suffix}",
        event_queue=ack_queue,
    )

    subject_action = f"cg.{PROTOCOL_VERSION}.{args.project}.{args.channel}.cmd.sys.ui.action"
    action_id_1 = uuid6.uuid7().hex
    action_id_2 = uuid6.uuid7().hex
    payload_base = {
        "agent_id": args.ui_agent_id,
        "tool_name": "delegate_async",
        "args": {
            "target_strategy": "reuse",
            "target_ref": args.chat_agent_id,
            "instruction": args.message,
        },
        "metadata": {
            "source": "demo_ui_action_busy",
            "client_ts": to_iso(utc_now()),
        },
    }

    headers, _, _ = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
    headers = ensure_recursion_depth(headers, default_depth=0)

    payload1 = dict(payload_base)
    payload1["action_id"] = action_id_1

    payload2 = dict(payload_base)
    payload2["action_id"] = action_id_2

    await nats.publish_event(subject_action, payload1, headers=headers)
    await asyncio.sleep(float(args.delay))
    await nats.publish_event(subject_action, payload2, headers=headers)

    ack_by_id: Dict[str, Dict[str, Any]] = {}
    deadline = time.monotonic() + float(args.timeout)
    while time.monotonic() < deadline and len(ack_by_id) < 2:
        remaining = max(0.1, deadline - time.monotonic())
        try:
            ack_payload, _ = await _wait_for_event(ack_queue, timeout_s=remaining)
        except asyncio.TimeoutError:
            break
        ack_id = str(ack_payload.get("action_id") or "")
        if ack_id in (action_id_1, action_id_2):
            ack_by_id[ack_id] = ack_payload

    print("[ack-1]", ack_by_id.get(action_id_1))
    print("[ack-2]", ack_by_id.get(action_id_2))

    if action_id_2 not in ack_by_id:
        return 2
    status2 = str(ack_by_id[action_id_2].get("status") or "")
    if args.expect_busy and status2 != "busy":
        return 1
    return 0


async def _run_demo(args: argparse.Namespace) -> int:
    cfg = load_toml_config()
    nats_cfg = cfg.get("nats", {}) if isinstance(cfg, dict) else {}

    ui_profile_path = Path(args.ui_profile).resolve()
    chat_profile_path = Path(args.chat_profile).resolve()

    async with httpx.AsyncClient(base_url=args.api_url, timeout=20.0) as client:
        if args.ensure_project:
            await create_project(
                client,
                project_id=args.project,
                title=args.project_title,
                owner_id=args.owner_id,
                bootstrap=args.bootstrap,
            )

        ui_profile_name = await upload_profile_yaml(
            client, project_id=args.project, yaml_path=ui_profile_path
        )
        chat_profile_name = await upload_profile_yaml(
            client, project_id=args.project, yaml_path=chat_profile_path
        )

        await upsert_agent(
            client,
            project_id=args.project,
            channel_id=args.channel,
            agent_id=args.ui_agent_id,
            profile_name=ui_profile_name,
            worker_target="ui_worker",
            tags=["ui"],
            display_name=args.ui_display_name,
            owner_agent_id=args.owner_id,
            metadata={"is_ui_agent": True},
        )
        await upsert_agent(
            client,
            project_id=args.project,
            channel_id=args.channel,
            agent_id=args.chat_agent_id,
            profile_name=chat_profile_name,
            worker_target="worker_generic",
            tags=["partner"],
            display_name=args.chat_display_name,
            owner_agent_id=args.owner_id,
            metadata={},
        )

        nats = NATSClient(config=nats_cfg)
        await nats.connect()

        try:
            if args.mode == "busy":
                return await _run_busy(args=args, nats=nats)
            return await _run_chat(args=args, nats=nats, client=client)
        finally:
            await nats.close()


def _parse_args(argv: list[str] | None = None, *, default_mode: str = "chat") -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo: UI action scenarios")
    parser.add_argument("--mode", choices=("chat", "busy"), default=default_mode)
    parser.add_argument("--api-url", default=os.environ.get("API_URL", "http://localhost:8099"))
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID", "proj_ui_chat_01"))
    parser.add_argument("--project-title", default=os.environ.get("PROJECT_TITLE", "UI Demo"))
    parser.add_argument("--owner-id", default=os.environ.get("OWNER_ID", "user_demo"))
    parser.add_argument("--channel", default=os.environ.get("CHANNEL_ID", "public"))
    parser.add_argument("--ui-agent-id", default=os.environ.get("UI_AGENT_ID", "ui_user_01"))
    parser.add_argument("--chat-agent-id", default=os.environ.get("CHAT_AGENT_ID", "chat_agent_ui"))
    parser.add_argument("--ui-display-name", default="UI Session")
    parser.add_argument("--chat-display-name", default="Chat Agent")
    parser.add_argument(
        "--ui-profile",
        default=str(REPO_ROOT / "examples/profiles/ui.yaml"),
        help="path to UI profile yaml",
    )
    parser.add_argument(
        "--chat-profile",
        default=str(REPO_ROOT / "examples/profiles/chat_assistant.yaml"),
        help="path to chat profile yaml",
    )
    parser.add_argument("--message", default="hello chat via ui")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--fetch-deliverable", action="store_true")
    parser.add_argument("--delay", type=float, default=0.1, help="delay before 2nd action")
    parser.add_argument("--expect-busy", action="store_true", help="assert 2nd action busy")
    parser.add_argument("--no-ensure-project", dest="ensure_project", action="store_false")
    parser.set_defaults(ensure_project=True)
    parser.add_argument("--no-bootstrap", dest="bootstrap", action="store_false")
    parser.set_defaults(bootstrap=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, *, default_mode: str = "chat") -> None:
    args = _parse_args(argv, default_mode=default_mode)
    try:
        exit_code = asyncio.run(_run_demo(args))
    except asyncio.TimeoutError:
        print("[error] timeout waiting for events")
        raise SystemExit(2)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
