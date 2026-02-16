"""
Demo: LLM-driven full workflow (PMO provision_agent + delegate_async -> principal fork_join -> final report)

This is a simpler alternative to demo_ppa_planning.py:
- It starts from `cmd.sys.pmo.internal.provision_agent` + `delegate_async` (does NOT bypass PMO).
- PMO spawns a principal agent run.
- The principal (LLM) is responsible for: fork_join -> final report -> submit_result.
- The script subscribes (Core NATS) to the principal's streaming chunks and final deliverable output (via evt.agent.*.task).

Usage:
  uv run -m examples.quickstarts.demo_principal_fullflow --project proj_mvp_001 --channel public \\
    --profile-name Principal_Planner_FullFlow \\
    "help me to do a research on k8s"
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import quote

import httpx
from scripts.utils.config import api_base_url, load_toml_config, require_cardbox_dsn
import uuid6

from core.config import PROTOCOL_VERSION
from core.subject import format_subject
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers, next_traceparent
from core.errors import ProtocolViolationError
from core.utp_protocol import Card, JsonContent, TextContent, ToolCallContent, ToolResultContent, extract_json_object
from core.utils import safe_str
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore
from infra.project_bootstrap import ensure_project

REPO_ROOT = Path(__file__).resolve().parents[2]


async def _wait_for_tool_result_inbox(
    *,
    execution_store: ExecutionStore,
    project_id: str,
    agent_id: str,
    tool_call_id: str,
    timeout_s: float,
) -> Dict[str, Any]:
    deadline = asyncio.get_event_loop().time() + timeout_s
    while True:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise TimeoutError("timeout waiting for tool_result inbox")
        rows = await execution_store.list_inbox_by_correlation(
            project_id=project_id,
            agent_id=agent_id,
            correlation_id=tool_call_id,
            limit=20,
        )
        for row in rows:
            if row.get("message_type") != "tool_result":
                continue
            inbox_id = row.get("inbox_id")
            if inbox_id:
                await execution_store.update_inbox_status(
                    inbox_id=inbox_id,
                    project_id=project_id,
                    status="consumed",
                )
            payload = row.get("payload") or {}
            if isinstance(payload, dict):
                return payload
        await asyncio.sleep(min(0.5, remaining))


async def _call_pmo_internal_tool(
    *,
    cardbox: CardBoxClient,
    execution_store: ExecutionStore,
    nats: NATSClient,
    project_id: str,
    channel_id: str,
    caller_agent_id: str,
    tool_name: str,
    tool_args: Dict[str, Any],
    after_execution: str = "suspend",
    trace_id: Optional[str] = None,
) -> Tuple[str, str, Dict[str, Any], Optional[str]]:
    tool_call_id = f"call_{uuid6.uuid7().hex}"
    step_id = f"step_demo_{uuid6.uuid7().hex}"
    agent_turn_id = f"turn_sys_{uuid6.uuid7().hex}"
    trace_id = trace_id or str(uuid6.uuid7())

    pmo_subject = (
        f"cg.{PROTOCOL_VERSION}.{project_id}.{channel_id}.cmd.sys.pmo.internal.{tool_name}"
    )
    tool_call_card = Card(
        card_id=uuid6.uuid7().hex,
        project_id=project_id,
        type="tool.call",
        content=ToolCallContent(
            tool_name=tool_name,
            arguments=tool_args,
            status="called",
            target_subject=pmo_subject,
        ),
        created_at=datetime.now(UTC),
        author_id=caller_agent_id,
        metadata={
            "agent_turn_id": agent_turn_id,
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "role": "assistant",
            "trace_id": trace_id,
        },
        tool_call_id=tool_call_id,
    )
    await cardbox.save_card(tool_call_card)

    payload: Dict[str, Any] = {
        "agent_id": caller_agent_id,
        "agent_turn_id": agent_turn_id,
        "turn_epoch": 1,
        "tool_call_id": tool_call_id,
        "tool_name": tool_name,
        "after_execution": after_execution,
        "tool_call_card_id": tool_call_card.card_id,
        "step_id": step_id,
    }
    headers, _, trace_id = ensure_trace_headers({}, trace_id=trace_id)
    headers = ensure_recursion_depth(headers, default_depth=0)
    traceparent, trace_id = next_traceparent(headers.get("traceparent"), trace_id)
    headers = dict(headers)
    headers["traceparent"] = traceparent

    await execution_store.insert_execution_edge(
        edge_id=f"edge_{uuid6.uuid7().hex}",
        project_id=project_id,
        channel_id=channel_id,
        primitive="enqueue",
        edge_phase="request",
        source_agent_id=caller_agent_id,
        source_agent_turn_id=agent_turn_id,
        source_step_id=step_id,
        target_agent_id="sys.pmo",
        target_agent_turn_id=None,
        correlation_id=tool_call_id,
        enqueue_mode="call",
        recursion_depth=int(headers.get("CG-Recursion-Depth", "0")),
        trace_id=trace_id,
        parent_step_id=step_id,
        metadata={
            "message_type": "tool_call",
            "enqueue_mode": "call",
            "inbox_id": tool_call_id,
            "recursion_depth": int(headers.get("CG-Recursion-Depth", "0")),
        },
    )
    await nats.publish_event(pmo_subject, payload, headers=headers)

    inbox_payload = await _wait_for_tool_result_inbox(
        execution_store=execution_store,
        project_id=project_id,
        agent_id=caller_agent_id,
        tool_call_id=tool_call_id,
        timeout_s=300.0,
    )

    tool_result: Dict[str, Any] = {}
    tool_result_card_id = inbox_payload.get("tool_result_card_id")
    if isinstance(tool_result_card_id, str) and tool_result_card_id:
        cards = await cardbox.get_cards([tool_result_card_id], project_id=project_id)
        tr = cards[0] if cards else None
        if tr:
            content_obj = getattr(tr, "content", None)
            if isinstance(content_obj, ToolResultContent):
                tool_result = {
                    "status": content_obj.status,
                    "after_execution": content_obj.after_execution,
                    "result": content_obj.result,
                    "error": content_obj.error,
                }
            elif isinstance(content_obj, dict):
                tool_result = content_obj
            else:
                tool_result = {"status": "failed", "result": {"error_message": "tool.result content not dict"}}

    return tool_call_id, step_id, tool_result, str(tool_result_card_id) if tool_result_card_id else None


async def _resolve_profile_box_id(*, api_url: str, project_id: str, profile_name: str) -> str:
    encoded = quote(profile_name, safe="")
    async with httpx.AsyncClient(base_url=api_url, timeout=10.0) as client:
        resp = await client.get(f"/projects/{project_id}/profiles/{encoded}")
        if resp.status_code == 404:
            raise RuntimeError(f"profile not found via api: project={project_id} name={profile_name!r}")
        resp.raise_for_status()
        data = resp.json() or {}
        box_id = data.get("profile_box_id")
        if not isinstance(box_id, str) or not box_id:
            raise RuntimeError("profile_box_id missing in api response")
        return box_id


async def _get_profile_json(*, api_url: str, project_id: str, profile_name: str) -> Dict[str, Any]:
    encoded = quote(profile_name, safe="")
    async with httpx.AsyncClient(base_url=api_url, timeout=10.0) as client:
        resp = await client.get(f"/projects/{project_id}/profiles/{encoded}")
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        data = resp.json() or {}
        return data if isinstance(data, dict) else {}


async def _get_tool_json(*, api_url: str, project_id: str, tool_name: str) -> Dict[str, Any]:
    encoded = quote(tool_name, safe="")
    async with httpx.AsyncClient(base_url=api_url, timeout=10.0) as client:
        resp = await client.get(f"/projects/{project_id}/tools/{encoded}")
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        data = resp.json() or {}
        return data if isinstance(data, dict) else {}


async def _bootstrap_project(*, api_url: str, project_id: str) -> None:
    async with httpx.AsyncClient(base_url=api_url, timeout=20.0) as client:
        resp = await client.post(f"/projects/{project_id}/bootstrap")
        if resp.status_code == 404:
            raise RuntimeError(f"project not found via api: {project_id}")
        resp.raise_for_status()


async def _upload_yaml_to_api(*, api_url: str, endpoint: str, yaml_path: Path) -> Dict[str, Any]:
    if not yaml_path.exists():
        raise RuntimeError(f"yaml file not found: {yaml_path}")
    async with httpx.AsyncClient(base_url=api_url, timeout=30.0) as client:
        with yaml_path.open("rb") as f:
            files = {"file": (yaml_path.name, f, "application/x-yaml")}
            resp = await client.post(endpoint, files=files)
        resp.raise_for_status()
        data = resp.json() or {}
        return data if isinstance(data, dict) else {}


async def _ensure_demo_resources(*, api_url: str, project_id: str, tool_yaml: Path) -> None:
    """
    Keep demo projects working even after protocol/schema changes by upserting:
    - internal PMO tools (bootstrap)
    - external web_search tool
    - demo profiles: Principal_Planner_FullFlow + Associate_Search
    """
    await _bootstrap_project(api_url=api_url, project_id=project_id)

    principal_yaml = REPO_ROOT / "examples" / "profiles" / "principal_planner_fullflow.yaml"
    associate_yaml = REPO_ROOT / "examples" / "profiles" / "associate_search.yaml"

    await _upload_yaml_to_api(api_url=api_url, endpoint=f"/projects/{project_id}/tools", yaml_path=tool_yaml)
    await _upload_yaml_to_api(api_url=api_url, endpoint=f"/projects/{project_id}/profiles", yaml_path=principal_yaml)
    await _upload_yaml_to_api(api_url=api_url, endpoint=f"/projects/{project_id}/profiles", yaml_path=associate_yaml)


async def _extract_profile_policy(
    cardbox: CardBoxClient, *, project_id: str, profile_box_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Returns (delegation_policy_dict, sys_profile_card_id).
    """
    box = await cardbox.get_box(profile_box_id, project_id=project_id)
    if not box or not getattr(box, "card_ids", None):
        return None, None
    cards = await cardbox.get_cards(list(box.card_ids), project_id=project_id)
    for c in cards:
        if getattr(c, "type", None) != "sys.profile":
            continue
        try:
            content = extract_json_object(c)
        except ProtocolViolationError:
            return None, str(getattr(c, "card_id", "") or "")
        policy = content.get("delegation_policy")
        return (policy if isinstance(policy, dict) else None), str(getattr(c, "card_id", "") or "")
    return None, None


async def _create_launcher_profile_box(
    cardbox: CardBoxClient,
    *,
    project_id: str,
    target_principal_profile_name: str,
) -> str:
    """
    Create a minimal caller profile (sys.profile) for the launcher agent.
    This profile exists only in CardBox (not necessarily in resource.profiles),
    and provides delegation_policy so PMO provisioning can pass hard checks.
    """
    card_id = uuid6.uuid7().hex
    content = {
        "name": "Demo_Launcher",
        "worker_target": "worker_generic",
        "tags": ["partner"],
        "description": "demo-only launcher profile for examples/quickstarts/demo_principal_fullflow.py",
        "delegation_policy": {
            "target_tags": ["principal"],
            "target_profiles": [str(target_principal_profile_name)],
        },
        "allowed_tools": ["provision_agent", "delegate_async"],
    }
    card = Card(
        card_id=card_id,
        project_id=project_id,
        type="sys.profile",
        content=JsonContent(data=content),
        created_at=datetime.now(UTC),
        author_id="demo",
        metadata={"role": "system", "name": "Demo_Launcher"},
    )
    await cardbox.save_card(card)
    box_id = await cardbox.save_box([card_id], project_id=project_id)
    return str(box_id)


async def _create_context_box(cardbox: CardBoxClient, *, project_id: str, instruction: str) -> str:
    card_id = uuid6.uuid7().hex
    card = Card(
        card_id=card_id,
        project_id=project_id,
        type="task.instruction",
        content=TextContent(text=instruction),
        created_at=datetime.now(UTC),
        author_id="user",
        metadata={"role": "user"},
    )
    await cardbox.save_card(card)
    box_id = await cardbox.save_box([card_id], project_id=project_id)
    return str(box_id)


async def _fetch_step_rows(
    state_store: StateStore,
    *,
    project_id: str,
    agent_turn_id: str,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT step_id, parent_step_id, trace_id
        FROM state.agent_steps
        WHERE project_id=%s AND agent_turn_id=%s
        ORDER BY started_at ASC NULLS LAST
    """
    async with state_store.pool.connection() as conn:
        res = await conn.execute(sql, (project_id, agent_turn_id))
        rows = await res.fetchall()
        return list(rows or [])


def _normalize_trace_id(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return str(value).lower().replace("-", "")


async def _verify_lineage(
    *,
    cardbox: CardBoxClient,
    state_store: StateStore,
    project_id: str,
    child_agent_id: str,
    child_agent_turn_id: str,
    parent_step_id: str,
    trace_id: str,
    output_box_id: Optional[str],
) -> None:
    # 1) state head (active-turn lineage fields are transient and may be cleared when idle)
    head = await state_store.fetch(project_id, child_agent_id)
    if not head:
        raise RuntimeError("lineage check failed: state head missing")

    head_status = safe_str(getattr(head, "status", None)) or ""
    head_parent_step_id = safe_str(getattr(head, "parent_step_id", None))
    head_trace_id = safe_str(getattr(head, "trace_id", None))
    is_active_turn = head_status in {"dispatched", "running", "suspended"}
    if is_active_turn:
        if head_parent_step_id != parent_step_id:
            raise RuntimeError(
                f"lineage check failed: state_head.parent_step_id mismatch "
                f"(expected={parent_step_id} actual={head.parent_step_id})"
            )
        if _normalize_trace_id(head_trace_id) != _normalize_trace_id(trace_id):
            raise RuntimeError(
                f"lineage check failed: state_head.trace_id mismatch "
                f"(expected={trace_id} actual={head.trace_id})"
            )
    else:
        # After turn completion, finish_turn_idle() clears these fields by design.
        if head_parent_step_id and head_parent_step_id != parent_step_id:
            raise RuntimeError(
                f"lineage check failed: state_head.parent_step_id mismatch after idle "
                f"(expected={parent_step_id} actual={head.parent_step_id})"
            )
        if head_trace_id and _normalize_trace_id(head_trace_id) != _normalize_trace_id(trace_id):
            raise RuntimeError(
                f"lineage check failed: state_head.trace_id mismatch after idle "
                f"(expected={trace_id} actual={head.trace_id})"
            )

    # 2) turn rows (retry briefly to allow async writes)
    turns: List[Dict[str, Any]] = []
    for _ in range(10):
        turns = await _fetch_step_rows(state_store, project_id=project_id, agent_turn_id=child_agent_turn_id)
        if turns:
            break
        await asyncio.sleep(0.3)
    if not turns:
        raise RuntimeError("lineage check failed: no turn rows found for child agent_turn_id")

    missing_trace = [t for t in turns if not safe_str(t.get("trace_id"))]
    if missing_trace:
        raise RuntimeError("lineage check failed: some turns missing trace_id")
    mismatch_trace = [
        t
        for t in turns
        if _normalize_trace_id(safe_str(t.get("trace_id"))) != _normalize_trace_id(trace_id)
    ]
    if mismatch_trace:
        raise RuntimeError("lineage check failed: some turns have mismatched trace_id")
    mismatch_parent = [t for t in turns if safe_str(t.get("parent_step_id")) != parent_step_id]
    if mismatch_parent:
        raise RuntimeError("lineage check failed: some turns have mismatched parent_step_id")

    # 3) card metadata in output_box
    if not output_box_id:
        output_box_id = safe_str(head.output_box_id)
    if not output_box_id:
        pointers = await state_store.fetch_pointers(project_id, child_agent_id)
        output_box_id = safe_str(getattr(pointers, "last_output_box_id", None)) if pointers else None
    if not output_box_id:
        raise RuntimeError("lineage check failed: output_box_id missing")

    box = await cardbox.get_box(str(output_box_id), project_id=project_id)
    if not box:
        raise RuntimeError(f"lineage check failed: output_box not found {output_box_id}")
    cards = await cardbox.get_cards(list(box.card_ids or []), project_id=project_id)
    required_types = {
        "agent.thought",
        "tool.call",
        "tool.result",
        "task.deliverable",
        "sys.must_end_with_required",
        "agent.error",
    }
    for c in cards:
        if getattr(c, "type", "") not in required_types:
            continue
        meta = getattr(c, "metadata", {}) or {}
        if _normalize_trace_id(meta.get("trace_id")) != _normalize_trace_id(trace_id):
            raise RuntimeError(
                f"lineage check failed: card {c.card_id} missing/mismatched trace_id"
            )
        if meta.get("parent_step_id") != parent_step_id:
            raise RuntimeError(
                f"lineage check failed: card {c.card_id} missing/mismatched parent_step_id"
            )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Trigger a principal agent to complete the fork_join fullflow demo.")
    parser.add_argument("--project", default="proj_mvp_001")
    parser.add_argument("--channel", default="public")
    parser.add_argument(
        "--start",
        choices=["launch_principal", "direct"],
        default="launch_principal",
        help="Start mode. 'launch_principal' uses PMO (provision_agent + delegate_async); 'direct' dispatches via Inbox + Wakeup.",
    )
    parser.add_argument(
        "--launcher-agent",
        default=None,
        help="caller agent_id for launch_principal (optional). If omitted, a new demo launcher agent_id is generated.",
    )
    parser.add_argument(
        "--agent",
        default=None,
        help="(direct mode) principal agent_id (optional). If omitted, a new demo agent_id is generated and registered in roster.",
    )
    parser.add_argument("--profile-name", default="Principal_Planner_FullFlow")
    parser.add_argument(
        "--search-tool-name",
        default="web_search",
        help="Tool name used by Associate_Search (default: web_search).",
    )
    parser.add_argument(
        "--search-tool-yaml",
        default=None,
        help="Path to tool YAML to upsert (default: examples/tools/web_search_tool.yaml).",
    )
    parser.add_argument("--api-url", default=None)
    parser.add_argument(
        "--no-ensure-resources",
        action="store_true",
        help="Do not auto-upsert demo tool/profile resources into the project before running.",
    )
    parser.add_argument(
        "--skip-lineage-check",
        action="store_true",
        help="Skip verifying trace_id/parent_step_id propagation.",
    )
    parser.add_argument(
        "--ensure-project",
        action="store_true",
        help="Ensure project exists in DB before running.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("instruction")
    args = parser.parse_args()
    if int(args.timeout_seconds) < 120:
        print(
            f"[demo] WARN: timeout={args.timeout_seconds}s may be too short for online fullflow; recommend >= 180s."
        )

    cfg = load_toml_config()
    if not cfg:
        raise RuntimeError("config.toml not found")
    api_url = str(args.api_url or api_base_url(cfg))
    dsn = require_cardbox_dsn(cfg, error="cardbox.postgres_dsn missing in config.toml")

    cardbox = CardBoxClient(config=cfg.get("cardbox", {}))
    await cardbox.init()

    state_store = StateStore(dsn)
    await state_store.open()

    if args.ensure_project:
        print(f"[demo] ensuring project {args.project} exists...")
        await ensure_project(state_store.pool, project_id=str(args.project), title="Demo Project", owner_id="demo_user")

    resource_store = ResourceStore(dsn)
    await resource_store.open()

    execution_store = ExecutionStore(dsn)
    await execution_store.open()

    nats = NATSClient(config=cfg.get("nats", {}))
    await nats.connect()

    done = asyncio.Event()

    project_id = str(args.project)
    channel_id = str(args.channel)
    profile_name = str(args.profile_name)

    search_tool_name = str(args.search_tool_name)
    tool_yaml = (
        Path(args.search_tool_yaml)
        if args.search_tool_yaml
        else (REPO_ROOT / "examples" / "tools" / "web_search_tool.yaml")
    )
    if not args.no_ensure_resources:
        print(f"[demo] ensuring demo resources (bootstrap + {search_tool_name} tool + profiles)...")
        await _ensure_demo_resources(api_url=api_url, project_id=project_id, tool_yaml=tool_yaml)

    # Preflight: make it obvious why web_search/mock_search might not run
    tool_json = await _get_tool_json(api_url=api_url, project_id=project_id, tool_name=search_tool_name)
    if tool_json:
        print(f"[demo] tool {search_tool_name}: ok target_subject={tool_json.get('target_subject')!r}")
    else:
        print(f"[demo] tool {search_tool_name}: MISSING (associate won't be able to call search)")

    # Resolve principal profile box id via API (informational + preflight)
    profile_box_id = await _resolve_profile_box_id(api_url=api_url, project_id=project_id, profile_name=profile_name)
    print(f"[demo] principal profile: name={profile_name!r} box_id={profile_box_id}")

    associate_json = await _get_profile_json(api_url=api_url, project_id=project_id, profile_name="Associate_Search")
    if associate_json.get("profile_box_id"):
        print(f"[demo] associate profile: ok name='Associate_Search' box_id={associate_json.get('profile_box_id')}")
    else:
        print("[demo] associate profile: MISSING (fork_join delegation will be denied: target profile not found)")

    principal_policy, principal_policy_card_id = await _extract_profile_policy(
        cardbox, project_id=project_id, profile_box_id=str(profile_box_id)
    )
    if principal_policy:
        print("[demo] principal delegation_policy:", json.dumps(principal_policy, ensure_ascii=False))
    else:
        print("[demo] principal delegation_policy: MISSING (principal fork_join will be denied unless policy exists).")
        if principal_policy_card_id:
            print(f"[demo] principal sys.profile card_id={principal_policy_card_id} (inspect content for debugging)")

    if associate_json.get("profile_box_id"):
        associate_policy, _associate_policy_card_id = await _extract_profile_policy(
            cardbox, project_id=project_id, profile_box_id=str(associate_json.get("profile_box_id"))
        )
        if associate_policy is None:
            print("[demo] associate delegation_policy: missing (ok; associates typically don't delegate)")

    child_agent_id: Optional[str] = None
    child_agent_turn_id: Optional[str] = None
    trace_id: Optional[str] = None
    parent_step_id: Optional[str] = None
    last_task_payload: Optional[Dict[str, Any]] = None
    launch_tool_result_card_id: Optional[str] = None
    buffered_chunks: list[str] = []

    async def _maybe_flush_buffered_chunks() -> None:
        nonlocal buffered_chunks
        if not child_agent_turn_id or not buffered_chunks:
            return
        for t in buffered_chunks:
            print(t, end="", flush=True)
        buffered_chunks = []

    async def _on_chunk(msg) -> None:
        nonlocal child_agent_turn_id, buffered_chunks
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception:
            return
        arid = data.get("agent_turn_id")
        if not isinstance(arid, str) or not arid:
            return
        if child_agent_turn_id and arid != child_agent_turn_id:
            return
        text = str(data.get("content") or "")
        if not text:
            return
        if child_agent_turn_id:
            print(text, end="", flush=True)
        else:
            # Buffer until we know which principal run we are following.
            buffered_chunks.append(text)
            if sum(len(x) for x in buffered_chunks) > 8000:
                buffered_chunks = buffered_chunks[-50:]

    async def _on_task(msg) -> None:
        nonlocal child_agent_turn_id, last_task_payload
        try:
            data = json.loads(msg.data.decode("utf-8"))
        except Exception:
            return
        arid = data.get("agent_turn_id")
        if child_agent_turn_id and arid != child_agent_turn_id:
            return
        last_task_payload = data
        await _maybe_flush_buffered_chunks()
        print("\n\n[demo] evt.agent.*.task:", json.dumps(data, ensure_ascii=False, indent=2))
        deliverable_card_id = data.get("deliverable_card_id")
        if isinstance(deliverable_card_id, str) and deliverable_card_id.strip():
            cards = await cardbox.get_cards([deliverable_card_id], project_id=project_id)
            deliverable = cards[0] if cards else None
            if deliverable:
                print("\n\n[demo] ✅ deliverable:")
                content = getattr(deliverable, "content", None)
                if isinstance(content, TextContent):
                    print(content.text)
                elif isinstance(content, JsonContent):
                    print(json.dumps(content.data, ensure_ascii=False, indent=2))
                elif isinstance(content, str):
                    print(content)
                else:
                    print(json.dumps(content, ensure_ascii=False, indent=2))
        done.set()

    # Start modes
    if args.start == "launch_principal":
        launcher_agent_id = str(args.launcher_agent) if args.launcher_agent else f"demo_launcher_{uuid6.uuid7().hex[:12]}"
        trace_id = str(uuid6.uuid7())

        # Create a minimal launcher profile with delegation_policy and register in roster.
        launcher_profile_box_id = await _create_launcher_profile_box(
            cardbox, project_id=project_id, target_principal_profile_name=profile_name
        )
        await resource_store.upsert_project_agent(
            project_id=project_id,
            agent_id=launcher_agent_id,
            profile_box_id=str(launcher_profile_box_id),
            worker_target="worker_generic",
            tags=["partner"],
            display_name=launcher_agent_id,
            owner_agent_id=None,
            metadata={"demo": "principal_fullflow", "mode": "launch_principal"},
        )
        print(f"[demo] launcher agent_id: {launcher_agent_id} (roster ok)")

        principal_agent_id = f"principal_{uuid6.uuid7().hex[:12]}"
        provision_args = {
            "action": "create",
            "agent_id": principal_agent_id,
            "profile_name": profile_name,
            "display_name": f"Principal ({profile_name})",
        }
        _, _provision_step_id, provision_result, _ = await _call_pmo_internal_tool(
            cardbox=cardbox,
            execution_store=execution_store,
            nats=nats,
            project_id=project_id,
            channel_id=channel_id,
            caller_agent_id=launcher_agent_id,
            tool_name="provision_agent",
            tool_args=provision_args,
            trace_id=trace_id,
        )
        if (provision_result or {}).get("status") != "success":
            raise RuntimeError(
                f"provision_agent failed: {json.dumps(provision_result, ensure_ascii=False)}"
            )

        dispatch_args = {
            "target_strategy": "reuse",
            "target_ref": principal_agent_id,
            "instruction": str(args.instruction),
        }
        _, dispatch_step_id, dispatch_result, dispatch_tool_result_card_id = await _call_pmo_internal_tool(
            cardbox=cardbox,
            execution_store=execution_store,
            nats=nats,
            project_id=project_id,
            channel_id=channel_id,
            caller_agent_id=launcher_agent_id,
            tool_name="delegate_async",
            tool_args=dispatch_args,
            trace_id=trace_id,
        )
        if (dispatch_result or {}).get("status") != "success":
            raise RuntimeError(
                f"delegate_async failed: {json.dumps(dispatch_result, ensure_ascii=False)}"
            )
        result_obj = (dispatch_result or {}).get("result") or {}
        if not isinstance(result_obj, dict):
            raise RuntimeError(f"delegate_async bad result: {json.dumps(dispatch_result, ensure_ascii=False)}")
        child_agent_id = principal_agent_id
        child_agent_turn_id = safe_str(result_obj.get("agent_turn_id"))
        parent_step_id = dispatch_step_id
        launch_tool_result_card_id = dispatch_tool_result_card_id
        print(f"[demo] ✅ PMO dispatched principal agent_id={child_agent_id}")

        # Subscribe to the spawned principal turn streams/events.
        stream_subject = format_subject(project_id, channel_id, "str", "agent", str(child_agent_id), "chunk")
        task_subject = format_subject(project_id, channel_id, "evt", "agent", str(child_agent_id), "task")
        await nats.subscribe_core(stream_subject, _on_chunk)
        await nats.subscribe_core(task_subject, _on_task)

        if child_agent_turn_id:
            print(f"[demo] following principal agent_turn_id={child_agent_turn_id}")
            await _maybe_flush_buffered_chunks()
        else:
            print("[demo] WARN: delegate_async missing agent_turn_id; streaming filter disabled")

    else:
        # Direct worker dispatch mode (bypasses PMO).
        agent_id = str(args.agent) if args.agent else f"demo_principal_{uuid6.uuid7().hex[:12]}"
        print(f"[demo] direct mode principal agent_id: {agent_id}")

        # Ensure principal is registered in roster (required for delegation_policy enforcement in PMO tools like fork_join).
        await resource_store.upsert_project_agent(
            project_id=project_id,
            agent_id=agent_id,
            profile_box_id=str(profile_box_id),
            worker_target="worker_generic",
            tags=["principal"],
            display_name=agent_id,
            owner_agent_id=None,
            metadata={"demo": "principal_fullflow", "mode": "direct", "profile_name": profile_name},
        )

        # Prepare context/output boxes
        ctx_box_id = await _create_context_box(cardbox, project_id=project_id, instruction=str(args.instruction))
        output_box_id = await cardbox.save_box([], project_id=project_id)
        trace_id = str(uuid6.uuid7())
        parent_step_id = None
        headers, _, trace_id = ensure_trace_headers({}, trace_id=trace_id)
        headers = ensure_recursion_depth(headers, default_depth=0)
        dispatcher = AgentDispatcher(
            resource_store=resource_store,
            state_store=state_store,
            cardbox=cardbox,
            nats=nats,
        )
        dispatch_result = await dispatcher.dispatch(
            DispatchRequest(
                project_id=project_id,
                channel_id=channel_id,
                agent_id=agent_id,
                profile_box_id=str(profile_box_id),
                context_box_id=str(ctx_box_id),
                output_box_id=str(output_box_id),
                trace_id=trace_id,
                headers=headers,
            )
        )
        if dispatch_result.status not in ("accepted", "pending"):
            raise RuntimeError(
                f"dispatch failed (agent_id={agent_id} status={dispatch_result.status})"
            )

        child_agent_id = agent_id
        child_agent_turn_id = dispatch_result.agent_turn_id
        if not child_agent_turn_id:
            raise RuntimeError("dispatch returned missing agent_turn_id")

        # Subscribe streaming + task
        stream_subject = format_subject(project_id, channel_id, "str", "agent", agent_id, "chunk")
        task_subject = format_subject(project_id, channel_id, "evt", "agent", agent_id, "task")
        await nats.subscribe_core(stream_subject, _on_chunk)
        await nats.subscribe_core(task_subject, _on_task)

        print(
            f"[demo] direct turn dispatched agent={agent_id} "
            f"agent_turn_id={child_agent_turn_id} trace={trace_id}"
        )
        print(f"[demo] stream subject: {stream_subject}")

    try:
        await asyncio.wait_for(done.wait(), timeout=float(args.timeout_seconds))
    except asyncio.TimeoutError:
        print(f"\n[demo] timeout after {args.timeout_seconds}s.")
    else:
        if not args.skip_lineage_check and args.start == "launch_principal":
            if not (child_agent_id and child_agent_turn_id and trace_id and parent_step_id):
                raise RuntimeError("lineage check missing required context")
            if launch_tool_result_card_id:
                cards = await cardbox.get_cards([launch_tool_result_card_id], project_id=project_id)
                tr = cards[0] if cards else None
                if not tr:
                    raise RuntimeError("lineage check failed: launch tool.result card missing")
                meta = getattr(tr, "metadata", {}) or {}
                if _normalize_trace_id(meta.get("trace_id")) != _normalize_trace_id(trace_id):
                    raise RuntimeError("lineage check failed: launch tool.result trace_id mismatch")

            output_box_id = None
            if last_task_payload:
                output_box_id = last_task_payload.get("output_box_id")
            await _verify_lineage(
                cardbox=cardbox,
                state_store=state_store,
                project_id=project_id,
                child_agent_id=str(child_agent_id),
                child_agent_turn_id=str(child_agent_turn_id),
                parent_step_id=str(parent_step_id),
                trace_id=str(trace_id),
                output_box_id=output_box_id if isinstance(output_box_id, str) else None,
            )
            print("[demo] ✅ lineage check passed")
    finally:
        await nats.close()
        await execution_store.close()
        await resource_store.close()
        await state_store.close()
        await cardbox.close(timeout=30.0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
