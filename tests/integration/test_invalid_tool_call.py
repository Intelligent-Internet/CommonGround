"""
Test: send a malformed tool.call card and verify ProtocolViolationError handling.

Usage:
  1) Start the tool service, e.g.:
     uv run services/tools/mock_search.py
  2) Run via pytest:
     uv run pytest tests/integration -m integration -k invalid_tool_call
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from datetime import UTC, datetime
from typing import Any, Dict

import nats
import uuid6

import pytest

from core.app_config import config_to_dict, load_app_config
from core.config import PROTOCOL_VERSION
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers
from core.utp_protocol import Card, TextContent
from infra.cardbox_client import CardBoxClient
from infra.primitives import enqueue as enqueue_primitive
from infra.stores import ExecutionStore, ResourceStore, StateStore
DEFAULT_CARDBOX_DSN = "postgresql://postgres:postgres@localhost:5433/cardbox"
DEFAULT_NATS_URL = "nats://localhost:4222"


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
        "protocol_version": PROTOCOL_VERSION,
    }


def _build_parser() -> argparse.ArgumentParser:
    runtime = _runtime_defaults()
    parser = argparse.ArgumentParser(description="Send an invalid tool.call card and await failure.")
    parser.add_argument("--project", default="proj_demo_01")
    parser.add_argument("--channel", default="public")
    parser.add_argument("--agent-id", default="demo_agent")
    parser.add_argument("--agent-turn-id", default="turn_demo_01")
    parser.add_argument("--tool-name", default="search")
    parser.add_argument("--tool-target", default="search")
    parser.add_argument("--tool-suffix", default="mock")
    parser.add_argument("--turn-epoch", type=int, default=1)
    parser.add_argument("--after-execution", default="suspend")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument(
        "--protocol-version",
        default=runtime["protocol_version"],
    )
    parser.add_argument("--nats-url", default=runtime["nats_url"])
    parser.add_argument("--dsn", default=runtime["dsn"])
    parser.add_argument("--worker-target", default=os.environ.get("WORKER_TARGET", "worker_generic"))
    return parser


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return _build_parser().parse_args(argv)


def _resolve_protocol_version(args: argparse.Namespace) -> str:
    if args.protocol_version != PROTOCOL_VERSION:
        raise ValueError(
            "protocol version mismatch: "
            f"test={args.protocol_version!r}, service={PROTOCOL_VERSION!r}. "
            "Use core.config.PROTOCOL_VERSION as single source."
        )
    return PROTOCOL_VERSION


async def _wait_for_result(
    execution_store: ExecutionStore,
    *,
    project_id: str,
    channel_id: str,
    agent_id: str,
    tool_call_id: str,
    timeout_s: float,
) -> Dict[str, Any]:
    _ = channel_id
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


def _format_subject(
    *,
    protocol_version: str,
    project_id: str,
    channel_id: str,
    category: str,
    component: str,
    target: str,
    suffix: str,
) -> str:
    return ".".join(
        [
            "cg",
            protocol_version,
            project_id,
            channel_id,
            category,
            component,
            target,
            suffix,
        ]
    )


async def _ensure_streams(js, protocol_version: str) -> None:
    cmd_stream = f"cg_cmd_{protocol_version}"
    evt_stream = f"cg_evt_{protocol_version}"

    async def _exists(name: str) -> bool:
        try:
            await js.stream_info(name)
            return True
        except Exception:
            return False

    if not await _exists(cmd_stream):
        await js.add_stream(
            name=cmd_stream,
            subjects=[f"cg.{protocol_version}.*.*.cmd.>"],
            max_age=24 * 60 * 60,
        )
    if not await _exists(evt_stream):
        await js.add_stream(
            name=evt_stream,
            subjects=[f"cg.{protocol_version}.*.*.evt.>"],
            max_age=7 * 24 * 60 * 60,
        )


async def _run(args: argparse.Namespace) -> None:
    protocol_version = _resolve_protocol_version(args)
    cardbox = CardBoxClient(config={"postgres_dsn": args.dsn})
    execution_store = ExecutionStore(args.dsn)
    state_store = StateStore(args.dsn)
    resource_store = ResourceStore(args.dsn)

    nc = None
    try:
        nc = await nats.connect(args.nats_url)
        js = nc.jetstream()
        await _ensure_streams(js, protocol_version)
        await cardbox.init()
        await execution_store.open()
        await state_store.open()
        await resource_store.open()

        tool_call_id = f"tc_{uuid6.uuid7().hex}"
        step_id = f"step_demo_{uuid6.uuid7().hex}"

        # Create an invalid tool.call card: content is TextContent instead of ToolCallContent.
        bad_card = Card(
            card_id=uuid6.uuid7().hex,
            project_id=args.project,
            type="tool.call",
            content=TextContent(text="invalid tool.call content"),
            created_at=datetime.now(UTC),
            author_id=args.agent_id,
            metadata={
                "agent_turn_id": args.agent_turn_id,
                "tool_call_id": tool_call_id,
                "step_id": step_id,
                "role": "assistant",
            },
            tool_call_id=tool_call_id,
        )
        await cardbox.save_card(bad_card)

        cmd_subject = _format_subject(
            protocol_version=protocol_version,
            project_id=args.project,
            channel_id=args.channel,
            category="cmd",
            component="tool",
            target=args.tool_target,
            suffix=args.tool_suffix,
        )
        payload = {
            "tool_name": args.tool_name,
            "tool_call_id": tool_call_id,
            "agent_turn_id": args.agent_turn_id,
            "turn_epoch": int(args.turn_epoch),
            "agent_id": args.agent_id,
            "after_execution": args.after_execution,
            "tool_call_card_id": bad_card.card_id,
            "channel_id": args.channel,
        }
        await state_store.init_if_absent(
            project_id=args.project,
            agent_id=args.agent_id,
            active_channel_id=args.channel,
        )
        await resource_store.upsert_project_agent(
            project_id=args.project,
            agent_id=args.agent_id,
            profile_box_id=str(uuid6.uuid7()),
            worker_target=args.worker_target,
            tags=[],
            display_name="Test Agent",
            owner_agent_id=None,
            metadata={"source": "test_invalid_tool_call"},
        )
        headers, _, trace_id = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
        headers = ensure_recursion_depth(headers, default_depth=0)
        async with execution_store.pool.connection() as conn:
            async with conn.transaction():
                enqueue_result = await enqueue_primitive(
                    store=execution_store,
                    project_id=args.project,
                    channel_id=args.channel,
                    target_agent_id=args.agent_id,
                    message_type="tool_call",
                    payload=payload,
                    enqueue_mode="call",
                    correlation_id=tool_call_id,
                    recursion_depth=int(headers.get("CG-Recursion-Depth", "0")),
                    headers=headers,
                    traceparent=headers.get("traceparent"),
                    tracestate=headers.get("tracestate"),
                    trace_id=trace_id,
                    parent_step_id=None,
                    source_agent_id=args.agent_id,
                    source_agent_turn_id=args.agent_turn_id,
                    source_step_id=None,
                    target_agent_turn_id=args.agent_turn_id,
                    conn=conn,
                )
        headers = dict(headers)
        headers["traceparent"] = enqueue_result.traceparent
        final_headers = dict(headers)
        final_headers.setdefault("CG-Timestamp", str(int(time.time() * 1000)))
        final_headers.setdefault("CG-Version", protocol_version)
        final_headers.setdefault("CG-Msg-Type", "Payload")
        final_headers.setdefault("CG-Sender", os.getenv("CG_SENDER", "cg-test"))
        await js.publish(cmd_subject, json.dumps(payload, default=str).encode("utf-8"), headers=final_headers)

        try:
            resume = await _wait_for_result(
                execution_store,
                project_id=args.project,
                channel_id=args.channel,
                agent_id=args.agent_id,
                tool_call_id=tool_call_id,
                timeout_s=args.timeout,
            )
        except TimeoutError as exc:
            raise RuntimeError("Timed out waiting for tool_result.") from exc

        if resume.get("status") != "failed":
            raise AssertionError(f"unexpected tool_result status: {resume.get('status')}")
        tool_result_card_id = resume.get("tool_result_card_id")
        if tool_result_card_id:
            cards = await cardbox.get_cards([str(tool_result_card_id)], project_id=args.project)
            if cards:
                print("tool.result content:", getattr(cards[0], "content", None))
    finally:
        if nc is not None:
            try:
                await nc.drain()
            except Exception:
                # Best-effort cleanup in tests; ignore drain errors.
                pass
        for close_call in (
            resource_store.close,
            state_store.close,
            execution_store.close,
            cardbox.close,
        ):
            try:
                await close_call()
            except Exception:
                # Best-effort cleanup in tests; ignore close errors.
                pass


async def main() -> None:
    args = _parse_args()
    await _run(args)


if __name__ == "__main__":
    asyncio.run(main())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_tool_call():
    args = _parse_args([])
    await _run(args)
