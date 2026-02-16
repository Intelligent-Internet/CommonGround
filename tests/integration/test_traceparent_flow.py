#!/usr/bin/env python3
"""
Verify traceparent behavior:
1) report_primitive returns a traceparent that is stored in inbox.
2) publish_event respects caller-provided traceparent (no overwrite).
3) publish_event injects traceparent when missing.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import asyncio
import os
from typing import Any, Dict, Optional

import nats
import uuid6

import pytest

from core.config import PROTOCOL_VERSION
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers
from infra.nats_client import NATSClient
from infra.primitives import enqueue as enqueue_primitive
from infra.primitives import report as report_primitive
from infra.stores import ExecutionStore

DEFAULT_CARDBOX_DSN = "postgresql://postgres:postgres@localhost:5433/cardbox"


async def _subscribe_once(url: str, subject: str) -> Dict[str, str]:
    nc = await nats.connect(url)
    fut: asyncio.Future[Dict[str, str]] = asyncio.get_running_loop().create_future()

    async def _handler(msg):
        headers = dict(msg.headers or {})
        if not fut.done():
            fut.set_result(headers)

    sub = await nc.subscribe(subject, cb=_handler)
    headers = await asyncio.wait_for(fut, timeout=5.0)
    await sub.unsubscribe()
    await nc.drain()
    return headers


async def _publish_and_capture(
    nats_client: NATSClient,
    nats_url: str,
    subject: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    capture_task = asyncio.create_task(_subscribe_once(nats_url, subject))
    await asyncio.sleep(0.05)  # ensure subscriber is ready
    await nats_client.publish_event(subject, payload, headers=headers)
    return await capture_task


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


async def _run(args: argparse.Namespace) -> int:
    nats_client = NATSClient(config={"servers": [args.nats_url]})
    execution_store = ExecutionStore(args.dsn)
    await nats_client.connect()
    await execution_store.open()
    protocol_version = _resolve_protocol_version(args)

    subject = (
        f"cg.{protocol_version}.{args.project}.{args.channel}.cmd.tool.traceparent_test.call"
    )

    try:
        headers, _, trace_id = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
        headers = ensure_recursion_depth(headers, default_depth=0)
        correlation_id = f"tc_{uuid6.uuid7().hex}"
        payload = {
            "tool_call_id": correlation_id,
            "agent_id": args.agent_id,
            "agent_turn_id": f"turn_{uuid6.uuid7().hex}",
            "turn_epoch": 1,
            "step_id": f"step_{uuid6.uuid7().hex}",
            "tool_result_card_id": f"card_{uuid6.uuid7().hex}",
        }
        enqueue_payload = {
            "tool_call_id": correlation_id,
            "agent_id": args.agent_id,
            "agent_turn_id": payload["agent_turn_id"],
            "turn_epoch": payload["turn_epoch"],
            "step_id": payload["step_id"],
        }
        async with execution_store.pool.connection() as conn:
            async with conn.transaction():
                enqueue_result = await enqueue_primitive(
                    store=execution_store,
                    project_id=args.project,
                    channel_id=args.channel,
                    target_agent_id=args.agent_id,
                    message_type="tool_call",
                    payload=enqueue_payload,
                    enqueue_mode="call",
                    correlation_id=correlation_id,
                    recursion_depth=int(headers.get("CG-Recursion-Depth", "0")),
                    headers=headers,
                    traceparent=headers.get("traceparent"),
                    tracestate=headers.get("tracestate"),
                    trace_id=trace_id,
                    parent_step_id=enqueue_payload["step_id"],
                    source_agent_id="sys.test_traceparent_flow",
                    source_agent_turn_id=None,
                    source_step_id=enqueue_payload["step_id"],
                    target_agent_turn_id=enqueue_payload["agent_turn_id"],
                    conn=conn,
                )
        headers = dict(headers)
        headers["traceparent"] = enqueue_result.traceparent

        async with execution_store.pool.connection() as conn:
            async with conn.transaction():
                report_result = await report_primitive(
                    store=execution_store,
                    project_id=args.project,
                    channel_id=args.channel,
                    target_agent_id=args.agent_id,
                    message_type="tool_result",
                    payload=payload,
                    correlation_id=correlation_id,
                    headers=headers,
                    traceparent=headers.get("traceparent"),
                    tracestate=headers.get("tracestate"),
                    trace_id=trace_id,
                    parent_step_id=payload["step_id"],
                    source_agent_id="sys.test_traceparent_flow",
                    conn=conn,
                )

        rows = await execution_store.list_inbox_by_correlation(
            project_id=args.project,
            agent_id=args.agent_id,
            correlation_id=correlation_id,
            limit=10,
        )
        inbox_traceparent = rows[0].get("traceparent") if rows else None
        assert inbox_traceparent == report_result.traceparent, (
            f"traceparent mismatch: inbox={inbox_traceparent} report={report_result.traceparent}"
        )

        # publish_event should respect provided traceparent
        headers = dict(headers)
        headers["traceparent"] = report_result.traceparent
        captured = await _publish_and_capture(
            nats_client,
            args.nats_url,
            subject,
            payload,
            headers=headers,
        )
        sent_traceparent = captured.get("traceparent")
        assert sent_traceparent == report_result.traceparent, (
            f"publish_event overwrote traceparent: sent={sent_traceparent} expected={report_result.traceparent}"
        )

        # publish_event should inject traceparent when missing
        captured_missing = await _publish_and_capture(
            nats_client,
            args.nats_url,
            subject,
            payload,
            headers=ensure_recursion_depth({}, default_depth=0),
        )
        assert captured_missing.get("traceparent"), (
            "publish_event failed to inject traceparent when missing"
        )

        print("[OK] traceparent persisted and respected by publish_event")
        return 0
    finally:
        await execution_store.close()
        await nats_client.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate traceparent propagation")
    parser.add_argument("--project", default="proj_traceparent_01")
    parser.add_argument("--channel", default="public")
    parser.add_argument("--agent-id", default="agent_traceparent_test")
    parser.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    parser.add_argument("--dsn", default=os.environ.get("PG_DSN", DEFAULT_CARDBOX_DSN))
    parser.add_argument("--protocol-version", default=PROTOCOL_VERSION)
    return parser


async def main() -> int:
    args = _parse_args()
    return await _run(args)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_traceparent_flow():
    args = _parse_args([])
    exit_code = await _run(args)
    if exit_code:
        raise SystemExit(exit_code)
