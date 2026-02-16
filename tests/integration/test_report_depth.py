#!/usr/bin/env python3
"""
Integration check for Report recursion_depth inheritance.

What it verifies:
1) tool_call request edge is written to execution_edges (primitive=tool_call, edge_phase=request)
2) Report(tool_result/timeout) inherits recursion_depth from the request edge (require_match=True)

Prereqs:
- Postgres + NATS running
- config.toml configured or pass PG_DSN / NATS_URL via env
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import uuid6

import pytest

from core.config import PROTOCOL_VERSION
from core.app_config import load_app_config, config_to_dict
from core.cg_context import CGContext
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers, trace_id_from_headers
from core.utils import set_loop_policy
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.primitives import report as report_primitive
from infra.stores import ExecutionStore, ResourceStore
from infra.stores.tool_store import ToolStore
from services.agent_worker.utp_dispatcher import UTPDispatcher
from scripts.utils.config import DEFAULT_CARDBOX_DSN, resolve_cardbox_dsn

set_loop_policy()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Integration check for Report recursion_depth.")
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID", "proj_report_depth_01"), help="Project ID")
    parser.add_argument("--channel", default="public", help="Channel ID (default: public)")
    parser.add_argument("--agent-id", default="agent_integration", help="Agent ID")
    parser.add_argument("--tool-name", default="integration_dummy", help="Tool name")
    parser.add_argument("--tool-target", default="integration_dummy", help="Tool subject target name")
    parser.add_argument("--tool-suffix", default="call", help="Tool subject suffix")
    parser.add_argument("--depth", type=int, default=1, help="Recursion depth to use")
    return parser


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return _build_parser().parse_args(argv)


async def _ensure_tool_definition(
    resource_store: ResourceStore,
    *,
    project_id: str,
    tool_name: str,
    target_subject: str,
) -> None:
    tool_store = ToolStore(resource_store.pool)
    await tool_store.upsert_tool(
        project_id=project_id,
        tool_name=tool_name,
        description="integration dummy tool",
        target_subject=target_subject,
        after_execution="suspend",
        parameters={"type": "object", "properties": {}, "required": []},
        options={},
    )


async def _fetch_request_depth(
    execution_store: ExecutionStore,
    *,
    project_id: str,
    correlation_id: str,
    primitive: str,
) -> Optional[int]:
    sql = """
        SELECT recursion_depth
        FROM state.execution_edges
        WHERE project_id=%s
          AND correlation_id=%s
          AND primitive=%s
          AND edge_phase='request'
        ORDER BY created_at ASC
        LIMIT 1
    """
    async with execution_store.pool.connection() as conn:
        res = await conn.execute(sql, (project_id, correlation_id, primitive))
        row = await res.fetchone()
        if not row:
            return None
        depth = row.get("recursion_depth")
        return int(depth) if depth is not None else None


async def _fetch_inbox_depth(
    execution_store: ExecutionStore,
    *,
    project_id: str,
    agent_id: str,
    correlation_id: str,
    message_type: str,
) -> Optional[int]:
    sql = """
        SELECT recursion_depth
        FROM state.agent_inbox
        WHERE project_id=%s
          AND agent_id=%s
          AND correlation_id=%s
          AND message_type=%s
        ORDER BY created_at DESC
        LIMIT 1
    """
    async with execution_store.pool.connection() as conn:
        res = await conn.execute(sql, (project_id, agent_id, correlation_id, message_type))
        row = await res.fetchone()
        if not row:
            return None
        depth = row.get("recursion_depth")
        return int(depth) if depth is not None else None


async def _run_single_check(
    *,
    dispatcher: UTPDispatcher,
    execution_store: ExecutionStore,
    project_id: str,
    channel_id: str,
    agent_id: str,
    tool_name: str,
    message_type: str,
    headers: Dict[str, str],
    depth: int,
) -> None:
    tool_call_id = f"tc_{uuid6.uuid7().hex}"
    step_id = f"step_{uuid6.uuid7().hex}"
    agent_turn_id = f"turn_{uuid6.uuid7().hex}"

    tool_call = {
        "id": tool_call_id,
        "type": "function",
        "function": {"name": tool_name, "arguments": {}},
    }

    await dispatcher.execute(
        tool_call=tool_call,
        ctx=CGContext(
            project_id=project_id,
            channel_id=channel_id,
            agent_id=agent_id,
            agent_turn_id=agent_turn_id,
            trace_id=trace_id_from_headers(headers),
            headers=dict(headers or {}),
            step_id=step_id,
        ),
        turn_epoch=1,
        context_box_id=None,
    )

    request_depth = await _fetch_request_depth(
        execution_store,
        project_id=project_id,
        correlation_id=tool_call_id,
        primitive="tool_call",
    )
    assert request_depth is not None, f"missing tool_call request edge for {tool_call_id}"
    assert request_depth == depth, (
        f"tool_call request depth mismatch (expected {depth}, got {request_depth})"
    )

    payload = {
        "tool_call_id": tool_call_id,
        "agent_id": agent_id,
        "agent_turn_id": agent_turn_id,
        "turn_epoch": 1,
        "step_id": step_id,
        "tool_result_card_id": f"card_{uuid6.uuid7().hex}",
    }
    async with execution_store.pool.connection() as conn:
        async with conn.transaction():
            await report_primitive(
                store=execution_store,
                project_id=project_id,
                channel_id=channel_id,
                target_agent_id=agent_id,
                message_type=message_type,
                payload=payload,
                correlation_id=tool_call_id,
                headers=headers,
                traceparent=headers.get("traceparent"),
                tracestate=headers.get("tracestate"),
                trace_id=trace_id_from_headers(headers),
                parent_step_id=step_id,
                source_agent_id="sys.integration",
                conn=conn,
            )

    inbox_depth = await _fetch_inbox_depth(
        execution_store,
        project_id=project_id,
        agent_id=agent_id,
        correlation_id=tool_call_id,
        message_type=message_type,
    )
    assert inbox_depth is not None, f"missing inbox row for {message_type} {tool_call_id}"
    assert inbox_depth == depth, (
        f"{message_type} inbox depth mismatch (expected {depth}, got {inbox_depth})"
    )

    print(
        f"[OK] {message_type} inherits recursion_depth={depth} (tool_call_id={tool_call_id})"
    )


async def _run(args: argparse.Namespace) -> None:
    cfg = config_to_dict(load_app_config())
    nats = NATSClient(config=cfg.get("nats", {}))
    cardbox = CardBoxClient(config=cfg.get("cardbox", {}))
    dsn, _ = resolve_cardbox_dsn(cfg, default=DEFAULT_CARDBOX_DSN)

    resource_store = ResourceStore(dsn)
    execution_store = ExecutionStore(dsn)

    await nats.connect()
    await cardbox.init()
    await resource_store.open()
    await execution_store.open()

    try:
        target_subject = (
            f"cg.{PROTOCOL_VERSION}.{{project_id}}.{{channel_id}}.cmd.tool."
            f"{args.tool_target}.{args.tool_suffix}"
        )
        await _ensure_tool_definition(
            resource_store,
            project_id=args.project,
            tool_name=args.tool_name,
            target_subject=target_subject,
        )

        dispatcher = UTPDispatcher(
            cardbox,
            nats,
            resource_store,
            execution_store=execution_store,
        )

        headers, _, _ = ensure_trace_headers({}, trace_id=str(uuid6.uuid7()))
        headers = ensure_recursion_depth(headers, default_depth=int(args.depth))

        await _run_single_check(
            dispatcher=dispatcher,
            execution_store=execution_store,
            project_id=args.project,
            channel_id=args.channel,
            agent_id=args.agent_id,
            tool_name=args.tool_name,
            message_type="tool_result",
            headers=headers,
            depth=int(args.depth),
        )
        await _run_single_check(
            dispatcher=dispatcher,
            execution_store=execution_store,
            project_id=args.project,
            channel_id=args.channel,
            agent_id=args.agent_id,
            tool_name=args.tool_name,
            message_type="timeout",
            headers=headers,
            depth=int(args.depth),
        )
    finally:
        await resource_store.close()
        await execution_store.close()
        await cardbox.close()
        await nats.close()


async def main() -> None:
    args = _parse_args()
    await _run(args)


if __name__ == "__main__":
    asyncio.run(main())


@pytest.mark.integration
@pytest.mark.asyncio
async def test_report_depth():
    args = _parse_args([])
    await _run(args)
