#!/usr/bin/env python3
"""
Test: /state/execution/edges and /state/identity/edges query endpoints.

Flow:
1) Ensure project exists (optional, via API).
2) Insert one execution edge + one identity edge.
3) Query endpoints with filters and verify the inserted rows are returned.
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
from typing import Optional

import httpx
import uuid6
import pytest

from infra.primitives.identity import provision_identity
from infra.stores import ExecutionStore, IdentityStore
from infra.stores.resource_store import ResourceStore
from scripts.utils.api import create_project

DEFAULT_CARDBOX_DSN = "postgresql://postgres:postgres@localhost:5433/cardbox"
DEFAULT_API_URL = "http://127.0.0.1:8099"

async def _run_test(args: argparse.Namespace) -> int:
    api_url = str(args.api_url or DEFAULT_API_URL)
    dsn = args.dsn or DEFAULT_CARDBOX_DSN
    assert dsn, "missing postgres dsn (set PG_DSN)"

    async with httpx.AsyncClient(base_url=api_url, timeout=20.0) as client:
        if args.ensure_project:
            await create_project(
                client,
                project_id=args.project,
                title=args.project_title,
                owner_id=args.owner_id,
                bootstrap=args.bootstrap,
            )

    execution_store = ExecutionStore(dsn)
    identity_store = IdentityStore(dsn)
    resource_store = ResourceStore(dsn)
    await execution_store.open()
    await identity_store.open()
    await resource_store.open()
    try:
        edge_suffix = uuid6.uuid7().hex
        execution_edge_id = f"edge_{edge_suffix}"
        correlation_id = f"corr_{edge_suffix}"
        trace_id = uuid6.uuid7().hex
        parent_step_id = f"step_{uuid6.uuid7().hex}"

        await execution_store.insert_execution_edge(
            edge_id=execution_edge_id,
            project_id=args.project,
            channel_id=args.channel,
            primitive="enqueue",
            edge_phase="request",
            source_agent_id=args.source_agent_id,
            source_agent_turn_id=f"turn_{uuid6.uuid7().hex}",
            source_step_id=f"step_{uuid6.uuid7().hex}",
            target_agent_id=args.target_agent_id,
            target_agent_turn_id=f"turn_{uuid6.uuid7().hex}",
            correlation_id=correlation_id,
            enqueue_mode="call",
            recursion_depth=0,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            metadata={"source": "test_state_edges"},
        )

        identity_action = "provision"
        identity_trace_id = uuid6.uuid7().hex
        identity_parent_step_id = f"step_{uuid6.uuid7().hex}"
        await provision_identity(
            identity_store=identity_store,
            resource_store=resource_store,
            project_id=args.project,
            channel_id=args.channel,
            action=identity_action,
            target_agent_id=args.target_agent_id,
            profile_box_id=str(uuid6.uuid7()),
            worker_target="worker_generic",
            tags=["edge-test"],
            display_name="State Edge Test Agent",
            owner_agent_id=args.owner_id,
            metadata={"source": "test_state_edges"},
            source_agent_id=args.source_agent_id,
            parent_step_id=identity_parent_step_id,
            trace_id=identity_trace_id,
        )
    finally:
        await resource_store.close()
        await identity_store.close()
        await execution_store.close()

    async with httpx.AsyncClient(base_url=api_url, timeout=20.0) as client:
        exec_resp = await client.get(
            f"/projects/{args.project}/state/execution/edges",
            params={"correlation_id": correlation_id},
        )
        assert exec_resp.status_code < 400, (
            f"execution edges query failed: {exec_resp.status_code} {exec_resp.text}"
        )
        exec_rows = exec_resp.json()
        assert any(row.get("correlation_id") == correlation_id for row in exec_rows), (
            "execution edge not found in response"
        )
        print("[ok] execution edges query returned inserted row")

        ident_resp = await client.get(
            f"/projects/{args.project}/state/identity/edges",
            params={"agent_id": args.target_agent_id, "action": identity_action},
        )
        assert ident_resp.status_code < 400, (
            f"identity edges query failed: {ident_resp.status_code} {ident_resp.text}"
        )
        ident_rows = ident_resp.json()
        assert any(row.get("target_agent_id") == args.target_agent_id for row in ident_rows), (
            "identity edge not found in response"
        )
        print("[ok] identity edges query returned inserted row")

        tag_resp = await client.get(
            f"/projects/{args.project}/state/identity/edges",
            params={"tag": "edge-test"},
        )
        assert tag_resp.status_code < 400, (
            f"identity edges tag filter failed: {tag_resp.status_code} {tag_resp.text}"
        )
        tag_rows = tag_resp.json()
        assert any(row.get("target_agent_id") == args.target_agent_id for row in tag_rows), (
            "identity edge not found with tag filter"
        )
        print("[ok] identity edges tag filter returned inserted row")

    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test state edges query endpoints")
    parser.add_argument("--api-url", default=os.environ.get("API_URL", DEFAULT_API_URL))
    parser.add_argument("--dsn", default=os.environ.get("PG_DSN", DEFAULT_CARDBOX_DSN))
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID", "proj_state_edges_01"))
    parser.add_argument("--project-title", default=os.environ.get("PROJECT_TITLE", "State Edges Test"))
    parser.add_argument("--owner-id", default=os.environ.get("OWNER_ID", "user_state_edges"))
    parser.add_argument("--channel", default=os.environ.get("CHANNEL_ID", "public"))
    parser.add_argument("--source-agent-id", default=os.environ.get("SOURCE_AGENT_ID", "agent_source_test"))
    parser.add_argument("--target-agent-id", default=os.environ.get("TARGET_AGENT_ID", "agent_target_test"))
    parser.add_argument("--no-ensure-project", dest="ensure_project", action="store_false")
    parser.set_defaults(ensure_project=True)
    parser.add_argument("--no-bootstrap", dest="bootstrap", action="store_false")
    parser.set_defaults(bootstrap=True)
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()
    try:
        exit_code = asyncio.run(_run_test(args))
    except asyncio.TimeoutError:
        print("[error] timeout waiting for API")
        raise SystemExit(2)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_state_edges():
    args = _parse_args([])
    exit_code = await _run_test(args)
    if exit_code:
        raise SystemExit(exit_code)
