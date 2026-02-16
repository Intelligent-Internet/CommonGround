#!/usr/bin/env python3
"""
Test watchdog safety net behaviors (PMO watchdog logic).

Prereqs:
  - Postgres + NATS reachable
  - schema initialized (scripts/setup/init_db.sql)

This script does NOT require PMO/worker to be running; it calls L0Guard directly.
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
from typing import Any, Dict, Optional

import uuid6
import pytest
from nats.errors import TimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from core.config import PROTOCOL_VERSION
from core.subject import format_subject
from core.status import STATUS_TIMEOUT
from core.trace import ensure_trace_headers
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore
from services.pmo.l0_guard import L0Guard
DEFAULT_CARDBOX_DSN = "postgresql://postgres:postgres@localhost:5433/cardbox"

def _nats_config(override: Optional[str]) -> Dict[str, Any]:
    if override:
        return {"servers": [override]}
    return {}


async def _create_js_pull(
    *,
    js: Any,
    subject: str,
    durable: str,
) -> Any:
    config = ConsumerConfig(
        durable_name=durable,
        deliver_policy=DeliverPolicy.NEW,
        ack_policy=AckPolicy.EXPLICIT,
    )
    return await js.pull_subscribe(subject, durable=durable, config=config)


async def _fetch_js_message(sub: Any, *, timeout_s: float) -> Optional[Any]:
    try:
        msgs = await sub.fetch(1, timeout=timeout_s)
    except TimeoutError:
        return None
    if not msgs:
        return None
    msg = msgs[0]
    try:
        await msg.ack()
    except Exception:
        pass
    return msg


async def _cleanup_consumer(js: Any, *, stream: str, durable: str) -> None:
    try:
        await js.delete_consumer(stream, durable)
    except Exception:
        pass


async def _test_dispatched_timeout(
    *,
    guard: L0Guard,
    nats: NATSClient,
    cardbox: CardBoxClient,
    state_store: StateStore,
    project_id: str,
    channel_id: str,
    timeout_s: float,
) -> None:
    suffix = uuid6.uuid7().hex[:8]
    agent_id = f"agent_watchdog_timeout_{suffix}"
    output_box_id = await cardbox.ensure_box_id(project_id=project_id, box_id=None)
    agent_turn_id = f"turn_{uuid6.uuid7().hex}"

    turn_epoch = await state_store.lease_agent_turn(
        project_id=project_id,
        agent_id=agent_id,
        agent_turn_id=agent_turn_id,
        active_channel_id=channel_id,
        profile_box_id=None,
        context_box_id=None,
        output_box_id=output_box_id,
        parent_step_id=None,
        trace_id=None,
        active_recursion_depth=0,
    )
    assert turn_epoch is not None, "failed to lease agent_run for dispatched timeout test"

    subject_evt = format_subject(
        project_id,
        channel_id,
        "evt",
        "agent",
        agent_id,
        "task",
    )
    durable = f"watchdog_evt_{suffix}"
    sub = await _create_js_pull(js=nats.js, subject=subject_evt, durable=durable)

    await asyncio.sleep(timeout_s + 0.2)
    await guard.reap_stuck_dispatched_timeout()
    msg = await _fetch_js_message(sub, timeout_s=5.0)
    await _cleanup_consumer(js=nats.js, stream=f"cg_evt_{PROTOCOL_VERSION}", durable=durable)

    assert msg is not None, "missing evt.agent.*.task for dispatched timeout"
    payload = json.loads(msg.data.decode("utf-8"))
    assert payload.get("agent_turn_id") == agent_turn_id, "evt.agent.*.task agent_turn_id mismatch"
    assert payload.get("status") == STATUS_TIMEOUT, "evt.agent.*.task status mismatch"
    assert payload.get("error") == "dispatch_timeout", "evt.agent.*.task error mismatch"

    state = await state_store.fetch(project_id, agent_id)
    assert state and state.status == "idle", "state not reset to idle after dispatched timeout"
    assert state.active_agent_turn_id is None, "active_agent_turn_id not cleared after dispatched timeout"
    assert state.turn_epoch == int(turn_epoch) + 1, "turn_epoch not bumped after dispatched timeout"

    deliverable_id = payload.get("deliverable_card_id")
    assert deliverable_id, "missing deliverable_card_id in dispatched timeout event"
    box = await cardbox.get_box(output_box_id, project_id=project_id)
    assert box and box.card_ids, "deliverable not appended to output_box"
    assert deliverable_id in box.card_ids, "deliverable_card_id not found in output_box"

    print("[ok] dispatched timeout produces evt.agent.*.task + deliverable + state reset")


async def _test_pending_wakeup(
    *,
    guard: L0Guard,
    nats: NATSClient,
    execution_store: ExecutionStore,
    resource_store: ResourceStore,
    project_id: str,
    channel_id: str,
    pending_age_s: float,
) -> None:
    suffix = uuid6.uuid7().hex[:8]
    agent_id = f"agent_watchdog_wakeup_{suffix}"
    agent_turn_id = f"turn_{uuid6.uuid7().hex}"
    inbox_id = f"inbox_{uuid6.uuid7().hex}"
    correlation_id = f"corr_{uuid6.uuid7().hex}"

    _, traceparent, trace_id = ensure_trace_headers({})

    await resource_store.upsert_project_agent(
        project_id=project_id,
        agent_id=agent_id,
        profile_box_id=str(uuid6.uuid7()),
        worker_target="worker_generic",
        tags=["watchdog"],
        display_name="Watchdog Pending Agent",
        owner_agent_id=None,
        metadata={"source": "test_watchdog_safety_net"},
    )

    await execution_store.insert_inbox(
        inbox_id=inbox_id,
        project_id=project_id,
        agent_id=agent_id,
        message_type="turn",
        enqueue_mode="call",
        correlation_id=correlation_id,
        recursion_depth=0,
        traceparent=traceparent,
        tracestate=None,
        trace_id=trace_id,
        parent_step_id=None,
        source_agent_id=None,
        source_step_id=None,
        payload={
            "agent_id": agent_id,
            "agent_turn_id": agent_turn_id,
            "channel_id": channel_id,
        },
        status="pending",
    )

    target_subject = format_subject(
        project_id,
        channel_id,
        "cmd",
        "agent",
        "worker_generic",
        "wakeup",
    )
    durable = f"watchdog_cmd_{suffix}"
    sub = await _create_js_pull(js=nats.js, subject=target_subject, durable=durable)

    await asyncio.sleep(pending_age_s + 0.2)
    await guard.poke_stale_inbox()
    msg = await _fetch_js_message(sub, timeout_s=5.0)
    await _cleanup_consumer(js=nats.js, stream=f"cg_cmd_{PROTOCOL_VERSION}", durable=durable)

    assert msg is not None, "missing cmd.agent.*.wakeup for pending inbox"
    payload = json.loads(msg.data.decode("utf-8"))
    assert payload.get("agent_id") == agent_id, "wakeup payload agent_id mismatch"

    print("[ok] pending inbox triggers wakeup emission")


async def _run_test(args: argparse.Namespace) -> int:
    dsn = args.dsn or DEFAULT_CARDBOX_DSN
    assert dsn, "missing postgres dsn (set PG_DSN)"
    nats_cfg = _nats_config(args.nats_url)

    cardbox = CardBoxClient(config={"postgres_dsn": dsn})
    nats = NATSClient(config=nats_cfg)
    state_store = StateStore(dsn)
    execution_store = ExecutionStore(dsn)
    resource_store = ResourceStore(dsn)
    await cardbox.init()
    await nats.connect()
    await state_store.open()
    await execution_store.open()
    await resource_store.open()

    guard = L0Guard(
        state_store=state_store,
        resource_store=resource_store,
        cardbox=cardbox,
        nats=nats,
        execution_store=execution_store,
        dispatched_retry_seconds=1.0,
        dispatched_timeout_seconds=float(args.dispatched_timeout),
        active_reap_seconds=3600.0,
        pending_wakeup_seconds=float(args.pending_wakeup),
        pending_wakeup_skip_seconds=float(args.pending_wakeup_skip),
    )

    try:
        await _test_dispatched_timeout(
            guard=guard,
            nats=nats,
            cardbox=cardbox,
            state_store=state_store,
            project_id=args.project,
            channel_id=args.channel,
            timeout_s=float(args.dispatched_timeout),
        )
        await _test_pending_wakeup(
            guard=guard,
            nats=nats,
            execution_store=execution_store,
            resource_store=resource_store,
            project_id=args.project,
            channel_id=args.channel,
            pending_age_s=float(args.pending_wakeup),
        )
    finally:
        await resource_store.close()
        await execution_store.close()
        await state_store.close()
        await nats.close()
        await cardbox.close()
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test PMO watchdog safety net behaviors")
    parser.add_argument("--dsn", default=os.environ.get("PG_DSN", DEFAULT_CARDBOX_DSN))
    parser.add_argument("--nats-url", default=os.environ.get("NATS_URL", "nats://localhost:4222"))
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID", f"proj_watchdog_{uuid6.uuid7().hex[:8]}"))
    parser.add_argument("--channel", default=os.environ.get("CHANNEL_ID", "public"))
    parser.add_argument("--dispatched-timeout", type=float, default=1.0)
    parser.add_argument("--pending-wakeup", type=float, default=1.0)
    parser.add_argument(
        "--pending-wakeup-skip",
        type=float,
        default=float(os.environ.get("PENDING_WAKEUP_SKIP_SECONDS", "5.0")),
    )
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()
    try:
        exit_code = asyncio.run(_run_test(args))
    except Exception as exc:  # noqa: BLE001
        print(f"[error] {exc}")
        raise SystemExit(1)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_watchdog_safety_net():
    args = _parse_args([])
    exit_code = await _run_test(args)
    if exit_code:
        raise SystemExit(exit_code)
