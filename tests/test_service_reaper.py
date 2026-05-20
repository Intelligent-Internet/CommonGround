from __future__ import annotations

from datetime import UTC, datetime
import time

from fastapi.testclient import TestClient

from CommonGround.agent_client import HttpAgentClient
from CommonGround.app import build_kernel_app
from CommonGround.contracts import AgentRef, DispatchAuthority, DispatchAuthorityMode, ManualClock, TURN_KIND_CONVERSATION_V1, TurnState
from CommonGround.service import ServiceConfig, create_service_app

from tests.auth_support import agent_headers
from tests.pg_support import reset_test_db


PROJECT_ID = "reaper-demo"
FRONTSIDE = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
WORKER = AgentRef(project_id=PROJECT_ID, agent_id="worker")


def _dispatch_root(
    client: HttpAgentClient,
    *,
    requested_by: AgentRef = FRONTSIDE,
    target_agent: AgentRef = WORKER,
    request_payload,
    request_id: str | None = None,
    dispatch_key: str | None = None,
    spawn_key: str | None = None,
):
    resolved_request_id = request_id or dispatch_key or spawn_key
    assert resolved_request_id is not None
    return client.dispatch(
        requested_by=requested_by,
        target_agent=target_agent,
        input_payload=request_payload,
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=resolved_request_id),
        dispatch_key=dispatch_key or spawn_key or resolved_request_id,
    )


def test_service_owned_claim_reaper_requeues_expired_turn(test_pg_dsn: str) -> None:
    reset_test_db(test_pg_dsn)
    clock = ManualClock(current=datetime(2026, 4, 16, tzinfo=UTC))
    kernel_app = build_kernel_app(pg_dsn=test_pg_dsn, claim_timeout_seconds=30, clock=clock)
    kernel_app.topology.register_agent(FRONTSIDE, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(WORKER, capabilities=(TURN_KIND_CONVERSATION_V1,))
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
            claim_reaper_interval_seconds=0.01,
        ),
        kernel_app=kernel_app,
    )

    with TestClient(app) as test_client:
        frontside_client = HttpAgentClient(client=test_client, headers=agent_headers(FRONTSIDE, test_pg_dsn))
        worker_client = HttpAgentClient(client=test_client, headers=agent_headers(WORKER, test_pg_dsn))
        turn = _dispatch_root(frontside_client,
            requested_by=FRONTSIDE,
            target_agent=WORKER,
            request_payload={"task": "expire me"},
            request_id="reaper-1",
        )
        claimed = worker_client.claim_turn(WORKER)
        assert claimed is not None

        kernel_app.advance_time(seconds=31)
        deadline = time.monotonic() + 1.0
        snapshot = worker_client.get_turn(turn)
        while snapshot.state != TurnState.QUEUED and time.monotonic() < deadline:
            time.sleep(0.02)
            snapshot = worker_client.get_turn(turn)

        assert snapshot.state == TurnState.QUEUED
        assert snapshot.current_claim_agent_id is None
        feed = worker_client.fetch_turn_feed(turn)
        reconciled = next(event for event in feed.items if event.event_type == "turn.claim_reconciled")
        assert reconciled.actor_kind == "system"
        assert reconciled.note == "service-owned claim reaper"
        assert reconciled.annotations["owner"] == "service"
        assert reconciled.annotations["reason"] == "service_claim_reaper"
        assert reconciled.annotations["system"] == {
            "note": "service-owned claim reaper",
            "reason": "service_claim_reaper",
            "owner": "service",
        }
        assert "caller" not in reconciled.annotations


def test_service_owned_claim_reaper_is_disabled_by_default(test_pg_dsn: str) -> None:
    reset_test_db(test_pg_dsn)
    clock = ManualClock(current=datetime(2026, 4, 16, tzinfo=UTC))
    kernel_app = build_kernel_app(pg_dsn=test_pg_dsn, claim_timeout_seconds=30, clock=clock)
    kernel_app.topology.register_agent(FRONTSIDE, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(WORKER, capabilities=(TURN_KIND_CONVERSATION_V1,))
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
        ),
        kernel_app=kernel_app,
    )

    with TestClient(app) as test_client:
        frontside_client = HttpAgentClient(client=test_client, headers=agent_headers(FRONTSIDE, test_pg_dsn))
        worker_client = HttpAgentClient(client=test_client, headers=agent_headers(WORKER, test_pg_dsn))
        turn = _dispatch_root(frontside_client,
            requested_by=FRONTSIDE,
            target_agent=WORKER,
            request_payload={"task": "stay running"},
            request_id="reaper-disabled-1",
        )
        claimed = worker_client.claim_turn(WORKER)
        assert claimed is not None

        kernel_app.advance_time(seconds=31)
        time.sleep(0.1)

        snapshot = worker_client.get_turn(turn)
        assert snapshot.state == TurnState.RUNNING
        assert snapshot.current_claim_agent_id == WORKER.agent_id


def test_service_owned_claim_reaper_does_not_touch_unexpired_turn(test_pg_dsn: str) -> None:
    reset_test_db(test_pg_dsn)
    clock = ManualClock(current=datetime(2026, 4, 16, tzinfo=UTC))
    kernel_app = build_kernel_app(pg_dsn=test_pg_dsn, claim_timeout_seconds=30, clock=clock)
    kernel_app.topology.register_agent(FRONTSIDE, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(WORKER, capabilities=(TURN_KIND_CONVERSATION_V1,))
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
            claim_reaper_interval_seconds=0.01,
        ),
        kernel_app=kernel_app,
    )

    with TestClient(app) as test_client:
        frontside_client = HttpAgentClient(client=test_client, headers=agent_headers(FRONTSIDE, test_pg_dsn))
        worker_client = HttpAgentClient(client=test_client, headers=agent_headers(WORKER, test_pg_dsn))
        turn = _dispatch_root(frontside_client,
            requested_by=FRONTSIDE,
            target_agent=WORKER,
            request_payload={"task": "not expired"},
            request_id="reaper-unexpired-1",
        )
        claimed = worker_client.claim_turn(WORKER)
        assert claimed is not None

        time.sleep(0.1)

        snapshot = worker_client.get_turn(turn)
        assert snapshot.state == TurnState.RUNNING
        assert snapshot.current_claim_agent_id == WORKER.agent_id
