from __future__ import annotations

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1, TurnOutcome
from CommonGround.service.projection.feed import fetch_project_feed
from CommonGround.service.projection.postgres_source import PostgresProjectionSource

from tests.projection_support import dispatch_root_turn, register_agent


PROJECT_ID = "projection-feed"


def test_fetch_project_feed_returns_project_events(kernel_app, test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)
    frontside = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
    worker = AgentRef(project_id=PROJECT_ID, agent_id="worker")
    register_agent(kernel_app, frontside, capabilities=("frontside.request",))
    register_agent(kernel_app, worker, capabilities=(TURN_KIND_CONVERSATION_V1,))

    dispatch_root_turn(kernel_app, requested_by=frontside, target_agent=worker, request_id="feed-1")
    claim = kernel_app.lifecycle.claim_turn(worker)
    assert claim is not None
    kernel_app.lifecycle.finish_turn(claim, TurnOutcome.SUCCEEDED)

    page = fetch_project_feed(source, project_id=PROJECT_ID, after_ledger_seq=0, limit=100)

    assert len(page.items) >= 2
    assert page.items[0].ledger_seq < page.items[-1].ledger_seq
    assert {item.event_type for item in page.items} >= {"turn.spawned", "turn.finished"}
    assert page.next_after_ledger_seq == page.items[-1].ledger_seq
