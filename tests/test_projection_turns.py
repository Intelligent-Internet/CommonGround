from __future__ import annotations

from CommonGround.contracts import AgentRef, TurnOutcome
from CommonGround.service.projection.filters import TurnEntryFilters
from CommonGround.service.projection.postgres_source import PostgresProjectionSource
from CommonGround.service.projection.turns import list_turn_entries

from tests.projection_support import dispatch_root_turn, register_agent


PROJECT_ID = "projection-turns"


def test_list_turn_entries_supports_state_and_outcome_filters(kernel_app, test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)
    frontside = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
    worker = AgentRef(project_id=PROJECT_ID, agent_id="worker")
    queued_worker = AgentRef(project_id=PROJECT_ID, agent_id="queued-worker")
    register_agent(kernel_app, frontside, capabilities=("frontside.request",), grants=("turn.stop.any",))
    register_agent(kernel_app, worker, capabilities=("turn.conversation.v1",))
    register_agent(kernel_app, queued_worker, capabilities=("turn.conversation.v1",))

    closed_turn = dispatch_root_turn(kernel_app, requested_by=frontside, target_agent=worker, request_id="closed-1")
    claim = kernel_app.lifecycle.claim_turn(worker)
    assert claim is not None
    kernel_app.lifecycle.finish_turn(claim, TurnOutcome.SUCCEEDED)

    stop_requested_turn = dispatch_root_turn(kernel_app, requested_by=frontside, target_agent=queued_worker, request_id="queued-1")
    kernel_app.lifecycle.request_stop_turn(stop_requested_turn, requested_by=frontside)

    closed_page = list_turn_entries(
        source,
        project_id=PROJECT_ID,
        filters=TurnEntryFilters(state="closed", outcome="succeeded", limit=100),
    )
    stop_page = list_turn_entries(
        source,
        project_id=PROJECT_ID,
        filters=TurnEntryFilters(stop_requested_only=True, limit=100),
    )
    all_page = list_turn_entries(
        source,
        project_id=PROJECT_ID,
        filters=TurnEntryFilters(limit=100),
    )

    assert [item.turn_id for item in closed_page.items] == [closed_turn.turn_id]
    assert [item.turn_id for item in stop_page.items] == [stop_requested_turn.turn_id]
    assert [item.project_turn_seq for item in all_page.items] == sorted(
        [item.project_turn_seq for item in all_page.items],
        reverse=True,
    )
    closed_row = source.get_turn_row(project_id=PROJECT_ID, turn_id=closed_turn.turn_id)
    stop_row = source.get_turn_row(project_id=PROJECT_ID, turn_id=stop_requested_turn.turn_id)
    assert closed_row is not None
    assert stop_row is not None
    assert closed_page.items[0].project_turn_seq == closed_row.project_turn_seq
    assert stop_page.items[0].project_turn_seq == stop_row.project_turn_seq
