from __future__ import annotations

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from CommonGround.service.projection.lineage import get_turn_lineage
from CommonGround.service.projection.postgres_source import PostgresProjectionSource

from tests.projection_support import dispatch_child_turn, dispatch_root_turn, register_agent


PROJECT_ID = "projection-lineage"


def test_get_turn_lineage_returns_direct_children(kernel_app, test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)
    frontside = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
    parent_worker = AgentRef(project_id=PROJECT_ID, agent_id="parent-worker")
    child_worker = AgentRef(project_id=PROJECT_ID, agent_id="child-worker")
    register_agent(kernel_app, frontside, capabilities=("frontside.request",))
    register_agent(kernel_app, parent_worker, capabilities=(TURN_KIND_CONVERSATION_V1,))
    register_agent(kernel_app, child_worker, capabilities=(TURN_KIND_CONVERSATION_V1,))

    parent_turn = dispatch_root_turn(kernel_app, requested_by=frontside, target_agent=parent_worker, request_id="parent-1")
    claim = kernel_app.lifecycle.claim_turn(parent_worker)
    assert claim is not None
    child_turn = dispatch_child_turn(
        kernel_app,
        parent_agent=parent_worker,
        parent_claim=claim,
        target_agent=child_worker,
        dispatch_key="child-1",
    )

    lineage = get_turn_lineage(source, project_id=PROJECT_ID, turn_id=parent_turn.turn_id, limit=100)

    assert lineage.parent.turn_id == parent_turn.turn_id
    assert [item.turn_id for item in lineage.direct_children] == [child_turn.turn_id]
    assert lineage.direct_children[0].cause_kind == "turn"
    assert lineage.direct_children[0].cause_id == parent_turn.turn_id
