from __future__ import annotations

from typing import Iterable

import pytest

from CommonGround.adapters import ExternalAgentAdapter
from CommonGround.contracts import (
    AgentRef,
    CauseRef,
    ConflictError,
    DispatchAuthority,
    DispatchAuthorityMode,
    FencingError,
    InvariantError,
    SpawnTurnSpec,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    TurnOutcome,
    TurnState,
)


PROJECT_ID = "demo"
FRONTSIDE = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
PLANNER = AgentRef(project_id=PROJECT_ID, agent_id="nanobot")
CODEX = AgentRef(project_id=PROJECT_ID, agent_id="codex")
OTHER_PROJECT_NANOBOT = AgentRef(project_id="other", agent_id="nanobot")


def _register_agents(kernel_app, agents: Iterable[AgentRef]) -> None:
    for agent in agents:
        kernel_app.topology.register_agent(agent, capabilities=(TURN_KIND_CONVERSATION_V1,))


def _claim_with_context(worker: ExternalAgentAdapter):
    claim = worker.claim_turn()
    assert claim is not None
    return claim, worker.fetch_context(claim.turn_ref())


def _dispatch_root(
    kernel_app,
    *,
    requested_by: AgentRef,
    target_agent: AgentRef,
    payload=None,
    bootstrap_input=None,
    request_id: str | None = None,
    spawn_key: str | None = None,
    turn_kind: str = TURN_KIND_CONVERSATION_V1,
    **_: object,
):
    resolved_payload = payload if payload is not None else bootstrap_input
    resolved_request_id = request_id or spawn_key
    assert resolved_payload is not None
    assert resolved_request_id is not None
    return kernel_app.sdk.dispatch(
        requested_by=requested_by,
        target_agent=target_agent,
        input_payload=resolved_payload,
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=resolved_request_id),
        dispatch_key=resolved_request_id,
        turn_kind=turn_kind,
    )


def _dispatch_derived(
    worker: ExternalAgentAdapter,
    claim,
    *,
    target_agent: AgentRef,
    payload,
    dispatch_key: str,
    turn_kind: str = TURN_KIND_CONVERSATION_V1,
):
    return worker.dispatch(
        claim,
        target_agent=target_agent,
        input_payload=payload,
        dispatch_key=dispatch_key,
        turn_kind=turn_kind,
    )


def test_root_turn_claim_progress_finish_flow(kernel_app) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))

    turn = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "summarize issue 546"},
        cause=CauseRef(kind="external_request", id="req-1"),
        spawn_key="req-1",
    )
    turn_seq = int(turn.turn_id.removeprefix("T-"))
    assert turn.turn_id == f"T-{turn_seq}"

    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    claim, context = _claim_with_context(worker)
    assert claim.turn_ref() == turn
    assert context.semantic_items[0].record.record_role == "bootstrap"
    assert context.semantic_items[0].content.payload() == {"task": "summarize issue 546"}

    worker.append_record(claim, {"phase": "working"})
    worker.finish_current(
        claim,
        outcome=TurnOutcome.SUCCEEDED,
        final_payload={"result": "done"},
    )

    snapshot = kernel_app.lifecycle.get_turn(turn)
    assert snapshot is not None
    assert snapshot.turn.turn_id == turn.turn_id
    assert snapshot.turn_kind == TURN_KIND_CONVERSATION_V1
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.SUCCEEDED
    assert snapshot.final_record_role == "deliverable"
    assert snapshot.final_cardbox_ref is not None
    assert snapshot.final_payload == {"result": "done"}

    feed = worker.fetch_turn_feed(turn)
    assert [event.event_type for event in feed.items] == [
        "turn.spawned",
        "turn.bootstrap_recorded",
        "turn.claimed",
        "turn.progress_appended",
        "turn.progress_appended",
        "turn.finished",
    ]


def test_suspended_turn_does_not_block_same_agent_claiming_queued_turn(kernel_app) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))
    first_turn = _dispatch_root(
        kernel_app,
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "first"},
        cause=CauseRef(kind="external_request", id="req-suspend-first"),
        spawn_key="req-suspend-first",
    )
    second_turn = _dispatch_root(
        kernel_app,
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "second"},
        cause=CauseRef(kind="external_request", id="req-suspend-second"),
        spawn_key="req-suspend-second",
    )

    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    first_claim, _ = _claim_with_context(worker)
    assert first_claim.turn_ref() == first_turn
    worker.suspend_current(first_claim, reason="await_external", note="waiting")

    second_claim, _ = _claim_with_context(worker)
    assert second_claim.turn_ref() == second_turn
    first_snapshot = kernel_app.lifecycle.get_turn(first_turn)
    second_snapshot = kernel_app.lifecycle.get_turn(second_turn)
    assert first_snapshot is not None
    assert second_snapshot is not None
    assert first_snapshot.state == TurnState.SUSPENDED
    assert second_snapshot.state == TurnState.RUNNING


def test_suspend_derived_dispatch_resume_flow(kernel_app) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))
    kernel_app.topology.register_agent(CODEX, capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,))

    parent_turn = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "plan parent turn"},
        cause=CauseRef(kind="external_request", id="req-parent"),
        spawn_key="req-parent",
    )

    parent_worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    parent_claim, _ = _claim_with_context(parent_worker)
    assert parent_claim.turn_ref() == parent_turn

    child_turn = _dispatch_derived(parent_worker, 
        parent_claim,
        target_agent=CODEX,
        payload={"task": "child work"},
        dispatch_key="child-1",
        turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    )
    parent_worker.suspend_current(parent_claim, reason="await_child", note="waiting on child turn")

    suspended = kernel_app.lifecycle.get_turn(parent_turn)
    assert suspended is not None
    assert suspended.state == TurnState.SUSPENDED

    child_worker = ExternalAgentAdapter(agent=CODEX, sdk=kernel_app.sdk)
    child_claim, _ = _claim_with_context(child_worker)
    assert child_claim.turn_ref() == child_turn
    child_snapshot = kernel_app.lifecycle.get_turn(child_turn)
    assert child_snapshot is not None
    assert child_snapshot.turn_kind == TURN_KIND_PROVISION_AGENT_SPAWN_V1
    child_worker.finish_current(
        child_claim,
        outcome=TurnOutcome.SUCCEEDED,
        final_payload={"child_result": "ok"},
    )

    parent_feed = parent_worker.fetch_turn_feed(parent_turn)
    assert any(event.event_type == "turn.spawned" and event.subject_id == child_turn.turn_id for event in parent_feed.items)
    assert any(event.event_type == "turn.finished" and event.subject_id == child_turn.turn_id for event in parent_feed.items)

    parent_worker.resume_turn(parent_turn, note="child done")
    reclaimed_claim, _ = _claim_with_context(parent_worker)
    assert reclaimed_claim.turn_ref() == parent_turn
    parent_worker.finish_current(reclaimed_claim, outcome=TurnOutcome.SUCCEEDED, final_payload={"parent_result": "ok"})

    closed = kernel_app.lifecycle.get_turn(parent_turn)
    assert closed is not None
    assert closed.state == TurnState.CLOSED
    assert closed.outcome == TurnOutcome.SUCCEEDED
    assert closed.final_record_role == "deliverable"
    assert closed.final_payload == {"parent_result": "ok"}


def test_turn_snapshot_prefers_authoritative_final_result_over_intermediate_deliverable(kernel_app) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))

    turn = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "final truth"},
        cause=CauseRef(kind="external_request", id="req-final-truth"),
        spawn_key="req-final-truth",
    )

    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    claim, _ = _claim_with_context(worker)
    worker.append_record(claim, {"draft": "intermediate"}, role="deliverable")
    worker.finish_current(
        claim,
        outcome=TurnOutcome.FAILED,
        final_payload={"error": "final"},
        final_record_role="error_report",
    )

    snapshot = kernel_app.lifecycle.get_turn(turn)
    assert snapshot is not None
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.FAILED
    assert snapshot.final_record_role == "error_report"
    assert snapshot.final_payload == {"error": "final"}


def test_turn_snapshot_raises_on_dangling_final_cardbox_ref(kernel_app, monkeypatch) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))

    turn = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "dangling final payload"},
        cause=CauseRef(kind="external_request", id="req-dangling-final"),
        spawn_key="req-dangling-final",
    )

    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    claim, _ = _claim_with_context(worker)
    worker.finish_current(
        claim,
        outcome=TurnOutcome.SUCCEEDED,
        final_payload={"result": "done"},
    )

    monkeypatch.setattr(kernel_app.cardbox, "hydrate_box", lambda ref: None)

    with pytest.raises(InvariantError, match="dangling final cardbox ref"):
        kernel_app.lifecycle.get_turn(turn)


def test_claim_expiry_requires_reconcile_before_reclaim(kernel_app, clock) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))

    turn = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "expire claim"},
        cause=CauseRef(kind="external_request", id="req-expire"),
        spawn_key="req-expire",
    )

    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    first_claim, _ = _claim_with_context(worker)
    clock.advance(seconds=31)

    with pytest.raises(FencingError):
        worker.append_record(first_claim, {"phase": "too_late"})

    summary = worker.reconcile_once()
    assert summary.scanned_count == 1
    assert summary.reconciled_count == 1

    second_claim, _ = _claim_with_context(worker)
    assert second_claim.turn_ref() == turn
    assert second_claim.token != first_claim.token
    worker.finish_current(second_claim, outcome=TurnOutcome.SUCCEEDED)


def test_stop_request_terminalizes_expired_running_claim(kernel_app, clock) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))

    turn = _dispatch_root(
        kernel_app,
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "will expire before stop"},
        cause=CauseRef(kind="external_request", id="req-expired-stop"),
        spawn_key="req-expired-stop",
    )

    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    claim, _ = _claim_with_context(worker)
    assert claim.turn_ref() == turn

    clock.advance(seconds=31)
    kernel_app.lifecycle.request_stop_turn(turn, requested_by=PLANNER)

    snapshot = kernel_app.lifecycle.get_turn(turn)
    assert snapshot is not None
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.STOPPED
    assert snapshot.stop_requested is True
    assert snapshot.claim_expires_at is None

    feed = worker.fetch_turn_feed(turn)
    assert [event.event_type for event in feed.items][-2:] == ["turn.stop_requested", "turn.finished"]


def test_repeated_stop_request_terminalizes_already_stop_requested_expired_claim(kernel_app, clock) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))

    turn = _dispatch_root(
        kernel_app,
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "stop then expire"},
        cause=CauseRef(kind="external_request", id="req-stop-then-expire"),
        spawn_key="req-stop-then-expire",
    )

    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    claim, _ = _claim_with_context(worker)
    assert claim.turn_ref() == turn

    kernel_app.lifecycle.request_stop_turn(turn, requested_by=PLANNER)
    clock.advance(seconds=31)
    kernel_app.lifecycle.request_stop_turn(turn, requested_by=PLANNER)

    snapshot = kernel_app.lifecycle.get_turn(turn)
    assert snapshot is not None
    assert snapshot.state == TurnState.CLOSED
    assert snapshot.outcome == TurnOutcome.STOPPED

    feed = worker.fetch_turn_feed(turn)
    assert [event.event_type for event in feed.items].count("turn.stop_requested") == 1
    assert [event.event_type for event in feed.items][-1] == "turn.finished"


def test_public_spawn_rejects_turn_cause(kernel_app) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER))

    bootstrap_ref = kernel_app.cardbox.create_payload_box(
        PROJECT_ID,
        {"task": "illegal public child cause"},
        metadata={"cg_role": "bootstrap_bundle"},
    )

    with pytest.raises(ConflictError):
        kernel_app.lifecycle.dispatch(
            requested_by=FRONTSIDE,
            authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="illegal-turn-cause"),
            spec=SpawnTurnSpec(
                target_agent=PLANNER,
                cause=CauseRef(kind="turn", id="parent-turn"),
                bootstrap_bundle_ref=bootstrap_ref,
                spawn_key="illegal-turn-cause",
            ),
        )


def test_spawn_rejects_missing_target_turn_capability(kernel_app) -> None:
    kernel_app.topology.register_agent(FRONTSIDE)
    kernel_app.topology.register_agent(PLANNER)

    with pytest.raises(ConflictError, match="missing required capability"):
        _dispatch_root(kernel_app, 
            requested_by=FRONTSIDE,
            target_agent=PLANNER,
            bootstrap_input={"task": "missing capability"},
            cause=CauseRef(kind="external_request", id="req-missing-capability"),
            spawn_key="req-missing-capability",
        )


def test_spawn_key_idempotency_rejects_different_turn_kind(kernel_app) -> None:
    kernel_app.topology.register_agent(FRONTSIDE, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(PLANNER, capabilities=(TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1))

    first = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "same spawn key"},
        cause=CauseRef(kind="external_request", id="req-kind-conflict"),
        spawn_key="req-kind-conflict",
    )
    first_snapshot = kernel_app.lifecycle.get_turn(first)
    assert first_snapshot is not None
    assert first_snapshot.turn_kind == TURN_KIND_CONVERSATION_V1

    with pytest.raises(ConflictError, match="turn_kind"):
        _dispatch_root(kernel_app, 
            requested_by=FRONTSIDE,
            target_agent=PLANNER,
            bootstrap_input={"task": "same spawn key"},
            cause=CauseRef(kind="external_request", id="req-kind-conflict"),
            spawn_key="req-kind-conflict",
            turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
        )


def test_spawn_key_idempotency_reuses_existing_turn_without_consuming_new_seq(kernel_app) -> None:
    kernel_app.topology.register_agent(FRONTSIDE, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(PLANNER, capabilities=(TURN_KIND_CONVERSATION_V1,))

    first = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "same spawn key"},
        cause=CauseRef(kind="external_request", id="req-idempotent"),
        spawn_key="req-idempotent",
    )
    second = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "same spawn key"},
        cause=CauseRef(kind="external_request", id="req-idempotent"),
        spawn_key="req-idempotent",
    )
    third = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "fresh spawn key"},
        cause=CauseRef(kind="external_request", id="req-next"),
        spawn_key="req-next",
    )

    assert first == second
    first_seq = int(first.turn_id.removeprefix("T-"))
    third_seq = int(third.turn_id.removeprefix("T-"))
    assert first.turn_id == f"T-{first_seq}"
    assert third.turn_id == f"T-{third_seq}"
    assert third_seq == first_seq + 1


def test_cross_project_agent_cannot_stop_or_resume_same_agent_id(kernel_app) -> None:
    _register_agents(kernel_app, (FRONTSIDE, PLANNER, OTHER_PROJECT_NANOBOT))

    turn = _dispatch_root(kernel_app, 
        requested_by=FRONTSIDE,
        target_agent=PLANNER,
        bootstrap_input={"task": "owner boundary"},
        cause=CauseRef(kind="external_request", id="req-owner-boundary"),
        spawn_key="req-owner-boundary",
    )
    worker = ExternalAgentAdapter(agent=PLANNER, sdk=kernel_app.sdk)
    claim, _ = _claim_with_context(worker)

    with pytest.raises(ConflictError):
        kernel_app.lifecycle.request_stop_turn(turn, requested_by=OTHER_PROJECT_NANOBOT)

    worker.suspend_current(claim, reason="pause")

    with pytest.raises(ConflictError):
        kernel_app.lifecycle.resume_turn(turn, requested_by=OTHER_PROJECT_NANOBOT)
