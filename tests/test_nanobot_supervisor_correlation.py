from __future__ import annotations

from types import SimpleNamespace

from CommonGround.agent_client import SuspendTurnAction
from CommonGround.contracts import (
    AgentRef,
    ClaimToken,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    TurnOutcome,
    TurnRef,
)
from Integrations.nanobot.adapter.context_mapping import EXECUTION_PLAN_PHASE, WORK_ORDER_KIND_V1
from Integrations.nanobot.adapter.supervisor_handler import SupervisorTurnHandler


def _semantic_item(role, payload):
    return SimpleNamespace(
        record=SimpleNamespace(record_role=role),
        content=SimpleNamespace(payload=lambda: payload),
    )


def _context(payload, *, work_orders=(), turn_id="T-parent"):
    semantic_items = [_semantic_item("bootstrap", payload)]
    if work_orders:
        semantic_items.append(
            _semantic_item(
                "progress",
                {
                    "phase": EXECUTION_PLAN_PHASE,
                    "source": "test",
                    "work_orders": list(work_orders),
                },
            )
        )
    return SimpleNamespace(
        turn=SimpleNamespace(
            turn=SimpleNamespace(turn_id=turn_id),
            target_agent=SimpleNamespace(agent_id="nanobot_parent"),
            turn_kind=TURN_KIND_CONVERSATION_V1,
            cause=SimpleNamespace(kind="external_request", id="request-1"),
        ),
        semantic_items=tuple(semantic_items),
    )


def _feed_page(items=()):
    return SimpleNamespace(items=tuple(items), next_after_ledger_seq=0)


def _claim(turn_id="T-parent"):
    return ClaimToken(
        project_id="demo",
        turn_id=turn_id,
        agent_id="nanobot_parent",
        token="token",
        expires_at=SimpleNamespace(),
    )


def _work_order(task_id, objective):
    return {
        "kind": WORK_ORDER_KIND_V1,
        "task_id": task_id,
        "objective": objective,
        "input": {},
        "expected_output": {"type": "text"},
        "delegation_policy": {"may_delegate": False},
        "provenance": {"parent_turn_id": "T-parent"},
    }


def _provision_context(*, task_id, parent_turn_id="T-parent", deliverable_task_id=None, leaf_agent_id=None):
    return SimpleNamespace(
        semantic_items=(
            _semantic_item(
                "bootstrap",
                {
                    "parent_turn_id": parent_turn_id,
                    "lifecycle": {"source_turn_id": parent_turn_id},
                    "work_order": {"task_id": task_id, "parent_turn_id": parent_turn_id},
                },
            ),
            _semantic_item(
                "deliverable",
                {
                    "task_id": deliverable_task_id or task_id,
                    "new_agent_ref": {"project_id": "demo", "agent_id": leaf_agent_id or f"leaf-{task_id}"},
                },
            ),
        )
    )


def _child_context(*, task_id, child_turn_id, parent_turn_id="T-parent", result=None):
    return SimpleNamespace(
        semantic_items=(
            _semantic_item(
                "bootstrap",
                {
                    "kind": WORK_ORDER_KIND_V1,
                    "task_id": task_id,
                    "objective": f"Objective {task_id}",
                    "provenance": {"task_id": task_id, "parent_turn_id": parent_turn_id},
                },
            ),
            _semantic_item("deliverable", result or {"content": f"result {task_id}", "child_turn_id": child_turn_id}),
        )
    )


class _Loop:
    async def process_direct(self, content: str, **kwargs):
        return SimpleNamespace(content="supervisor final")


class _CorrelationClient:
    def __init__(self, *, events, snapshots, contexts):
        self._events = events
        self._snapshots = snapshots
        self._contexts = contexts
        self.dispatch_calls = []

    def fetch_turn_feed(self, turn, *, after_ledger_seq=0, limit=500):
        assert turn == TurnRef(project_id="demo", turn_id="T-parent")
        return _feed_page(self._events)

    def get_turn(self, turn):
        return self._snapshots[turn.turn_id]

    def fetch_context(self, turn):
        return self._contexts[turn.turn_id]

    def dispatch(self, **kwargs):
        self.dispatch_calls.append(kwargs)
        return SimpleNamespace(turn_id=f"T-dispatched-{len(self.dispatch_calls)}")


def test_supervisor_maps_reversed_feed_order_by_task_id():
    work_orders = (_work_order("task_a", "Do A"), _work_order("task_b", "Do B"))
    client = _CorrelationClient(
        events=(
            SimpleNamespace(event_type="turn.spawned", subject_id="P-b"),
            SimpleNamespace(event_type="turn.spawned", subject_id="C-b"),
            SimpleNamespace(event_type="turn.spawned", subject_id="P-a"),
            SimpleNamespace(event_type="turn.spawned", subject_id="C-a"),
        ),
        snapshots={
            "P-a": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
            "P-b": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
            "C-a": SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=TurnOutcome.SUCCEEDED),
            "C-b": SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=TurnOutcome.SUCCEEDED),
        },
        contexts={
            "P-a": _provision_context(task_id="task_a", leaf_agent_id="leaf-a"),
            "P-b": _provision_context(task_id="task_b", leaf_agent_id="leaf-b"),
            "C-a": _child_context(task_id="task_a", child_turn_id="C-a"),
            "C-b": _child_context(task_id="task_b", child_turn_id="C-b"),
        },
    )

    action = SupervisorTurnHandler(
        loop=_Loop(),
        provisioner_agent=AgentRef(project_id="demo", agent_id="provisioner"),
    ).handle_turn(_context({"task": "delegate"}, work_orders=work_orders), client, _claim())

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["provision_turn_ids"] == ["P-a", "P-b"]
    assert action.final_payload["child_turn_ids"] == ["C-a", "C-b"]
    assert [item["task_id"] for item in action.final_payload["child_results"]] == ["task_a", "task_b"]
    assert client.dispatch_calls == []


def test_supervisor_ignores_extra_historical_child_and_provision_turns():
    work_orders = (_work_order("task_current", "Do current"),)
    client = _CorrelationClient(
        events=(
            SimpleNamespace(event_type="turn.spawned", subject_id="P-old"),
            SimpleNamespace(event_type="turn.spawned", subject_id="C-old"),
            SimpleNamespace(event_type="turn.spawned", subject_id="P-current"),
            SimpleNamespace(event_type="turn.spawned", subject_id="C-current"),
        ),
        snapshots={
            "P-old": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
            "C-old": SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=TurnOutcome.SUCCEEDED),
            "P-current": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED),
            "C-current": SimpleNamespace(turn_kind=TURN_KIND_CONVERSATION_V1, outcome=TurnOutcome.SUCCEEDED),
        },
        contexts={
            "P-old": _provision_context(task_id="task_old", leaf_agent_id="leaf-old"),
            "C-old": _child_context(task_id="task_old", child_turn_id="C-old"),
            "P-current": _provision_context(task_id="task_current", leaf_agent_id="leaf-current"),
            "C-current": _child_context(task_id="task_current", child_turn_id="C-current"),
        },
    )

    action = SupervisorTurnHandler(
        loop=_Loop(),
        provisioner_agent=AgentRef(project_id="demo", agent_id="provisioner"),
    ).handle_turn(_context({"task": "delegate"}, work_orders=work_orders), client, _claim())

    assert action.outcome == TurnOutcome.SUCCEEDED
    assert action.final_payload["provision_turn_id"] == "P-current"
    assert action.final_payload["child_turn_id"] == "C-current"
    assert client.dispatch_calls == []


def test_supervisor_fails_duplicate_work_order_task_id():
    work_orders = (_work_order("task_same", "One"), _work_order("task_same", "Two"))

    class Client:
        def fetch_turn_feed(self, *args, **kwargs):
            raise AssertionError("duplicate task ids should fail before feed readback")

    action = SupervisorTurnHandler(
        loop=_Loop(),
        provisioner_agent=AgentRef(project_id="demo", agent_id="provisioner"),
    ).handle_turn(_context({"task": "delegate"}, work_orders=work_orders), Client(), _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "invalid_work_order_task_ids"
    assert "duplicate work order task_id" in action.final_payload["message"]


def test_supervisor_fails_invalid_work_order_task_id_before_feed_readback():
    work_orders = (_work_order("bad task", "One"),)

    class Client:
        def fetch_turn_feed(self, *args, **kwargs):
            raise AssertionError("invalid task ids should fail before feed readback")

    action = SupervisorTurnHandler(
        loop=_Loop(),
        provisioner_agent=AgentRef(project_id="demo", agent_id="provisioner"),
    ).handle_turn(_context({"task": "delegate"}, work_orders=work_orders), Client(), _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload["error"] == "invalid_work_order_task_ids"
    assert "contain only" in action.final_payload["message"]


def test_supervisor_fails_provision_task_id_mismatch():
    work_orders = (_work_order("task_a", "Do A"),)
    client = _CorrelationClient(
        events=(SimpleNamespace(event_type="turn.spawned", subject_id="P-a"),),
        snapshots={"P-a": SimpleNamespace(turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1, outcome=TurnOutcome.SUCCEEDED)},
        contexts={"P-a": _provision_context(task_id="task_a", deliverable_task_id="task_b", leaf_agent_id="leaf-a")},
    )

    action = SupervisorTurnHandler(
        loop=_Loop(),
        provisioner_agent=AgentRef(project_id="demo", agent_id="provisioner"),
    ).handle_turn(_context({"task": "delegate"}, work_orders=work_orders), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload == {
        "error": "provision_task_id_mismatch",
        "provision_turn_id": "P-a",
        "expected_task_id": "task_a",
        "actual_task_id": "task_b",
    }


def test_supervisor_fails_child_target_agent_mismatch():
    work_orders = (_work_order("task_a", "Do A"),)
    provisioner = AgentRef(project_id="demo", agent_id="provisioner")
    expected_leaf = AgentRef(project_id="demo", agent_id="leaf-a")
    actual_leaf = AgentRef(project_id="demo", agent_id="leaf-other")
    client = _CorrelationClient(
        events=(
            SimpleNamespace(event_type="turn.spawned", subject_id="P-a"),
            SimpleNamespace(event_type="turn.spawned", subject_id="C-a"),
        ),
        snapshots={
            "P-a": SimpleNamespace(
                turn_kind=TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                outcome=TurnOutcome.SUCCEEDED,
                target_agent=provisioner,
            ),
            "C-a": SimpleNamespace(
                turn_kind=TURN_KIND_CONVERSATION_V1,
                outcome=TurnOutcome.SUCCEEDED,
                target_agent=actual_leaf,
            ),
        },
        contexts={
            "P-a": _provision_context(task_id="task_a", leaf_agent_id=expected_leaf.agent_id),
            "C-a": _child_context(task_id="task_a", child_turn_id="C-a"),
        },
    )

    action = SupervisorTurnHandler(
        loop=_Loop(),
        provisioner_agent=provisioner,
    ).handle_turn(_context({"task": "delegate"}, work_orders=work_orders), client, _claim())

    assert action.outcome == TurnOutcome.FAILED
    assert action.final_payload == {
        "error": "child_target_agent_mismatch",
        "task_id": "task_a",
        "child_turn_id": "C-a",
        "expected_target_agent_ref": {"project_id": "demo", "agent_id": "leaf-a"},
        "actual_target_agent_ref": {"project_id": "demo", "agent_id": "leaf-other"},
    }


def test_supervisor_uses_task_id_dispatch_keys_for_missing_turns():
    work_orders = (_work_order("task_a", "Do A"),)
    client = _CorrelationClient(events=(), snapshots={}, contexts={})

    action = SupervisorTurnHandler(
        loop=_Loop(),
        provisioner_agent=AgentRef(project_id="demo", agent_id="provisioner"),
    ).handle_turn(_context({"task": "delegate"}, work_orders=work_orders), client, _claim())

    assert isinstance(action, SuspendTurnAction)
    assert client.dispatch_calls[0]["dispatch_key"] == "T-parent:provision:leaf:task_a"
    assert client.dispatch_calls[0]["input_payload"]["parent_turn_id"] == "T-parent"
    assert client.dispatch_calls[0]["input_payload"]["work_order"]["task_id"] == "task_a"
