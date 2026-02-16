from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pytest

from infra.l0_engine import L0Engine


class DummyNats:
    def merge_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        return dict(headers or {})


class DummyExecutionStore:
    def __init__(self) -> None:
        self.enqueues: List[Tuple[Any, Any]] = []

    async def get_request_recursion_depth_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        correlation_id: str,
    ) -> Optional[int]:
        _ = conn, project_id, correlation_id
        return None

    async def enqueue_with_conn(self, conn: Any, *, inbox: Any, edge: Any) -> None:
        _ = conn
        self.enqueues.append((inbox, edge))


class DummyResourceStore:
    pass


class DummyStateStore:
    def __init__(self, leased_turn_epoch: Optional[int]) -> None:
        self.leased_turn_epoch = leased_turn_epoch
        self.calls: List[Dict[str, Any]] = []

    async def lease_agent_turn_with_conn(self, conn: Any, **kwargs: Any) -> Optional[int]:
        _ = conn
        self.calls.append(dict(kwargs))
        return self.leased_turn_epoch


@pytest.mark.asyncio
async def test_enqueue_ui_action_leases_turn_and_patches_payload() -> None:
    execution_store = DummyExecutionStore()
    state_store = DummyStateStore(leased_turn_epoch=3)
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=state_store,
    )

    result = await engine._enqueue_with_existing_conn(
        object(),
        project_id="proj_test",
        channel_id="public",
        target_agent_id="ui_agent",
        message_type="ui_action",
        payload={
            "action_id": "act_1",
            "agent_id": "ui_agent",
            "tool_name": "delegate_async",
            "args": {"target_strategy": "reuse", "target_ref": "chat_agent", "instruction": "hello"},
        },
        correlation_id="act_1",
        recursion_depth=0,
        headers={},
        trace_id="11111111111111111111111111111111",
        parent_step_id="step_test",
        source_agent_id=None,
        source_agent_turn_id=None,
        source_step_id=None,
        target_agent_turn_id=None,
        enqueue_mode="call",
        wakeup=False,
    )

    assert result.status == "accepted"
    assert result.error_code is None
    assert result.payload.get("turn_epoch") == 3
    assert str(result.payload.get("agent_turn_id") or "").startswith("turn_")

    assert len(state_store.calls) == 1
    assert len(execution_store.enqueues) == 1
    inbox, _edge = execution_store.enqueues[0]
    assert inbox.message_type == "ui_action"
    assert inbox.payload.get("turn_epoch") == 3
    assert inbox.payload.get("agent_turn_id") == result.payload.get("agent_turn_id")


@pytest.mark.asyncio
async def test_enqueue_ui_action_returns_busy_when_lease_fails() -> None:
    execution_store = DummyExecutionStore()
    state_store = DummyStateStore(leased_turn_epoch=None)
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=state_store,
    )

    result = await engine._enqueue_with_existing_conn(
        object(),
        project_id="proj_test",
        channel_id="public",
        target_agent_id="ui_agent",
        message_type="ui_action",
        payload={
            "action_id": "act_1",
            "agent_id": "ui_agent",
            "tool_name": "delegate_async",
            "args": {"target_strategy": "reuse", "target_ref": "chat_agent", "instruction": "hello"},
        },
        correlation_id="act_1",
        recursion_depth=0,
        headers={},
        trace_id="11111111111111111111111111111111",
        parent_step_id="step_test",
        source_agent_id=None,
        source_agent_turn_id=None,
        source_step_id=None,
        target_agent_turn_id=None,
        enqueue_mode="call",
        wakeup=False,
    )

    assert result.status == "busy"
    assert result.error_code == "agent_busy"
    assert execution_store.enqueues == []
