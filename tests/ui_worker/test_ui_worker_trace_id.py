from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

import pytest

from core.config import PROTOCOL_VERSION
from core.resource_models import ToolDefinition
from core.trace import ensure_trace_headers
from services.ui_worker.loop import UIWorkerService


class DummyNats:
    def __init__(self) -> None:
        self.events: List[Tuple[str, Dict[str, Any], Dict[str, str]]] = []

    def merge_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        return dict(headers or {})

    async def publish_event(self, subject: str, payload: Dict[str, Any], headers: Dict[str, str]) -> None:
        self.events.append((subject, payload, dict(headers or {})))


class DummyResourceStore:
    async def fetch_roster(self, project_id: str, agent_id: str) -> Dict[str, Any]:
        _ = project_id, agent_id
        return {
            "worker_target": "ui_worker",
            "profile_box_id": "profile_box_1",
        }

    async def fetch_tool_definition_model(self, project_id: str, tool_name: str) -> ToolDefinition:
        return ToolDefinition(
            project_id=project_id,
            tool_name=tool_name,
            options={"ui_allowed": True},
        )


class DummyStateStore:
    async def fetch(self, project_id: str, agent_id: str) -> Any:
        _ = project_id, agent_id
        return SimpleNamespace(
            status="idle",
            active_recursion_depth=0,
            profile_box_id="profile_box_1",
            context_box_id="context_box_1",
            output_box_id="output_box_1",
        )

    async def fetch_pointers(self, project_id: str, agent_id: str) -> Any:
        _ = project_id, agent_id
        return SimpleNamespace(
            memory_context_box_id="context_box_1",
            last_output_box_id="output_box_1",
        )

    async def ensure_memory_context_box_id(self, **kwargs: Any) -> str:
        _ = kwargs
        return "context_box_1"

    async def update(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True

    async def finish_turn_idle(self, **kwargs: Any) -> bool:
        _ = kwargs
        return True


class BusyStateStore(DummyStateStore):
    async def fetch(self, project_id: str, agent_id: str) -> Any:
        _ = project_id, agent_id
        return SimpleNamespace(
            status="running",
            active_recursion_depth=0,
            profile_box_id="profile_box_1",
            context_box_id="context_box_1",
            output_box_id="output_box_1",
        )


class DummyStepStore:
    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []

    async def insert_step(self, **kwargs: Any) -> None:
        self.records.append(dict(kwargs))


class DummyDispatcher:
    def __init__(self, expected_trace_id: str) -> None:
        self.expected_trace_id = expected_trace_id
        self.calls: List[str] = []

    async def execute(self, **kwargs: Any) -> Any:
        tool_call = kwargs.get("tool_call") or {}
        assert tool_call.get("type") == "function"
        function = tool_call.get("function") or {}
        assert function.get("name") == "delegate_async"
        args = function.get("arguments") or {}
        assert args.get("target_strategy") == "reuse"
        assert args.get("target_ref") == "agent_chat"
        assert args.get("instruction") == "hello"
        ctx = kwargs.get("ctx")
        trace_id = getattr(ctx, "trace_id", None)
        self.calls.append(trace_id)
        assert trace_id == self.expected_trace_id
        return SimpleNamespace(cards=[], suspend=True)


class DummyIdem:
    async def try_acquire(self, key: str) -> bool:
        _ = key
        return True

    async def set_result(self, key: str, payload: Dict[str, Any]) -> None:
        _ = key, payload

    async def delete(self, key: str) -> None:
        _ = key

    async def wait_result(self, key: str, timeout: float) -> Any:
        _ = key, timeout
        return None


class DummyCardbox:
    def __init__(self) -> None:
        self.append_calls: List[Tuple[str, List[str], str]] = []

    async def ensure_box_id(self, *, project_id: str, box_id: str | None, conn: Any = None) -> str:
        _ = conn
        _ = project_id
        return box_id or "output_box_1"

    async def save_box(self, card_ids: List[str], *, project_id: str, conn: Any = None) -> str:
        _ = conn
        _ = card_ids, project_id
        return "context_box_1"

    async def append_to_box(
        self, box_id: str, card_ids: List[str], project_id: str, conn: Any = None
    ) -> None:
        _ = conn
        self.append_calls.append((box_id, list(card_ids), project_id))


class _DummyTx:
    async def __aenter__(self) -> Any:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        _ = exc_type, exc, tb
        return False


class _DummyConn:
    def transaction(self) -> _DummyTx:
        return _DummyTx()


class _DummyConnCtx:
    async def __aenter__(self) -> _DummyConn:
        return _DummyConn()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        _ = exc_type, exc, tb
        return False


class DummyExecutionStore:
    class _Pool:
        def connection(self) -> _DummyConnCtx:
            return _DummyConnCtx()

    def __init__(self) -> None:
        self.pool = self._Pool()


class DummyL0:
    def __init__(self, *, ack_status: str = "accepted", ack_error_code: str | None = None) -> None:
        self.ack_status = ack_status
        self.ack_error_code = ack_error_code
        self.enqueue_calls: List[Dict[str, Any]] = []
        self.published_wakeup_signals: List[Any] = []

    async def enqueue(self, **kwargs: Any) -> Any:
        self.enqueue_calls.append(dict(kwargs))
        return SimpleNamespace(
            ack_status=self.ack_status,
            ack_error_code=self.ack_error_code,
            wakeup_signals=(),
        )

    async def publish_wakeup_signals(self, signals: Any) -> None:
        self.published_wakeup_signals.extend(list(signals or ()))


@pytest.mark.asyncio
async def test_ui_worker_process_action_uses_traceparent_trace_id() -> None:
    expected_trace_id = "11111111111111111111111111111111"
    headers, _, _ = ensure_trace_headers({}, trace_id=expected_trace_id)

    service = UIWorkerService.__new__(UIWorkerService)
    service.nats = DummyNats()
    service.resource_store = DummyResourceStore()
    service.state_store = DummyStateStore()
    service.step_store = DummyStepStore()
    service.worker_target = "ui_worker"
    service.dispatcher = DummyDispatcher(expected_trace_id)
    service.idem = DummyIdem()
    service.cardbox = DummyCardbox()

    parts = SimpleNamespace(project_id="proj_test", channel_id="public")
    payload = {
        "action_id": "act_1",
        "agent_id": "agent_ui",
        "agent_turn_id": "turn_ui_test",
        "turn_epoch": 1,
        "tool_name": "delegate_async",
        "args": {
            "target_strategy": "reuse",
            "target_ref": "agent_chat",
            "instruction": "hello",
        },
    }

    processed = await service._process_ui_action(parts=parts, payload=payload, headers=headers)

    assert processed is True
    assert service.step_store.records
    assert service.step_store.records[0]["trace_id"] == expected_trace_id
    assert service.dispatcher.calls == [expected_trace_id]


@pytest.mark.asyncio
async def test_handle_ui_action_busy_gate_rejects_without_enqueue() -> None:
    headers, _, _ = ensure_trace_headers({}, trace_id="22222222222222222222222222222222")
    service = UIWorkerService.__new__(UIWorkerService)
    service.nats = DummyNats()
    service.resource_store = DummyResourceStore()
    service.state_store = BusyStateStore()
    service.worker_target = "ui_worker"
    service.l0 = DummyL0()

    subject = f"cg.{PROTOCOL_VERSION}.proj_test.public.cmd.sys.ui.action"
    payload = {
        "action_id": "act_busy",
        "agent_id": "agent_ui",
        "tool_name": "delegate_async",
        "args": {"target_strategy": "reuse", "target_ref": "agent_chat", "instruction": "hello"},
    }
    span = SimpleNamespace(set_attribute=lambda *args, **kwargs: None)

    await service._handle_ui_action(subject, payload, headers, _span=span)

    assert service.l0.enqueue_calls == []
    assert service.nats.events
    _, ack_payload, _ = service.nats.events[-1]
    assert ack_payload.get("status") == "busy"
    assert ack_payload.get("error_code") == "agent_busy"


@pytest.mark.asyncio
async def test_handle_ui_action_idle_path_enqueues_via_l0() -> None:
    headers, _, _ = ensure_trace_headers({}, trace_id="33333333333333333333333333333333")
    service = UIWorkerService.__new__(UIWorkerService)
    service.nats = DummyNats()
    service.resource_store = DummyResourceStore()
    service.state_store = DummyStateStore()
    service.execution_store = DummyExecutionStore()
    service.worker_target = "ui_worker"
    service.idem = DummyIdem()
    service.l0 = DummyL0(ack_status="accepted")

    subject = f"cg.{PROTOCOL_VERSION}.proj_test.public.cmd.sys.ui.action"
    payload = {
        "action_id": "act_idle",
        "agent_id": "agent_ui",
        "tool_name": "delegate_async",
        "args": {"target_strategy": "reuse", "target_ref": "agent_chat", "instruction": "hello"},
    }
    span = SimpleNamespace(set_attribute=lambda *args, **kwargs: None)

    await service._handle_ui_action(subject, payload, headers, _span=span)

    assert len(service.l0.enqueue_calls) == 1
    call = service.l0.enqueue_calls[0]
    assert call.get("message_type") == "ui_action"
    assert "drop_if_busy" not in call
    assert service.nats.events
    _, ack_payload, _ = service.nats.events[-1]
    assert ack_payload.get("status") == "accepted"
