from types import SimpleNamespace

import pytest

from core.status import STATUS_SUCCESS
from services.agent_worker.models import AgentTurnWorkItem
from services.agent_worker.resume_handler import ResumeHandler
from infra.tool_executor import ToolResultBuilder, ToolResultContext


class _DummyStateStore:
    def __init__(self, state, *, waiting_rows=None):
        self.state = state
        self.update_calls = []
        self.ledger_pending_calls = []
        self.waiting_by_call = {}
        for row in waiting_rows or []:
            tool_call_id = str(row.get("tool_call_id") or "").strip()
            if not tool_call_id:
                continue
            self.waiting_by_call[tool_call_id] = {
                "tool_call_id": tool_call_id,
                "wait_status": str(row.get("wait_status") or "waiting"),
                "tool_result_card_id": row.get("tool_result_card_id"),
                "step_id": row.get("step_id") or "step_dispatch",
                "tool_name": row.get("tool_name") or "skills.task_watch",
            }

    async def fetch(self, project_id, agent_id):
        return self.state

    async def update(self, **kwargs):
        self.update_calls.append(kwargs)
        new_status = kwargs.get("new_status")
        if new_status is not None:
            self.state.status = new_status
        if kwargs.get("output_box_id") is not None:
            self.state.output_box_id = kwargs.get("output_box_id")
        if kwargs.get("waiting_tool_count") is not None:
            self.state.waiting_tool_count = kwargs.get("waiting_tool_count")
        if kwargs.get("bump_epoch"):
            self.state.turn_epoch = int(self.state.turn_epoch) + 1
        return True

    async def get_turn_waiting_tool(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
    ):
        row = self.waiting_by_call.get(str(tool_call_id))
        return dict(row) if row else None

    async def mark_turn_waiting_tool_received(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
        tool_result_card_id,
        step_id=None,
        tool_name=None,
        conn=None,
    ):
        row = self.waiting_by_call.get(str(tool_call_id))
        if not row:
            return False
        row["wait_status"] = "received"
        row["tool_result_card_id"] = tool_result_card_id
        if step_id:
            row["step_id"] = step_id
        if tool_name:
            row["tool_name"] = tool_name
        return True

    async def mark_turn_waiting_tool_applied(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
        tool_result_card_id=None,
        conn=None,
    ):
        row = self.waiting_by_call.get(str(tool_call_id))
        if not row:
            return False
        row["wait_status"] = "applied"
        if tool_result_card_id:
            row["tool_result_card_id"] = tool_result_card_id
        return True

    async def record_turn_resume_ledger(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
        tool_result_card_id,
        payload=None,
        conn=None,
    ):
        return True

    async def mark_turn_resume_ledger_applied(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
        tool_result_card_id,
        conn=None,
    ):
        return True

    async def mark_turn_resume_ledger_pending_with_backoff(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
        tool_result_card_id,
        delay_seconds,
        last_error=None,
        conn=None,
    ):
        self.ledger_pending_calls.append(
            {
                "project_id": project_id,
                "agent_id": agent_id,
                "agent_turn_id": agent_turn_id,
                "turn_epoch": turn_epoch,
                "tool_call_id": tool_call_id,
                "tool_result_card_id": tool_result_card_id,
                "delay_seconds": delay_seconds,
                "last_error": last_error,
            }
        )
        return True

    async def mark_turn_resume_ledger_dropped(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        tool_call_id,
        tool_result_card_id,
        reason=None,
        conn=None,
    ):
        return True

    async def list_turn_waiting_tools(
        self,
        *,
        project_id,
        agent_id,
        agent_turn_id,
        turn_epoch,
        statuses=None,
    ):
        if statuses:
            allowed = {str(s) for s in statuses}
            return [dict(v) for v in self.waiting_by_call.values() if v.get("wait_status") in allowed]
        return [dict(v) for v in self.waiting_by_call.values()]


class _DummyStepStore:
    def __init__(self):
        self.insert_calls = []
        self.update_calls = []

    async def insert_step(self, **kwargs):
        self.insert_calls.append(kwargs)

    async def update_step(self, **kwargs):
        self.update_calls.append(kwargs)

    async def count_react_steps(self, **kwargs):
        return 0


class _QueueRecorder:
    def __init__(self):
        self.items = []

    async def put(self, item):
        self.items.append(item)


class _DummyCardBox:
    def __init__(self, tool_result, thought_card, *, record_box_cards: bool = True):
        self.tool_result = tool_result
        self.thought_card = thought_card
        self.append_calls = []
        self.box_card_ids = []
        self.record_box_cards = record_box_cards

    async def get_cards(self, card_ids, *, project_id, conn=None):
        _ = conn
        if self.tool_result.card_id in card_ids:
            return [self.tool_result]
        if self.box_card_ids:
            return [self.tool_result]
        return []

    async def get_tool_results_by_step_and_call_ids(self, *, project_id, step_id, tool_call_ids, conn=None):
        _ = conn
        return [self.tool_result]

    async def get_latest_card_by_step_type(self, *, project_id, step_id, card_type, conn=None):
        _ = conn
        return self.thought_card

    async def ensure_box_id(self, *, project_id, box_id, conn=None):
        _ = conn
        return box_id or "box_1"

    async def append_to_box(self, box_id, card_ids, *, project_id, conn=None):
        _ = conn
        self.append_calls.append((box_id, list(card_ids)))
        if self.record_box_cards:
            self.box_card_ids.extend(card_ids)
        return box_id

    async def get_box(self, box_id, *, project_id, conn=None):
        _ = conn
        return SimpleNamespace(box_id=box_id, card_ids=list(self.box_card_ids))

    async def get_cards_by_tool_call_ids(self, *, project_id, tool_call_ids, conn=None):
        _ = conn
        return []

    async def save_card(self, card, conn=None):
        _ = conn
        return None


def _build_tool_result(*, tool_call_id: str, result: dict, function_name: str):
    cmd_data = {
        "tool_call_id": tool_call_id,
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "agent_id": "agent_1",
        "tool_name": function_name,
        "after_execution": "suspend",
    }
    tool_call_meta = {"step_id": "step_dispatch"}
    result_context = ToolResultContext.from_cmd_data(
        project_id="proj_1",
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta,
    )
    payload, card = ToolResultBuilder(
        result_context,
        author_id="tool.skills",
        function_name=function_name,
        error_source="tool",
    ).build(
        status=STATUS_SUCCESS,
        result=result,
        error=None,
        function_name=function_name,
        after_execution="suspend",
    )
    return payload, card


@pytest.mark.asyncio
async def test_resume_handler_skips_append_for_watch_heartbeat(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_1"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"task_watch": "running", "status": {"ok": True}},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(type="agent.thought", tool_calls=[{"id": tool_call_id}])
    cardbox = _DummyCardBox(
        tool_result=tool_result,
        thought_card=thought_card,
        record_box_cards=False,
    )
    state = SimpleNamespace(
        active_agent_turn_id="turn_1",
        turn_epoch=1,
        status="suspended",
        output_box_id="box_1",
        profile_box_id=None,
        context_box_id=None,
        expecting_correlation_id=None,
        waiting_tool_count=1,
        parent_step_id=None,
        trace_id=None,
    )
    state_store = _DummyStateStore(
        state,
        waiting_rows=[{"tool_call_id": tool_call_id, "wait_status": "waiting"}],
    )
    step_store = _DummyStepStore()

    handler = ResumeHandler(
        state_store=state_store,
        step_store=step_store,
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=SimpleNamespace(),
        queue=SimpleNamespace(put=_noop),
        suspend_timeout_seconds=60,
    )

    item = AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        resume_data={
            "tool_call_id": tool_call_id,
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert any(
        call.get("new_status") == "suspended"
        and call.get("resume_deadline") is not None
        and call.get("waiting_tool_count") == 1
        for call in state_store.update_calls
    )
    assert state_store.waiting_by_call[tool_call_id]["wait_status"] == "waiting"


@pytest.mark.asyncio
async def test_resume_handler_appends_for_non_heartbeat_tool_result(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_2"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(type="agent.thought", tool_calls=[{"id": tool_call_id}])
    cardbox = _DummyCardBox(tool_result=tool_result, thought_card=thought_card)
    state = SimpleNamespace(
        active_agent_turn_id="turn_1",
        turn_epoch=1,
        status="suspended",
        output_box_id="box_1",
        profile_box_id=None,
        context_box_id=None,
        expecting_correlation_id=None,
        waiting_tool_count=1,
        parent_step_id=None,
        trace_id=None,
    )
    state_store = _DummyStateStore(
        state,
        waiting_rows=[{"tool_call_id": tool_call_id, "wait_status": "waiting"}],
    )
    step_store = _DummyStepStore()

    handler = ResumeHandler(
        state_store=state_store,
        step_store=step_store,
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=SimpleNamespace(),
        queue=SimpleNamespace(put=_noop),
        suspend_timeout_seconds=60,
    )

    item = AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        resume_data={
            "tool_call_id": tool_call_id,
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == [("box_1", [tool_result.card_id])]
    assert state_store.waiting_by_call[tool_call_id]["wait_status"] == "applied"
    assert any(
        call.get("new_status") == "running" and call.get("waiting_tool_count") == 0
        for call in state_store.update_calls
    )
    assert any(
        call.get("new_status") == "running" and call.get("bump_epoch") is True
        for call in state_store.update_calls
    )


@pytest.mark.asyncio
async def test_resume_handler_consumes_early_resume_when_state_running(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_running"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(type="agent.thought", tool_calls=[{"id": tool_call_id}])
    cardbox = _DummyCardBox(tool_result=tool_result, thought_card=thought_card)
    state = SimpleNamespace(
        active_agent_turn_id="turn_1",
        turn_epoch=1,
        status="running",
        output_box_id="box_1",
        profile_box_id=None,
        context_box_id=None,
        expecting_correlation_id=None,
        waiting_tool_count=1,
        parent_step_id=None,
        trace_id=None,
    )
    state_store = _DummyStateStore(
        state,
        waiting_rows=[{"tool_call_id": tool_call_id, "wait_status": "waiting"}],
    )
    step_store = _DummyStepStore()

    handler = ResumeHandler(
        state_store=state_store,
        step_store=step_store,
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=SimpleNamespace(),
        queue=SimpleNamespace(put=_noop),
        suspend_timeout_seconds=60,
    )
    item = AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        resume_data={
            "tool_call_id": tool_call_id,
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert state_store.update_calls == []


@pytest.mark.asyncio
async def test_resume_handler_requeues_running_then_processes_after_suspended(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_race_resume"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(type="agent.thought", tool_calls=[{"id": tool_call_id}])
    cardbox = _DummyCardBox(tool_result=tool_result, thought_card=thought_card)
    state = SimpleNamespace(
        active_agent_turn_id="turn_1",
        turn_epoch=1,
        status="running",
        output_box_id="box_1",
        profile_box_id=None,
        context_box_id=None,
        expecting_correlation_id=None,
        waiting_tool_count=1,
        parent_step_id=None,
        trace_id=None,
    )
    state_store = _DummyStateStore(
        state,
        waiting_rows=[{"tool_call_id": tool_call_id, "wait_status": "waiting"}],
    )
    step_store = _DummyStepStore()
    queue = _QueueRecorder()

    handler = ResumeHandler(
        state_store=state_store,
        step_store=step_store,
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=SimpleNamespace(),
        queue=queue,
        suspend_timeout_seconds=60,
    )
    item = AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        resume_data={
            "tool_call_id": tool_call_id,
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)
    assert cardbox.append_calls == []
    assert state_store.update_calls == []
    assert state_store.ledger_pending_calls
    assert state_store.ledger_pending_calls[-1]["last_error"] == "turn_running_not_suspended"

    state.status = "suspended"
    await handler.handle(item)

    assert cardbox.append_calls == [("box_1", [tool_result.card_id])]
    assert state_store.waiting_by_call[tool_call_id]["wait_status"] == "applied"
    assert any(
        call.get("new_status") == "running" and call.get("waiting_tool_count") == 0
        for call in state_store.update_calls
    )
    assert any(
        call.get("new_status") == "running" and call.get("bump_epoch") is True
        for call in state_store.update_calls
    )
    assert len(queue.items) == 1
    assert queue.items[0].is_continuation is True
    assert queue.items[0].turn_epoch == state.turn_epoch
    assert item.turn_epoch == state.turn_epoch


@pytest.mark.asyncio
async def test_resume_handler_drops_resume_when_state_not_running_or_suspended(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_state_done"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(type="agent.thought", tool_calls=[{"id": tool_call_id}])
    cardbox = _DummyCardBox(tool_result=tool_result, thought_card=thought_card)
    state = SimpleNamespace(
        active_agent_turn_id="turn_1",
        turn_epoch=1,
        status="completed",
        output_box_id="box_1",
        profile_box_id=None,
        context_box_id=None,
        expecting_correlation_id=None,
        waiting_tool_count=0,
        parent_step_id=None,
        trace_id=None,
    )
    state_store = _DummyStateStore(state)
    step_store = _DummyStepStore()

    handler = ResumeHandler(
        state_store=state_store,
        step_store=step_store,
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=SimpleNamespace(),
        queue=SimpleNamespace(put=_noop),
        suspend_timeout_seconds=60,
    )
    item = AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        resume_data={
            "tool_call_id": tool_call_id,
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert state_store.update_calls == []


@pytest.mark.asyncio
async def test_resume_handler_drops_resume_when_tool_not_in_waiting_set(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_mismatch"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(type="agent.thought", tool_calls=[{"id": tool_call_id}])
    cardbox = _DummyCardBox(tool_result=tool_result, thought_card=thought_card)
    state = SimpleNamespace(
        active_agent_turn_id="turn_1",
        turn_epoch=1,
        status="suspended",
        output_box_id="box_1",
        profile_box_id=None,
        context_box_id=None,
        expecting_correlation_id="call_expected_other",
        waiting_tool_count=1,
        parent_step_id=None,
        trace_id=None,
    )
    state_store = _DummyStateStore(
        state,
        waiting_rows=[{"tool_call_id": "call_expected_other", "wait_status": "waiting"}],
    )
    step_store = _DummyStepStore()

    handler = ResumeHandler(
        state_store=state_store,
        step_store=step_store,
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=SimpleNamespace(),
        queue=SimpleNamespace(put=_noop),
        suspend_timeout_seconds=60,
    )
    item = AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        resume_data={
            "tool_call_id": tool_call_id,
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": "missing_card_id",
            "step_id": "step_dispatch",
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert state_store.update_calls == []
