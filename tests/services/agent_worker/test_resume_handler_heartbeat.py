from types import SimpleNamespace

import pytest

from core.cg_context import CGContext
from core.utils import safe_str
from core.status import STATUS_SUCCESS
from services.agent_worker.models import AgentTurnWorkItem
from services.agent_worker.resume_handler import ResumeHandler
from infra.tool_executor import ToolResultBuilder, ToolResultContext


def _make_item(
    *,
    project_id: str,
    channel_id: str,
    agent_turn_id: str,
    agent_id: str,
    turn_epoch: int,
    headers: dict,
    profile_box_id=None,
    context_box_id=None,
    output_box_id=None,
    resume_data=None,
    stop_data=None,
    tool_call_id=None,
):
    resolved_tool_call_id = safe_str(tool_call_id)
    if not resolved_tool_call_id and isinstance(resume_data, dict):
        resolved_tool_call_id = safe_str(resume_data.get("tool_call_id"))
    return AgentTurnWorkItem(
        ctx=CGContext(
            project_id=project_id,
            channel_id=channel_id,
            agent_turn_id=agent_turn_id,
            agent_id=agent_id,
            turn_epoch=int(turn_epoch),
            tool_call_id=resolved_tool_call_id,
            headers=dict(headers or {}),
        ),
        profile_box_id=profile_box_id,
        context_box_id=context_box_id,
        output_box_id=output_box_id,
        resume_data=resume_data,
        stop_data=stop_data,
    )


class _DummyStateStore:
    def __init__(
        self,
        state,
        *,
        waiting_rows=None,
        update_results=None,
        finish_turn_idle_result: bool = True,
        apply_result: bool = True,
    ):
        self.state = state
        self.update_calls = []
        self.finish_turn_idle_calls = []
        self.received_calls = []
        self.applied_calls = []
        self._update_results = list(update_results or [])
        self.finish_turn_idle_result = bool(finish_turn_idle_result)
        self.apply_result = bool(apply_result)
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

    async def fetch(self, ctx):
        return self.state

    async def update(self, **kwargs):
        self.update_calls.append(kwargs)
        result = self._update_results.pop(0) if self._update_results else True
        if not result:
            return False
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

    async def finish_turn_idle(self, **kwargs):
        self.finish_turn_idle_calls.append(kwargs)
        if not self.finish_turn_idle_result:
            return False
        self.state.status = "idle"
        if kwargs.get("last_output_box_id") is not None:
            self.state.output_box_id = kwargs.get("last_output_box_id")
        self.state.turn_epoch = int(self.state.turn_epoch) + 1
        return True

    async def get_turn_waiting_tool(
        self,
        *,
        ctx,
        tool_call_id=None,
    ):
        resolved_tool_call_id = str(tool_call_id or getattr(ctx, "tool_call_id", None) or "")
        row = self.waiting_by_call.get(resolved_tool_call_id)
        return dict(row) if row else None

    async def mark_turn_waiting_tool_received(
        self,
        *,
        ctx,
        tool_result_card_id,
        step_id=None,
        tool_name=None,
        conn=None,
    ):
        _ = conn
        self.received_calls.append(
            {
                "ctx": ctx,
                "tool_result_card_id": tool_result_card_id,
                "tool_name": tool_name,
            }
        )
        row = self.waiting_by_call.get(str(getattr(ctx, "tool_call_id", "") or ""))
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
        ctx,
        tool_result_card_id=None,
        conn=None,
    ):
        _ = conn
        self.applied_calls.append(
            {
                "ctx": ctx,
                "tool_result_card_id": tool_result_card_id,
            }
        )
        row = self.waiting_by_call.get(str(getattr(ctx, "tool_call_id", "") or ""))
        if not row or not self.apply_result:
            return False
        row["wait_status"] = "applied"
        if tool_result_card_id:
            row["tool_result_card_id"] = tool_result_card_id
        return True

    async def list_turn_waiting_tools(
        self,
        *,
        ctx,
        statuses=None,
    ):
        _ = ctx
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


class _DummyNats:
    def __init__(self):
        self.events = []

    async def publish_event(self, subject, payload, headers=None):
        self.events.append((subject, payload, dict(headers or {})))


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


class _DuplicateOnSaveCardBox(_DummyCardBox):
    def __init__(self, tool_result, thought_card):
        super().__init__(tool_result, thought_card)
        self.lookup_count = 0
        self.save_calls = 0

    async def get_cards_by_tool_call_ids(self, *, project_id, tool_call_ids, conn=None):
        _ = project_id, tool_call_ids, conn
        self.lookup_count += 1
        if self.lookup_count >= 2:
            return [self.tool_result]
        return []

    async def save_card(self, card, conn=None):
        _ = card, conn
        self.save_calls += 1
        raise RuntimeError('duplicate key value violates unique constraint "uniq_cards_tool_result_call"')


def _build_tool_result(
    *,
    tool_call_id: str,
    result: dict,
    function_name: str,
    after_execution: str = "suspend",
):
    cmd_data = {
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "agent_id": "agent_1",
        "tool_name": function_name,
        "after_execution": after_execution,
    }
    tool_call_meta = {"step_id": "step_dispatch"}
    cg_ctx = CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=1,
        step_id="step_dispatch",
        tool_call_id=tool_call_id,
    )
    result_context = ToolResultContext.from_cmd_data(
        ctx=cg_ctx,
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
        after_execution=after_execution,
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

    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
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

    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert state_store.update_calls == []
    assert item.inbox_finalize_action == "defer"
    assert item.retry_reason == "turn_running_not_suspended"
    assert item.retry_delay_seconds == pytest.approx(1.0)


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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)
    assert cardbox.append_calls == []
    assert state_store.update_calls == []
    assert item.inbox_finalize_action == "defer"
    assert item.retry_reason == "turn_running_not_suspended"
    assert item.retry_delay_seconds == pytest.approx(1.0)

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
    assert queue.items[0].ctx.turn_epoch == state.turn_epoch
    assert item.ctx.turn_epoch == state.turn_epoch
    assert item.inbox_finalize_action == "consume"
    assert item.retry_reason is None
    assert item.retry_delay_seconds == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_resume_handler_defers_when_tool_result_card_missing(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_missing_card"
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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": "missing_card_id",
        },
    )

    await handler.handle(item)

    assert item.inbox_finalize_action == "defer"
    assert item.retry_reason == "tool_result_card_not_found"
    assert item.retry_delay_seconds == pytest.approx(1.5)
    assert cardbox.append_calls == []


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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": "missing_card_id",
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert state_store.update_calls == []


@pytest.mark.asyncio
async def test_resume_handler_duplicate_applied_resume_is_idempotent_consume(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_applied"
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
        waiting_tool_count=0,
        parent_step_id=None,
        trace_id=None,
    )
    state_store = _DummyStateStore(
        state,
        waiting_rows=[{"tool_call_id": tool_call_id, "wait_status": "applied"}],
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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert state_store.update_calls == []
    assert item.inbox_finalize_action == "consume"
    assert item.retry_reason is None


@pytest.mark.asyncio
async def test_resume_handler_claim_lost_skips_append_and_state_transition(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_claim_lost"
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
        apply_result=False,
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

    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert len(state_store.received_calls) == 1
    assert len(state_store.applied_calls) == 1
    assert cardbox.append_calls == []
    assert state_store.update_calls == []
    assert state_store.waiting_by_call[tool_call_id]["wait_status"] == "received"
    assert item.inbox_finalize_action == "consume"
    assert item.retry_reason is None


@pytest.mark.asyncio
async def test_resume_handler_guard_mismatch_consumes_without_defer(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_guard_mismatch"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(type="agent.thought", tool_calls=[{"id": tool_call_id}])
    cardbox = _DummyCardBox(tool_result=tool_result, thought_card=thought_card)
    state = SimpleNamespace(
        active_agent_turn_id="turn_other",
        turn_epoch=2,
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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert cardbox.append_calls == []
    assert state_store.update_calls == []
    assert item.inbox_finalize_action == "consume"
    assert item.retry_reason is None


@pytest.mark.asyncio
async def test_resume_handler_defers_when_running_cas_fails(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_running_cas_fail"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
        after_execution="suspend",
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
        update_results=[False],
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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert item.inbox_finalize_action == "defer"
    assert item.retry_reason == "resume_cas_failed_running"
    assert item.retry_delay_seconds == pytest.approx(1.0)
    assert queue.items == []


@pytest.mark.asyncio
async def test_resume_handler_defers_when_terminate_cas_fails(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr("services.agent_worker.shared.emit_agent_step", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_terminate_cas_fail"
    _, tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"done": True},
        function_name="skills.task_watch",
        after_execution="terminate",
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
        finish_turn_idle_result=False,
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
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        profile_box_id=None,
        context_box_id=None,
        output_box_id="box_1",
        turn_epoch=1,
        headers={},
        tool_call_id=tool_call_id,
        resume_data={
            "status": "success",
            "after_execution": "terminate",
            "tool_result_card_id": tool_result.card_id,
        },
    )

    await handler.handle(item)

    assert item.inbox_finalize_action == "defer"
    assert item.retry_reason == "resume_cas_failed_terminate"
    assert item.retry_delay_seconds == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_resume_handler_fail_resume_reuses_existing_tool_result_on_duplicate(monkeypatch) -> None:
    async def _noop(*args, **kwargs):
        return None

    import services.agent_worker.resume_handler as resume_handler

    monkeypatch.setattr(resume_handler, "emit_agent_state", _noop)
    monkeypatch.setattr(resume_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(resume_handler, "publish_idle_wakeup", _noop)

    tool_call_id = "call_dup_resume"
    _, existing_tool_result = _build_tool_result(
        tool_call_id=tool_call_id,
        result={"ok": False},
        function_name="skills.task_watch",
    )
    thought_card = SimpleNamespace(
        card_id="thought_dispatch",
        type="agent.thought",
        tool_calls=[{"id": tool_call_id, "function": {"name": "skills.task_watch"}}],
        metadata={"step_id": "step_dispatch", "agent_turn_id": "turn_1"},
    )

    state = SimpleNamespace(
        status="suspended",
        active_agent_turn_id="turn_1",
        turn_epoch=1,
        active_channel_id="public",
        output_box_id="box_1",
        profile_box_id="profile_box",
        context_box_id="context_box",
        waiting_tool_count=1,
        trace_id="11111111111111111111111111111111",
        parent_step_id=None,
    )
    state_store = _DummyStateStore(state, waiting_rows=[])
    cardbox = _DuplicateOnSaveCardBox(existing_tool_result, thought_card)
    handler = ResumeHandler(
        state_store=state_store,
        step_store=_DummyStepStore(),
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=_DummyNats(),
        queue=_QueueRecorder(),
        suspend_timeout_seconds=30,
    )

    headers = {"CG-Project-Id": "proj_1", "CG-Agent-Id": "agent_1", "CG-Turn-Epoch": "1"}
    item = _make_item(
        project_id="proj_1",
        channel_id="public",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        turn_epoch=1,
        headers=headers,
        profile_box_id="profile_box",
        context_box_id="context_box",
        output_box_id="box_1",
        tool_call_id=tool_call_id,
        resume_data={
            "status": "failed",
            "after_execution": "suspend",
            "agent_turn_id": "turn_1",
            "turn_epoch": 1,
            # Trigger _fail_invalid_resume -> _fail_resume path.
            "tool_result_card_id": "",
        },
    )

    await handler.handle(item)

    assert cardbox.save_calls == 1
    assert state_store.finish_turn_idle_calls, "resume fail should still close turn to idle"
    assert cardbox.append_calls
    appended_card_ids = cardbox.append_calls[-1][1]
    assert existing_tool_result.card_id in appended_card_ids
