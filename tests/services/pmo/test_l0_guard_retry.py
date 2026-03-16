from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import services.pmo.l0_guard.guard as guard_module
from services.pmo.l0_guard.guard import L0Guard


class _Tx:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        _ = exc_type, exc, tb
        return False


class _Conn:
    def transaction(self) -> _Tx:
        return _Tx()


class _ConnCtx:
    async def __aenter__(self) -> _Conn:
        return _Conn()

    async def __aexit__(self, exc_type, exc, tb):
        _ = exc_type, exc, tb
        return False


class _Pool:
    def connection(self) -> _ConnCtx:
        return _ConnCtx()


@pytest.mark.asyncio
async def test_retry_stuck_dispatched_uses_unique_correlation_and_wakeup() -> None:
    turn_id = "turn_watchdog_retry_1"
    head = SimpleNamespace(
        project_id="proj_watchdog",
        agent_id="agent_watchdog",
        active_agent_turn_id=turn_id,
        active_channel_id="public",
        turn_epoch=7,
        active_recursion_depth=1,
        trace_id="1" * 32,
        parent_step_id="step_parent_1",
        profile_box_id="profile_box_1",
        context_box_id="context_box_1",
        output_box_id="output_box_1",
    )

    guard = L0Guard.__new__(L0Guard)
    guard.dispatched_retry_seconds = 30.0
    guard.state_store = SimpleNamespace(
        list_stuck_dispatched=AsyncMock(return_value=[head]),
        update=AsyncMock(return_value=True),
    )
    guard.execution_store = SimpleNamespace(pool=_Pool())

    captured_correlation_ids: list[str] = []

    async def _enqueue_intent(*, source_ctx, intent, conn, wakeup):
        _ = source_ctx, conn, wakeup
        correlation_id = str(intent.correlation_id or "")
        captured_correlation_ids.append(correlation_id)
        # Reproduce old bug behavior: reusing turn_id as correlation_id gets rejected.
        if correlation_id == turn_id:
            return SimpleNamespace(
                status="rejected",
                error_code="protocol_violation",
                wakeup_signals=(),
                payload={},
            )
        return SimpleNamespace(
            status="accepted",
            error_code=None,
            wakeup_signals=(),
            payload={},
        )

    guard.l0 = SimpleNamespace(
        enqueue_intent=AsyncMock(side_effect=_enqueue_intent),
        publish_wakeup_signals=AsyncMock(),
        wakeup=AsyncMock(),
    )

    await L0Guard.retry_stuck_dispatched(guard)

    guard.state_store.list_stuck_dispatched.assert_awaited_once_with(
        older_than_seconds=30.0,
        limit=200,
    )
    guard.state_store.update.assert_awaited_once()
    update_call = guard.state_store.update.await_args.kwargs
    assert update_call["new_status"] == "dispatched"
    assert update_call["ctx"].agent_turn_id == turn_id

    assert len(captured_correlation_ids) == 1
    retry_correlation_id = captured_correlation_ids[0]
    assert retry_correlation_id != turn_id
    assert retry_correlation_id.startswith(f"{turn_id}:watchdog_retransmit:")

    guard.l0.publish_wakeup_signals.assert_not_awaited()
    guard.l0.wakeup.assert_awaited_once()
    wakeup_kwargs = guard.l0.wakeup.await_args.kwargs
    assert wakeup_kwargs["reason"] == "watchdog_retransmit"
    assert wakeup_kwargs["metadata"] == {"agent_turn_id": turn_id}


@pytest.mark.asyncio
async def test_reap_stuck_dispatched_emits_task_with_committed_epoch(monkeypatch: pytest.MonkeyPatch) -> None:
    head = SimpleNamespace(
        project_id="proj_watchdog",
        agent_id="agent_watchdog",
        active_agent_turn_id="turn_watchdog",
        active_channel_id="public",
        turn_epoch=7,
        active_recursion_depth=1,
        trace_id="1" * 32,
        parent_step_id="step_parent_1",
        profile_box_id="profile_box_1",
        context_box_id="context_box_1",
        output_box_id="output_box_1",
    )
    task_calls: list[dict] = []

    async def _finish_turn_idle_and_emit(**kwargs):  # noqa: ANN001
        ctx = kwargs["ctx"]
        return ctx.with_bumped_epoch(int(ctx.turn_epoch) + 1)

    async def _emit_agent_task(**kwargs):  # noqa: ANN001
        task_calls.append(dict(kwargs))

    monkeypatch.setattr(guard_module, "finish_turn_idle_and_emit", _finish_turn_idle_and_emit)
    monkeypatch.setattr(guard_module, "emit_agent_task", _emit_agent_task)

    guard = L0Guard.__new__(L0Guard)
    guard.dispatched_timeout_seconds = 30.0
    guard.state_store = SimpleNamespace(
        list_stuck_dispatched=AsyncMock(return_value=[head]),
        record_force_termination=AsyncMock(),
    )
    guard.nats = object()
    guard._ensure_watchdog_deliverable = AsyncMock(return_value="deliverable_1")
    guard._normalize_watchdog_ctx = lambda *, ctx: ctx

    await L0Guard.reap_stuck_dispatched_timeout(guard)

    assert len(task_calls) == 1
    assert task_calls[0]["ctx"].turn_epoch == 8


@pytest.mark.asyncio
async def test_reap_stuck_active_uses_committed_epoch_for_wakeup_and_events(monkeypatch: pytest.MonkeyPatch) -> None:
    head = SimpleNamespace(
        project_id="proj_watchdog",
        agent_id="agent_watchdog",
        active_agent_turn_id="turn_watchdog",
        active_channel_id="public",
        turn_epoch=4,
        active_recursion_depth=1,
        trace_id="1" * 32,
        parent_step_id="step_parent_1",
        profile_box_id="profile_box_1",
        context_box_id="context_box_1",
        output_box_id="output_box_1",
        status="running",
    )
    state_calls: list[dict] = []
    task_calls: list[dict] = []

    async def _finish_turn_idle_and_emit(**kwargs):  # noqa: ANN001
        ctx = kwargs["ctx"]
        return ctx.with_bumped_epoch(int(ctx.turn_epoch) + 1)

    async def _emit_agent_state(**kwargs):  # noqa: ANN001
        state_calls.append(dict(kwargs))

    async def _emit_agent_task(**kwargs):  # noqa: ANN001
        task_calls.append(dict(kwargs))

    monkeypatch.setattr(guard_module, "finish_turn_idle_and_emit", _finish_turn_idle_and_emit)
    monkeypatch.setattr(guard_module, "emit_agent_state", _emit_agent_state)
    monkeypatch.setattr(guard_module, "emit_agent_task", _emit_agent_task)

    guard = L0Guard.__new__(L0Guard)
    guard.active_reap_seconds = 30.0
    guard.state_store = SimpleNamespace(
        list_stuck_active=AsyncMock(return_value=[head]),
        record_force_termination=AsyncMock(),
    )
    guard.nats = object()
    guard.l0 = SimpleNamespace(wakeup=AsyncMock())
    guard._ensure_watchdog_deliverable = AsyncMock(return_value="deliverable_1")
    guard._normalize_watchdog_ctx = lambda *, ctx: ctx

    await L0Guard.reap_stuck_active(guard)

    guard.l0.wakeup.assert_awaited_once()
    assert guard.l0.wakeup.await_args.kwargs["target_ctx"].turn_epoch == 5
    assert state_calls[0]["ctx"].turn_epoch == 5
    assert task_calls[0]["ctx"].turn_epoch == 5
