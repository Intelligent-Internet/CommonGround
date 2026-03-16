from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from core.cg_context import CGContext
from services.agent_worker.loop import AgentWorker
from services.agent_worker.models import AgentTurnWorkItem


def _make_resume_item(*, retry_count: int, retry_reason: str = "missing_tool_result") -> AgentTurnWorkItem:
    return AgentTurnWorkItem(
        ctx=CGContext(
            project_id="proj_1",
            channel_id="ch_1",
            agent_turn_id="turn_1",
            agent_id="agent_1",
            turn_epoch=1,
            headers={},
        ),
        profile_box_id=None,
        context_box_id=None,
        output_box_id=None,
        resume_data={"tool_call_id": "call_1"},
        inbox_id="inbox_1",
        inbox_retry_count=retry_count,
        inbox_finalize_action="defer",
        retry_delay_seconds=2.5,
        retry_reason=retry_reason,
    )


@pytest.mark.asyncio
async def test_finalize_inbox_defers_when_retry_threshold_not_exceeded() -> None:
    worker = AgentWorker.__new__(AgentWorker)
    worker.max_resume_defer_retries = 3
    worker.execution_store = SimpleNamespace(
        defer_inbox_with_backoff=AsyncMock(return_value=True),
        update_inbox_status=AsyncMock(return_value=True),
    )
    worker.resume_handler = SimpleNamespace(
        fail_retry_exhausted=AsyncMock(),
    )
    worker._emit_inbox_progress = AsyncMock()
    scheduled = {}

    def _record_background_task(coro, *, name: str) -> None:
        scheduled["name"] = name
        coro.close()

    worker._spawn_background_task = Mock(side_effect=_record_background_task)

    async def _noop_delayed_wakeup(**_kwargs):
        return None

    worker._schedule_delayed_wakeup = Mock(side_effect=_noop_delayed_wakeup)

    item = _make_resume_item(retry_count=1, retry_reason="state_not_found")

    await AgentWorker._finalize_inbox_item(worker, worker_id=1, item=item)

    worker.resume_handler.fail_retry_exhausted.assert_not_awaited()
    worker.execution_store.defer_inbox_with_backoff.assert_awaited_once_with(
        inbox_id="inbox_1",
        project_id="proj_1",
        delay_seconds=2.5,
        reason="state_not_found",
        last_error="state_not_found",
        expected_status="processing",
    )
    worker.execution_store.update_inbox_status.assert_not_awaited()
    worker._emit_inbox_progress.assert_awaited_once_with(
        ctx=item.ctx,
        inbox_id="inbox_1",
        status="deferred",
        reason="state_not_found",
    )
    worker._spawn_background_task.assert_called_once()
    assert scheduled["name"].startswith("deferred_wakeup:")


@pytest.mark.asyncio
async def test_finalize_inbox_marks_consumed_when_retry_threshold_exceeded() -> None:
    worker = AgentWorker.__new__(AgentWorker)
    worker.max_resume_defer_retries = 3
    worker.execution_store = SimpleNamespace(
        defer_inbox_with_backoff=AsyncMock(return_value=True),
        update_inbox_status=AsyncMock(return_value=True),
    )

    async def _fail_retry_exhausted(item: AgentTurnWorkItem, *, retry_count: int, max_retries: int) -> None:
        _ = retry_count
        _ = max_retries
        item.inbox_finalize_action = "consume"
        item.retry_delay_seconds = 0.0
        item.retry_reason = None

    worker.resume_handler = SimpleNamespace(
        fail_retry_exhausted=AsyncMock(side_effect=_fail_retry_exhausted),
    )
    worker._emit_inbox_progress = AsyncMock()
    worker._spawn_background_task = Mock()
    worker._schedule_delayed_wakeup = AsyncMock()

    item = _make_resume_item(retry_count=3)

    await AgentWorker._finalize_inbox_item(worker, worker_id=1, item=item)

    worker.resume_handler.fail_retry_exhausted.assert_awaited_once()
    worker.execution_store.defer_inbox_with_backoff.assert_not_awaited()
    worker.execution_store.update_inbox_status.assert_awaited_once_with(
        inbox_id="inbox_1",
        project_id="proj_1",
        status="consumed",
        expected_status="processing",
    )
    worker._emit_inbox_progress.assert_awaited_once_with(
        ctx=item.ctx,
        inbox_id="inbox_1",
        status="consumed",
        reason="worker_consumed",
    )
    worker._spawn_background_task.assert_not_called()


@pytest.mark.asyncio
async def test_finalize_inbox_running_resume_triggers_retry_exhausted() -> None:
    worker = AgentWorker.__new__(AgentWorker)
    worker.max_resume_defer_retries = 1
    worker.execution_store = SimpleNamespace(
        defer_inbox_with_backoff=AsyncMock(return_value=True),
        update_inbox_status=AsyncMock(return_value=True),
    )
    
    async def _fail_retry_exhausted(item: AgentTurnWorkItem, *, retry_count: int, max_retries: int) -> None:
        _ = retry_count
        _ = max_retries
        item.inbox_finalize_action = "consume"
        item.retry_delay_seconds = 0.0
        item.retry_reason = None

    worker.resume_handler = SimpleNamespace(
        fail_retry_exhausted=AsyncMock(side_effect=_fail_retry_exhausted),
    )
    worker._emit_inbox_progress = AsyncMock()
    worker._spawn_background_task = Mock(side_effect=lambda coro, *, name: coro.close())

    async def _noop_delayed_wakeup(**_kwargs):
        return None

    worker._schedule_delayed_wakeup = Mock(side_effect=_noop_delayed_wakeup)

    item = _make_resume_item(retry_count=99, retry_reason="turn_running_not_suspended")

    await AgentWorker._finalize_inbox_item(worker, worker_id=1, item=item)

    worker.resume_handler.fail_retry_exhausted.assert_awaited_once()
    worker.execution_store.defer_inbox_with_backoff.assert_not_awaited()
    worker.execution_store.update_inbox_status.assert_awaited_once_with(
        inbox_id="inbox_1",
        project_id="proj_1",
        status="consumed",
        expected_status="processing",
    )


@pytest.mark.asyncio
async def test_schedule_delayed_wakeup_rings_l0_after_sleep(monkeypatch) -> None:
    worker = AgentWorker.__new__(AgentWorker)
    worker.l0 = SimpleNamespace(wakeup=AsyncMock())
    sleep_calls = []

    async def _fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    monkeypatch.setattr("services.agent_worker.loop.asyncio.sleep", _fake_sleep)

    ctx = CGContext(
        project_id="proj_1",
        channel_id="ch_1",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        turn_epoch=1,
        headers={},
    )

    await AgentWorker._schedule_delayed_wakeup(worker, target_ctx=ctx, delay_seconds=1.5)

    assert sleep_calls == [1.5]
    worker.l0.wakeup.assert_awaited_once_with(target_ctx=ctx, reason="deferred_wakeup")


@pytest.mark.asyncio
async def test_schedule_delayed_wakeup_swallows_l0_error(monkeypatch) -> None:
    worker = AgentWorker.__new__(AgentWorker)
    worker.l0 = SimpleNamespace(wakeup=AsyncMock(side_effect=RuntimeError("nats down")))

    async def _fake_sleep(delay: float) -> None:
        _ = delay

    monkeypatch.setattr("services.agent_worker.loop.asyncio.sleep", _fake_sleep)

    ctx = CGContext(
        project_id="proj_1",
        channel_id="ch_1",
        agent_turn_id="turn_1",
        agent_id="agent_1",
        turn_epoch=1,
        headers={},
    )

    await AgentWorker._schedule_delayed_wakeup(worker, target_ctx=ctx, delay_seconds=0.0)

    worker.l0.wakeup.assert_awaited_once_with(target_ctx=ctx, reason="deferred_wakeup")
