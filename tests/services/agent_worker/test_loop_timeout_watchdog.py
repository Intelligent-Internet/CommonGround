from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import services.agent_worker.loop as loop_module
from services.agent_worker.loop import AgentWorker


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


@dataclass(frozen=True)
class _DummyContent:
    tool_name: str


@pytest.mark.asyncio
async def test_inject_timeout_reports_uses_suspend_when_tool_definition_lookup_fails() -> None:
    worker = AgentWorker.__new__(AgentWorker)
    worker.state_store = SimpleNamespace(
        list_timed_out_waiting_tools=AsyncMock(
            return_value=[
                {
                    "project_id": "proj_1",
                    "agent_id": "agent_1",
                    "active_agent_turn_id": "turn_1",
                    "tool_call_id": "tc_1",
                    "active_channel_id": "public",
                    "turn_epoch": 3,
                    "step_id": "step_dispatch_1",
                    "parent_step_id": "step_parent_1",
                    "trace_id": "11111111111111111111111111111111",
                    "active_recursion_depth": 1,
                }
            ]
        ),
        update=AsyncMock(return_value=True),
    )
    worker.resource_store = SimpleNamespace(
        fetch_tool_definition=AsyncMock(side_effect=RuntimeError("db unavailable")),
    )
    worker.cardbox = SimpleNamespace(save_card=AsyncMock())
    worker.execution_store = SimpleNamespace(pool=_Pool())
    worker.l0 = SimpleNamespace(
        report_intent=AsyncMock(
            return_value=SimpleNamespace(
                status="accepted",
                error_code=None,
                wakeup_signals=(),
                payload={},
            )
        ),
        publish_wakeup_signals=AsyncMock(),
    )
    worker._watchdog_load_tool_call_card = AsyncMock(
        return_value=SimpleNamespace(
            type="tool.call",
            content=_DummyContent(tool_name="skills.task_watch"),
            metadata={"step_id": "step_dispatch_1"},
        )
    )

    await AgentWorker._inject_timeout_reports(worker)

    worker.resource_store.fetch_tool_definition.assert_awaited_once_with("proj_1", "skills.task_watch")
    worker.cardbox.save_card.assert_awaited_once()
    worker.l0.report_intent.assert_awaited_once()
    report_call = worker.l0.report_intent.await_args.kwargs
    intent = report_call["intent"]
    assert intent.message_type == "timeout"
    assert intent.correlation_id == "tc_1"
    assert intent.payload["after_execution"] == "suspend"
    worker.state_store.update.assert_awaited_once()


@pytest.mark.asyncio
async def test_wakeup_due_deferred_inbox_continues_after_single_row_failure() -> None:
    wakeup_calls = []

    async def _wakeup(*, target_ctx, reason):
        wakeup_calls.append((target_ctx.agent_id, reason))
        if target_ctx.agent_id == "agent_1":
            raise RuntimeError("boom")

    worker = AgentWorker.__new__(AgentWorker)
    worker.execution_store = SimpleNamespace(
        list_due_deferred_inbox_targets=AsyncMock(
            return_value=[
                {"project_id": "proj_1", "agent_id": "agent_1", "channel_id": "public"},
                {"project_id": "proj_1", "agent_id": "agent_2", "channel_id": "public"},
            ]
        )
    )
    worker.l0 = SimpleNamespace(wakeup=AsyncMock(side_effect=_wakeup))

    await AgentWorker._wakeup_due_deferred_inbox(worker)

    assert wakeup_calls == [
        ("agent_1", "watchdog_due_deferred"),
        ("agent_2", "watchdog_due_deferred"),
    ]


@pytest.mark.asyncio
async def test_enqueue_suspended_reconcile_resumes_uses_state_head_context_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper_calls = []
    wait_calls = []

    helper_ctx = loop_module.CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=3,
        trace_id="1" * 32,
    )

    def _build_state_head_context(head):  # noqa: ANN001
        helper_calls.append(head)
        return helper_ctx

    async def _list_turn_waiting_tools(*, ctx, statuses):  # noqa: ANN001
        wait_calls.append((ctx, tuple(statuses)))
        return []

    monkeypatch.setattr(loop_module, "build_state_head_context", _build_state_head_context)

    worker = AgentWorker.__new__(AgentWorker)
    worker.state_store = SimpleNamespace(
        list_suspended_heads_with_waiting=AsyncMock(
            return_value=[
                SimpleNamespace(
                    project_id="proj_1",
                    agent_id="agent_1",
                    active_agent_turn_id="turn_1",
                    active_channel_id="public",
                    turn_epoch=3,
                    active_recursion_depth=1,
                    trace_id="1" * 32,
                    parent_step_id="step_1",
                )
            ]
        ),
        list_turn_waiting_tools=_list_turn_waiting_tools,
    )
    worker.execution_store = SimpleNamespace()

    await AgentWorker._enqueue_suspended_reconcile_resumes(worker, limit=10, include_received=True)

    assert len(helper_calls) == 1
    assert len(wait_calls) == 1
    assert wait_calls[0][0] == helper_ctx
    assert wait_calls[0][1] == ("waiting", "received")
