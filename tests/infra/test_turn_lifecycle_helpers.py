from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from core.cg_context import CGContext
import infra.l1_helpers.turn_lifecycle as turn_lifecycle


class _DummyStateStore:
    def __init__(self, *, updated: bool) -> None:
        self.updated = bool(updated)
        self.finish_calls: List[Dict[str, Any]] = []

    async def finish_turn_idle_transition(self, **kwargs: Any):
        self.finish_calls.append(dict(kwargs))
        if not self.updated:
            return None
        ctx = kwargs["ctx"]
        next_epoch = int(ctx.turn_epoch) + (1 if kwargs.get("bump_epoch") else 0)
        return SimpleNamespace(committed_ctx=ctx.with_bumped_epoch(next_epoch))


def _ctx() -> CGContext:
    return CGContext(
        project_id="proj_test",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=1,
    )


@pytest.mark.asyncio
async def test_finish_turn_idle_and_emit_returns_false_on_cas_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store = _DummyStateStore(updated=False)
    emitted: List[str] = []

    async def _emit_state(**kwargs: Any) -> None:
        _ = kwargs
        emitted.append("state")

    async def _emit_task(**kwargs: Any) -> None:
        _ = kwargs
        emitted.append("task")

    monkeypatch.setattr(turn_lifecycle, "emit_agent_state", _emit_state)
    monkeypatch.setattr(turn_lifecycle, "emit_agent_task", _emit_task)

    committed_ctx = await turn_lifecycle.finish_turn_idle_and_emit(
        state_store=state_store,
        nats=object(),
        ctx=_ctx(),
        last_output_box_id="out_1",
        task_status="failed",
    )

    assert committed_ctx is None
    assert len(state_store.finish_calls) == 1
    assert emitted == []


@pytest.mark.asyncio
async def test_finish_turn_idle_and_emit_emits_state_and_task(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store = _DummyStateStore(updated=True)
    state_calls: List[Dict[str, Any]] = []
    task_calls: List[Dict[str, Any]] = []

    async def _emit_state(**kwargs: Any) -> None:
        state_calls.append(dict(kwargs))

    async def _emit_task(**kwargs: Any) -> None:
        task_calls.append(dict(kwargs))

    monkeypatch.setattr(turn_lifecycle, "emit_agent_state", _emit_state)
    monkeypatch.setattr(turn_lifecycle, "emit_agent_task", _emit_task)

    committed_ctx = await turn_lifecycle.finish_turn_idle_and_emit(
        state_store=state_store,
        nats="nats",
        ctx=_ctx(),
        last_output_box_id="out_1",
        state_metadata={"error": "timeout"},
        task_status="failed",
        deliverable_card_id="card_1",
        task_error="timeout",
        task_stats={"attempt": 1},
    )

    assert committed_ctx is not None
    assert len(state_calls) == 1
    assert state_calls[0]["status"] == "idle"
    assert state_calls[0]["metadata"] == {"error": "timeout"}
    assert state_calls[0]["ctx"].turn_epoch == 1
    assert len(task_calls) == 1
    assert task_calls[0]["status"] == "failed"
    assert task_calls[0]["deliverable_card_id"] == "card_1"
    assert task_calls[0]["stats"] == {"attempt": 1}
    assert task_calls[0]["ctx"].turn_epoch == 1


@pytest.mark.asyncio
async def test_finish_turn_idle_and_emit_uses_committed_ctx_for_bumped_epoch(monkeypatch: pytest.MonkeyPatch) -> None:
    state_store = _DummyStateStore(updated=True)
    state_calls: List[Dict[str, Any]] = []
    task_calls: List[Dict[str, Any]] = []

    async def _emit_state(**kwargs: Any) -> None:
        state_calls.append(dict(kwargs))

    async def _emit_task(**kwargs: Any) -> None:
        task_calls.append(dict(kwargs))

    monkeypatch.setattr(turn_lifecycle, "emit_agent_state", _emit_state)
    monkeypatch.setattr(turn_lifecycle, "emit_agent_task", _emit_task)

    committed_ctx = await turn_lifecycle.finish_turn_idle_and_emit(
        state_store=state_store,
        nats="nats",
        ctx=_ctx(),
        last_output_box_id="out_1",
        bump_epoch=True,
        task_status="failed",
    )

    assert committed_ctx is not None
    assert committed_ctx.turn_epoch == 2
    assert state_calls[0]["ctx"].turn_epoch == 2
    assert task_calls[0]["ctx"].turn_epoch == 2
