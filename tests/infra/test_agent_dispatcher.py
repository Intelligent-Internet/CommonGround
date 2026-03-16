from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest

from core.cg_context import CGContext
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest
from infra.l0_engine import DepthPolicy, L0Result


class _DummyCardBox:
    def __init__(self) -> None:
        self.calls: list[Dict[str, Any]] = []

    async def ensure_box_id(
        self,
        *,
        project_id: str,
        box_id: Optional[str],
        conn: Any = None,
    ) -> str:
        self.calls.append(
            {
                "project_id": project_id,
                "box_id": box_id,
                "conn": conn,
            }
        )
        return str(box_id or "box_output_generated")


class _DummyExecutionStore:
    def __init__(self) -> None:
        self.pool = SimpleNamespace()


class _DummyStateStore:
    def __init__(self) -> None:
        self.pool = SimpleNamespace()


class _DummyResourceStore:
    pass


class _DummyNats:
    pass


def _source_ctx() -> CGContext:
    return CGContext(
        project_id="proj_test",
        channel_id="public",
        agent_id="sys.pmo",
        agent_turn_id="turn_parent",
        turn_epoch=7,
        step_id="step_parent",
        trace_id="11111111111111111111111111111111",
        recursion_depth=1,
        headers={"CG-Recursion-Depth": "1"},
    )


@pytest.mark.asyncio
async def test_dispatch_transactional_uses_l0_spawn_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    dispatcher = AgentDispatcher(
        resource_store=_DummyResourceStore(),
        state_store=_DummyStateStore(),
        cardbox=_DummyCardBox(),
        nats=_DummyNats(),
        execution_store=_DummyExecutionStore(),
    )
    captured: Dict[str, Any] = {}

    async def _fake_resolve_agent_target(*, resource_store, ctx, conn=None, default_target=None):  # noqa: ANN001
        _ = resource_store, conn, default_target
        captured["resolve_ctx"] = ctx
        return "worker_demo"

    async def _fake_enqueue_intent(*, source_ctx, intent, conn, wakeup):  # noqa: ANN001
        captured["source_ctx"] = source_ctx
        captured["intent"] = intent
        captured["conn"] = conn
        captured["wakeup"] = wakeup
        return L0Result(
            status="accepted",
            payload={
                "agent_turn_id": "turn_l0_1",
                "turn_epoch": 3,
                "trace_id": "22222222222222222222222222222222",
                "recursion_depth": 2,
            },
        )

    monkeypatch.setattr("infra.agent_dispatcher.resolve_agent_target", _fake_resolve_agent_target)
    monkeypatch.setattr(dispatcher.l0, "enqueue_intent", _fake_enqueue_intent)

    result, wakeup_signals = await dispatcher.dispatch_transactional(
        DispatchRequest(
            source_ctx=_source_ctx(),
            target_agent_id="agent_child",
            target_channel_id="private",
            correlation_id="batch_task_1",
            profile_box_id="profile_1",
            context_box_id="context_1",
            output_box_id="output_1",
            depth_policy=DepthPolicy.DESCEND,
        ),
        conn=object(),
    )

    assert wakeup_signals == []
    assert result.status == "accepted"
    assert result.agent_id == "agent_child"
    assert result.agent_turn_id == "turn_l0_1"
    assert result.turn_epoch == 3
    assert result.recursion_depth == 2

    resolve_ctx = captured["resolve_ctx"]
    assert resolve_ctx.project_id == "proj_test"
    assert resolve_ctx.channel_id == "private"
    assert resolve_ctx.agent_id == "agent_child"
    assert resolve_ctx.agent_turn_id == ""
    assert resolve_ctx.parent_agent_id is None
    assert resolve_ctx.parent_agent_turn_id is None
    assert resolve_ctx.parent_step_id is None

    intent = captured["intent"]
    assert intent.target_agent_id == "agent_child"
    assert intent.correlation_id == "batch_task_1"
    assert intent.depth_policy == DepthPolicy.DESCEND
