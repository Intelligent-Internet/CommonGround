from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional

import pytest

from core.cg_context import CGContext
from core.errors import BadRequestError
from services.tools.tool_caller import ToolCaller


class _Tx:
    async def __aenter__(self) -> Any:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        _ = exc_type, exc, tb
        return False


class _Conn:
    def transaction(self) -> _Tx:
        return _Tx()


class _ConnCtx:
    async def __aenter__(self) -> _Conn:
        return _Conn()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        _ = exc_type, exc, tb
        return False


class _Pool:
    def connection(self) -> _ConnCtx:
        return _ConnCtx()


@dataclass
class _DummyToolDef:
    target_subject: str
    after_execution: str


class _DummyResourceStore:
    async def fetch_tool_definition_model(self, project_id: str, tool_name: str) -> Any:
        _ = project_id, tool_name
        return _DummyToolDef(
            target_subject="cg.v1r4.{project_id}.{channel_id}.cmd.sys.pmo.internal.delegate_async",
            after_execution="suspend",
        )


class _DummyExecutionStore:
    pool: Any = _Pool()

    async def get_active_recursion_depth(self, *, ctx: CGContext) -> Optional[int]:
        _ = ctx
        return 0


class _DummyCardBox:
    def __init__(self) -> None:
        self.saved_cards = []

    async def save_card(self, card: Any) -> None:
        self.saved_cards.append(card)


class _DummyL0:
    def __init__(self) -> None:
        self.last_intent = None
        self.published = []

    async def command_intent(self, *, source_ctx: CGContext, intent: Any, conn: Any) -> Any:
        _ = source_ctx, conn
        self.last_intent = intent
        return SimpleNamespace(
            status="accepted",
            error_code=None,
            command_signals=[
                SimpleNamespace(subject=str(intent.subject), payload=dict(intent.payload or {}), headers={})
            ],
        )

    async def publish_command_signals(self, signals: Any) -> None:
        self.published.append(list(signals or ()))


def _build_caller() -> tuple[ToolCaller, _DummyL0]:
    caller = ToolCaller(
        cardbox=_DummyCardBox(),
        nats=SimpleNamespace(),
        resource_store=_DummyResourceStore(),
        execution_store=_DummyExecutionStore(),
    )
    fake_l0 = _DummyL0()
    caller.l0 = fake_l0  # type: ignore[assignment]
    return caller, fake_l0


def _ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        headers={},
    ).with_trace_transport(base_headers={}, trace_id="11111111111111111111111111111111", default_depth=0)


@pytest.mark.asyncio
async def test_tool_caller_after_execution_override_applied() -> None:
    caller, fake_l0 = _build_caller()
    result = await caller.call(
        caller_ctx=_ctx(),
        tool_name="delegate_async",
        args={"instruction": "hi"},
        after_execution_override="terminate",
        tool_call_id="call_custom_1",
        step_id="step_custom_1",
        agent_turn_id="turn_custom_1",
        await_result=False,
        ack_result=False,
    )
    assert fake_l0.last_intent is not None
    assert fake_l0.last_intent.correlation_id == "call_custom_1"
    assert fake_l0.last_intent.payload["after_execution"] == "terminate"
    assert result.tool_call_id == "call_custom_1"
    assert result.step_id == "step_custom_1"
    assert result.agent_turn_id == "turn_custom_1"


@pytest.mark.asyncio
async def test_tool_caller_uses_tool_default_after_execution() -> None:
    caller, fake_l0 = _build_caller()
    await caller.call(
        caller_ctx=_ctx(),
        tool_name="delegate_async",
        args={"instruction": "hi"},
        await_result=False,
        ack_result=False,
    )
    assert fake_l0.last_intent is not None
    assert fake_l0.last_intent.payload["after_execution"] == "suspend"


@pytest.mark.asyncio
async def test_tool_caller_invalid_after_execution_override_rejected() -> None:
    caller, _ = _build_caller()
    with pytest.raises(BadRequestError):
        await caller.call(
            caller_ctx=_ctx(),
            tool_name="delegate_async",
            args={},
            after_execution_override="unknown",
            await_result=False,
            ack_result=False,
        )
