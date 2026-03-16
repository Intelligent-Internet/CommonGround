from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional

import pytest

from core.cg_context import CGContext
from infra.l0.tool_reports import publish_tool_result_report


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


class _DummyPool:
    def connection(self) -> _DummyConnCtx:
        return _DummyConnCtx()


@dataclass
class _DummyExecutionStore:
    pool: Any = _DummyPool()


class _DummyEngine:
    def __init__(self, *, status: str, error_code: Optional[str] = None) -> None:
        self.status = status
        self.error_code = error_code
        self.report_calls = []
        self.published = []

    async def report_intent(self, **kwargs: Any) -> Any:
        self.report_calls.append(dict(kwargs))
        return SimpleNamespace(
            status=self.status,
            error_code=self.error_code,
            wakeup_signals=(),
        )

    async def publish_wakeup_signals(self, signals: Any) -> None:
        self.published.append(list(signals or ()))


def _ctx(*, agent_id: str, agent_turn_id: str, turn_epoch: int) -> CGContext:
    return CGContext(
        project_id="proj_test",
        channel_id="public",
        agent_id=agent_id,
        agent_turn_id=agent_turn_id,
        turn_epoch=int(turn_epoch),
        headers={},
    ).with_trace_transport(base_headers={}, trace_id="11111111111111111111111111111111", default_depth=0)


@pytest.mark.asyncio
async def test_publish_tool_result_report_rejected_raises() -> None:
    engine = _DummyEngine(status="rejected", error_code="protocol_violation")
    with pytest.raises(RuntimeError, match="rejected"):
        await publish_tool_result_report(
            nats=object(),
            execution_store=_DummyExecutionStore(),
            l0_engine=engine,
            source_ctx=_ctx(agent_id="sys.tool.demo", agent_turn_id="", turn_epoch=0),
            target_ctx=_ctx(agent_id="agent_parent", agent_turn_id="turn_1", turn_epoch=1),
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_1",
        )


@pytest.mark.asyncio
async def test_publish_tool_result_report_accepted_calls_report() -> None:
    engine = _DummyEngine(status="accepted")
    await publish_tool_result_report(
        nats=object(),
        execution_store=_DummyExecutionStore(),
        l0_engine=engine,
        source_ctx=_ctx(agent_id="sys.tool.demo", agent_turn_id="", turn_epoch=0),
        target_ctx=_ctx(agent_id="agent_parent", agent_turn_id="turn_1", turn_epoch=1),
        payload={"tool_result_card_id": "card_1"},
        correlation_id="call_1",
    )
    assert len(engine.report_calls) == 1
