from types import SimpleNamespace

import pytest

from core.cg_context import CGContext
from core.errors import BadRequestError
from services.agent_worker.dispatcher_effects import create_error_outcome, record_tool_call_edge


class _DummyCardBox:
    def __init__(self) -> None:
        self.saved_cards = []

    async def save_card(self, card, conn=None) -> None:
        _ = conn
        self.saved_cards.append(card)


class _DummyExecutionStore:
    def __init__(self) -> None:
        self.calls = []

    async def insert_execution_edge(self, **kwargs) -> None:
        self.calls.append(kwargs)


class _DummyLogger:
    def __init__(self) -> None:
        self.warning_calls: list[str] = []

    def warning(self, msg, *args) -> None:
        self.warning_calls.append(msg % args if args else str(msg))


@pytest.mark.asyncio
async def test_create_error_outcome_saves_tool_result_card() -> None:
    cardbox = _DummyCardBox()
    tool_call_card = SimpleNamespace(card_id="tc_1")
    outcome = await create_error_outcome(
        cardbox=cardbox,
        ctx=CGContext(
            project_id="proj_1",
            agent_id="agent_1",
            agent_turn_id="turn_1",
        ),
        tool_call_id="call_1",
        fn_name="demo_tool",
        exc=BadRequestError("bad args"),
        tool_call_meta={"step_id": "step_1"},
        tool_call_card=tool_call_card,
    )
    assert len(cardbox.saved_cards) == 1
    result_card = cardbox.saved_cards[0]
    assert result_card.type == "tool.result"
    assert outcome.suspend is False
    assert outcome.mark_complete is False
    assert outcome.cards == [tool_call_card, result_card]


@pytest.mark.asyncio
async def test_record_tool_call_edge_skips_when_depth_header_missing() -> None:
    execution_store = _DummyExecutionStore()
    logger = _DummyLogger()
    await record_tool_call_edge(
        execution_store=execution_store,
        ctx=CGContext(
            project_id="proj_1",
            channel_id="public",
            agent_id="agent_1",
            agent_turn_id="turn_1",
            headers={},
            step_id="step_1",
        ),
        tool_call_id="call_1",
        tool_name="demo_tool",
        logger=logger,
    )
    assert execution_store.calls == []
    assert any("invalid recursion_depth" in msg for msg in logger.warning_calls)


@pytest.mark.asyncio
async def test_record_tool_call_edge_inserts_when_header_valid() -> None:
    execution_store = _DummyExecutionStore()
    logger = _DummyLogger()
    await record_tool_call_edge(
        execution_store=execution_store,
        ctx=CGContext(
            project_id="proj_1",
            channel_id="public",
            agent_id="agent_1",
            agent_turn_id="turn_1",
            headers={"CG-Recursion-Depth": "2"},
            trace_id="trace_1",
            step_id="step_1",
            parent_step_id="parent_1",
        ),
        tool_call_id="call_1",
        tool_name="demo_tool",
        logger=logger,
    )
    assert len(execution_store.calls) == 1
    call = execution_store.calls[0]
    assert call["project_id"] == "proj_1"
    assert call["channel_id"] == "public"
    assert call["correlation_id"] == "call_1"
    assert call["recursion_depth"] == 2
    assert call["trace_id"] == "trace_1"
    assert call["parent_step_id"] == "parent_1"
    assert call["metadata"]["tool_name"] == "demo_tool"
    assert call["edge_id"].startswith("edge_")
