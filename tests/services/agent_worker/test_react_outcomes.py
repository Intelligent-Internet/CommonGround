import pytest

from core.cg_context import CGContext
from services.agent_worker.models import ActionOutcome
from services.agent_worker.react_outcomes import normalize_action_outcomes


class _DummyCardBox:
    def __init__(self) -> None:
        self.saved_cards = []

    async def save_card(self, card, conn=None) -> None:
        _ = conn
        self.saved_cards.append(card)


class _DummyLogger:
    def __init__(self) -> None:
        self.errors = []

    def error(self, msg, *args) -> None:
        self.errors.append(msg % args if args else str(msg))


async def _normalize(tool_calls, outcomes):
    cardbox = _DummyCardBox()
    logger = _DummyLogger()
    ordered, error_count = await normalize_action_outcomes(
        cardbox=cardbox,
        ctx=CGContext(
            project_id="proj_1",
            agent_id="agent_1",
            agent_turn_id="turn_1",
            trace_id="trace_1",
            step_id="step_1",
            parent_step_id="parent_1",
        ),
        turn_epoch=1,
        tool_calls=tool_calls,
        outcomes=outcomes,
        logger=logger,
    )
    return ordered, error_count, cardbox, logger

@pytest.mark.asyncio
async def test_normalize_action_outcomes_converts_exception_to_error_card() -> None:
    tool_calls = [{"id": "tc_1", "function": {"name": "search"}}]
    ordered, error_count, cardbox, logger = await _normalize(tool_calls, [RuntimeError("boom")])
    assert error_count == 1
    assert len(ordered) == 1
    assert ordered[0].suspend is False
    assert ordered[0].mark_complete is False
    assert len(ordered[0].cards) == 1
    assert ordered[0].cards[0].type == "tool.result"
    assert len(cardbox.saved_cards) == 1
    assert any("Tool execution failed" in msg for msg in logger.errors)


@pytest.mark.asyncio
async def test_normalize_action_outcomes_converts_empty_outcome_to_error_card() -> None:
    tool_calls = [{"id": "tc_1", "function": {"name": "search"}}]
    outcomes = [ActionOutcome(cards=[], suspend=False, mark_complete=False)]
    ordered, error_count, cardbox, logger = await _normalize(tool_calls, outcomes)
    assert error_count == 1
    assert len(ordered) == 1
    assert len(ordered[0].cards) == 1
    assert ordered[0].cards[0].type == "tool.result"
    assert len(cardbox.saved_cards) == 1
    assert any("Tool returned no cards without suspend/complete" in msg for msg in logger.errors)


@pytest.mark.asyncio
async def test_normalize_action_outcomes_keeps_valid_outcome() -> None:
    tool_calls = [{"id": "tc_1", "function": {"name": "search"}}]
    outcomes = [ActionOutcome(cards=["card_a"], suspend=True, mark_complete=False)]
    ordered, error_count, cardbox, logger = await _normalize(tool_calls, outcomes)
    assert error_count == 0
    assert ordered == outcomes
    assert cardbox.saved_cards == []
    assert logger.errors == []
