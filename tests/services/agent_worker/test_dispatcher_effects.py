from types import SimpleNamespace

import pytest

from core.cg_context import CGContext
from core.errors import BadRequestError
from services.agent_worker.dispatcher_effects import create_error_outcome


class _DummyCardBox:
    def __init__(self) -> None:
        self.saved_cards = []

    async def save_card(self, card, conn=None) -> None:
        _ = conn
        self.saved_cards.append(card)


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
            tool_call_id="call_1",
        ),
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
