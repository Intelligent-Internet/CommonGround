import pytest

from core.cg_context import CGContext
from core.status import STATUS_FAILED
from services.agent_worker.react_tool_cards import save_failed_tool_result_card


class _DummyCardBox:
    def __init__(self) -> None:
        self.saved_cards = []

    async def save_card(self, card, conn=None) -> None:
        _ = conn
        self.saved_cards.append(card)


def _tool_result_payload(card) -> dict:
    content = card.content
    return {
        "status": content.status,
        "after_execution": content.after_execution,
        "result": content.result,
        "error": content.error,
    }


async def _build_error_card() -> tuple[object, _DummyCardBox]:
    cardbox = _DummyCardBox()
    card = await save_failed_tool_result_card(
        cardbox=cardbox,
        ctx=CGContext(
            project_id="proj_1",
            agent_id="agent_1",
            agent_turn_id="turn_1",
            trace_id="trace_1",
            step_id="step_1",
            parent_step_id="parent_1",
        ),
        turn_epoch=3,
        tool_call_id="call_1",
        function_name="demo_tool",
        error="boom",
    )
    return card, cardbox

@pytest.mark.asyncio
async def test_save_failed_tool_result_card_persists_tool_result_with_lineage() -> None:
    card, cardbox = await _build_error_card()
    assert len(cardbox.saved_cards) == 1
    assert cardbox.saved_cards[0].card_id == card.card_id
    assert card.type == "tool.result"
    payload = _tool_result_payload(card)
    assert payload["status"] == STATUS_FAILED
    assert payload["after_execution"] == "suspend"
    assert payload["result"] is None
    assert isinstance(payload["error"], dict)
    assert card.metadata["step_id"] == "step_1"
    assert card.metadata["trace_id"] == "trace_1"
    assert card.metadata["parent_step_id"] == "parent_1"
    assert card.metadata["function_name"] == "demo_tool"
