import pytest

from core.cg_context import CGContext
from core.status import STATUS_FAILED, STATUS_SUCCESS
from services.pmo.reply import (
    build_and_save_tool_result_reply,
    load_tool_call_lineage,
)


class _ToolCallCard:
    def __init__(self, metadata):
        self.metadata = metadata


class _DummyCardBox:
    def __init__(self, cards=None, *, fail_get=False):
        self.cards = list(cards or [])
        self.fail_get = fail_get
        self.get_calls = []
        self.saved_cards = []

    async def get_cards(self, card_ids, *, project_id, conn=None):
        _ = conn
        self.get_calls.append((list(card_ids), project_id))
        if self.fail_get:
            raise RuntimeError("cardbox down")
        return list(self.cards)

    async def save_card(self, card, conn=None):
        _ = conn
        self.saved_cards.append(card)


def _base_cmd_data() -> dict:
    return {
        "after_execution": "suspend",
        "tool_name": "delegate_async",
    }


@pytest.mark.asyncio
async def test_load_tool_call_lineage_returns_empty_when_missing_card_id() -> None:
    lineage = await load_tool_call_lineage(
        cardbox=_DummyCardBox(),
        project_id="proj_1",
        tool_call_card_id=None,
        warn_context="resume",
    )
    assert lineage == {}


@pytest.mark.asyncio
async def test_build_and_save_tool_result_reply_uses_tool_call_lineage() -> None:
    cardbox = _DummyCardBox(
        cards=[_ToolCallCard({"step_id": "step_1", "trace_id": "trace_1", "parent_step_id": "parent_1"})]
    )
    payload, result_card = await build_and_save_tool_result_reply(
        cardbox=cardbox,
        ctx=CGContext(project_id="proj_1", agent_id="agent_1", agent_turn_id="turn_1"),
        tool_call_card_id="tc_card_1",
        cmd_data=_base_cmd_data(),
        status=STATUS_SUCCESS,
        result={"ok": True},
        error=None,
        function_name="delegate_async",
        after_execution="suspend",
        warn_context="resume",
    )
    # SSOT: identity fields come from ctx; lineage lookup is best-effort metadata source only.
    assert payload["status"] == STATUS_SUCCESS
    assert payload["tool_result_card_id"]
    assert "step_id" not in payload
    assert cardbox.get_calls == [(["tc_card_1"], "proj_1")]
    assert len(cardbox.saved_cards) == 1
    assert cardbox.saved_cards[0].card_id == result_card.card_id
    assert cardbox.saved_cards[0].metadata["step_id"] == "step_1"


@pytest.mark.asyncio
async def test_build_and_save_tool_result_reply_continues_when_lineage_lookup_fails() -> None:
    cardbox = _DummyCardBox(fail_get=True)
    payload, result_card = await build_and_save_tool_result_reply(
        cardbox=cardbox,
        ctx=CGContext(project_id="proj_1", agent_id="agent_1", agent_turn_id="turn_1"),
        tool_call_card_id="tc_card_1",
        cmd_data=_base_cmd_data(),
        status=STATUS_FAILED,
        result={"error_code": "bad_request", "error_message": "invalid payload"},
        error=None,
        function_name="delegate_async",
        after_execution="suspend",
        warn_context="bad_request",
    )
    assert payload["status"] == STATUS_FAILED
    assert payload["tool_result_card_id"]
    assert "step_id" not in payload
    assert cardbox.get_calls == [(["tc_card_1"], "proj_1")]
    assert len(cardbox.saved_cards) == 1
    assert cardbox.saved_cards[0].card_id == result_card.card_id
    assert "step_id" not in cardbox.saved_cards[0].metadata
