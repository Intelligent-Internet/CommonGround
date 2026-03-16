from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.utp_protocol import ToolResultContent
from infra.tool_executor import fetch_and_parse_tool_result


class _DummyCardBox:
    def __init__(self, cards: list[object] | None = None, *, raise_error: bool = False) -> None:
        self.cards = list(cards or [])
        self.raise_error = bool(raise_error)

    async def get_cards(self, card_ids, *, project_id):  # noqa: ANN001
        _ = card_ids, project_id
        if self.raise_error:
            raise RuntimeError("boom")
        return list(self.cards)


def _tool_result_card(*, tool_call_id: str = "tc_1", result: object | None = None) -> object:
    return SimpleNamespace(
        card_id="card_1",
        type="tool.result",
        tool_call_id=tool_call_id,
        metadata={"tool_call_id": tool_call_id},
        content=ToolResultContent(
            status="success",
            after_execution="suspend",
            result={"ok": True} if result is None else result,
            error=None,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("card_id", "cards", "raise_error", "expected"),
    [
        ("", [], False, "missing_card_id"),
        ("card_1", [], False, "card_not_found"),
        ("card_1", [], True, "card_fetch_failed"),
    ],
)
async def test_fetch_and_parse_tool_result_basic_errors(
    card_id: str,
    cards: list[object],
    raise_error: bool,
    expected: str,
) -> None:
    res = await fetch_and_parse_tool_result(
        cardbox=_DummyCardBox(cards=cards, raise_error=raise_error),
        project_id="proj_1",
        card_id=card_id,
    )
    assert res.error_code == expected


@pytest.mark.asyncio
async def test_fetch_and_parse_tool_result_type_and_call_id_validation() -> None:
    bad_type = SimpleNamespace(card_id="card_1", type="agent.thought", tool_call_id="tc_1", content={})
    res_type = await fetch_and_parse_tool_result(
        cardbox=_DummyCardBox(cards=[bad_type]),
        project_id="proj_1",
        card_id="card_1",
        require_tool_result_type=True,
    )
    assert res_type.error_code == "invalid_card_type"

    ok_card = _tool_result_card(tool_call_id="tc_actual")
    res_mismatch = await fetch_and_parse_tool_result(
        cardbox=_DummyCardBox(cards=[ok_card]),
        project_id="proj_1",
        card_id="card_1",
        expected_tool_call_id="tc_expected",
        require_tool_result_type=True,
    )
    assert res_mismatch.error_code == "tool_call_id_mismatch"


@pytest.mark.asyncio
async def test_fetch_and_parse_tool_result_success() -> None:
    card = _tool_result_card(result={"value": 1})
    res = await fetch_and_parse_tool_result(
        cardbox=_DummyCardBox(cards=[card]),
        project_id="proj_1",
        card_id="card_1",
        require_tool_result_type=True,
    )
    assert res.ok
    assert res.payload == {
        "status": "success",
        "after_execution": "suspend",
        "result": {"value": 1},
        "error": None,
    }


@pytest.mark.asyncio
async def test_fetch_and_parse_tool_result_protocol_violation_keeps_card() -> None:
    card = SimpleNamespace(card_id="card_1", type="tool.result", content={"status": "success"})
    res = await fetch_and_parse_tool_result(
        cardbox=_DummyCardBox(cards=[card]),
        project_id="proj_1",
        card_id="card_1",
    )
    assert res.error_code == "protocol_violation"
    assert res.card is card
