import pytest

from core.errors import BadRequestError, NotFoundError, ProtocolViolationError
from core.subject import format_subject
from core.utp_protocol import ToolCallContent
from services.pmo.protocol import decode_tool_command, parse_subject_cmd


class _Card:
    def __init__(self, content):
        self.content = content


class _DummyCardBox:
    def __init__(self, cards=None):
        self.cards = list(cards or [])
        self.calls = []

    async def get_cards(self, card_ids, *, project_id, conn=None):
        _ = conn
        self.calls.append((list(card_ids), project_id))
        return list(self.cards)


def _base_payload() -> dict:
    return {
        "tool_name": "delegate_async",
        "tool_call_id": "call_1",
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "step_id": "step_1",
        "agent_id": "agent_1",
        "after_execution": "suspend",
        "tool_call_card_id": "card_1",
    }


def test_parse_subject_cmd_accepts_valid_pmo_subject() -> None:
    subject = format_subject("proj_1", "public", "cmd", "sys", "pmo", "internal.delegate_async")
    meta = parse_subject_cmd(subject)
    assert meta is not None
    assert meta.project_id == "proj_1"
    assert meta.channel_id == "public"
    assert meta.component == "sys"
    assert meta.target == "pmo"
    assert meta.tool_suffix == "internal.delegate_async"


def test_parse_subject_cmd_rejects_non_pmo_subject() -> None:
    subject = format_subject("proj_1", "public", "cmd", "sys", "ui", "action")
    assert parse_subject_cmd(subject) is None


@pytest.mark.asyncio
async def test_decode_tool_command_hydrates_arguments_from_tool_call_card() -> None:
    cardbox = _DummyCardBox(
        cards=[
            _Card(
                ToolCallContent(
                    tool_name="delegate_async",
                    arguments={
                        "target_strategy": "reuse",
                        "target_ref": "agent_b",
                        "instruction": "hello",
                    },
                    status="called",
                    target_subject="cg.v1r3.proj_1.public.cmd.sys.pmo.internal.delegate_async",
                )
            )
        ]
    )
    cmd = await decode_tool_command(
        data=_base_payload(),
        cardbox=cardbox,
        project_id="proj_1",
    )
    assert cmd.arguments == {
        "target_strategy": "reuse",
        "target_ref": "agent_b",
        "instruction": "hello",
    }
    assert cardbox.calls == [(["card_1"], "proj_1")]


@pytest.mark.asyncio
async def test_decode_tool_command_rejects_missing_required_fields() -> None:
    data = _base_payload()
    data.pop("tool_call_id")
    with pytest.raises(BadRequestError):
        await decode_tool_command(
            data=data,
            cardbox=_DummyCardBox(),
            project_id="proj_1",
        )


@pytest.mark.asyncio
async def test_decode_tool_command_rejects_forbidden_args_field() -> None:
    data = _base_payload()
    data["args"] = {}
    with pytest.raises(ProtocolViolationError):
        await decode_tool_command(
            data=data,
            cardbox=_DummyCardBox(),
            project_id="proj_1",
        )


@pytest.mark.asyncio
async def test_decode_tool_command_raises_when_tool_call_card_missing() -> None:
    with pytest.raises(NotFoundError):
        await decode_tool_command(
            data=_base_payload(),
            cardbox=_DummyCardBox(cards=[]),
            project_id="proj_1",
        )
