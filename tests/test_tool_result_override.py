import pytest

from card_box_core.structures import ToolResultContent
from core.utp_protocol import extract_tool_result_payload, resolve_effective_after_execution
from infra.tool_executor import ToolResultContext


class DummyCard:
    def __init__(self, content):
        self.content = content


def _extract_payload(result, after_execution="suspend"):
    content = ToolResultContent(
        status="success",
        after_execution=after_execution,
        result=result,
        error=None,
    )
    return extract_tool_result_payload(DummyCard(content))


@pytest.mark.parametrize(
    "result_payload, default_value, expected",
    [
        ({"__cg_control": {"after_execution": "terminate"}}, "suspend", "terminate"),
        ({"__cg_control": {"after_execution": "suspend"}}, "terminate", "suspend"),
        ({"__cg_control": {"after_execution": "pause"}}, "suspend", "suspend"),
        ({"__cg_control": {"after_execution": ""}}, "suspend", "suspend"),
        ({"__cg_control": {"after_execution": None}}, "terminate", "terminate"),
        ({"__cg_control": "terminate"}, "suspend", "suspend"),
        ({"foo": "bar"}, "terminate", "terminate"),
    ],
)
def test_resolve_effective_after_execution(result_payload, default_value, expected):
    assert resolve_effective_after_execution(result_payload, default_value) == expected


@pytest.mark.parametrize(
    "result_payload, expected",
    [
        ({"__cg_control": {"after_execution": "terminate"}}, "terminate"),
        ({"__cg_control": {"after_execution": "pause"}}, "suspend"),
        ({"__cg_control": {"after_execution": ""}}, "suspend"),
        ({"__cg_control": None}, "suspend"),
        ({"foo": "bar"}, "suspend"),
    ],
)
def test_extract_tool_result_payload_after_execution_override(result_payload, expected):
    payload = _extract_payload(result_payload, after_execution="suspend")
    assert payload["after_execution"] == expected
    assert payload["result"] == result_payload


def test_tool_result_context_result_override_has_highest_priority():
    ctx = ToolResultContext.from_cmd_data(
        project_id="proj_1",
        cmd_data={"after_execution": "suspend"},
    )
    ctx.override_after_execution("terminate")
    assert ctx.effective_after_execution({"__cg_control": {"after_execution": "suspend"}}) == "suspend"
    assert ctx.effective_after_execution({"foo": "bar"}) == "terminate"
