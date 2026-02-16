from core.status import STATUS_FAILED, STATUS_SUCCESS
from services.agent_worker.resume_context import (
    has_tool_result_card_id,
    is_valid_after_execution,
    normalize_resume_status,
    parse_resume_command,
)


def test_parse_resume_command_extracts_expected_fields() -> None:
    cmd = parse_resume_command(
        {
            "tool_call_id": "call_1",
            "status": "success",
            "after_execution": "suspend",
            "tool_result_card_id": "card_1",
            "step_id": "step_1",
            "source_agent_id": "agent_a",
        }
    )
    assert cmd.tool_call_id == "call_1"
    assert cmd.status == "success"
    assert cmd.after_execution == "suspend"
    assert cmd.tool_result_card_id == "card_1"
    assert cmd.step_id_hint == "step_1"
    assert cmd.source_agent_id == "agent_a"
    assert cmd.has_inline_result is False


def test_parse_resume_command_marks_inline_result_as_forbidden() -> None:
    cmd = parse_resume_command({"result": {"ok": True}})
    assert cmd.has_inline_result is True


def test_normalize_resume_status_falls_back_to_failed() -> None:
    assert normalize_resume_status("unknown") == STATUS_FAILED
    assert normalize_resume_status(None) == STATUS_FAILED
    assert normalize_resume_status(STATUS_SUCCESS) == STATUS_SUCCESS


def test_after_execution_and_card_id_validators() -> None:
    assert is_valid_after_execution("suspend") is True
    assert is_valid_after_execution("terminate") is True
    assert is_valid_after_execution("continue") is False

    assert has_tool_result_card_id("card_1") is True
    assert has_tool_result_card_id("   ") is False
    assert has_tool_result_card_id(None) is False
