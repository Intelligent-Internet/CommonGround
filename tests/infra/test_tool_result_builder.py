from __future__ import annotations

from infra.tool_executor import ToolResultBuilder, ToolResultContext


def _build_builder() -> ToolResultBuilder:
    ctx = ToolResultContext.from_cmd_data(
        project_id="proj_1",
        cmd_data={
            "tool_call_id": "call_1",
            "agent_turn_id": "turn_1",
            "turn_epoch": 3,
            "agent_id": "agent_1",
            "tool_name": "demo.tool",
            "after_execution": "suspend",
        },
        tool_call_meta={"step_id": "step_1", "trace_id": "trace_1"},
    )
    return ToolResultBuilder(
        ctx,
        author_id="tool.demo",
        function_name="demo.tool",
        error_source="tool",
    )


def test_tool_result_builder_invalid_status_is_coerced_to_failed() -> None:
    payload, card = _build_builder().build(status="weird_status", result={"ok": True})

    assert payload["status"] == "failed"
    assert card.content.status == "failed"
    assert card.content.error["code"] == "invalid_status"
    assert card.content.error["source"] == "tool"


def test_tool_result_builder_error_with_success_status_forces_failed() -> None:
    payload, card = _build_builder().build(
        status="success",
        result={"ok": True},
        error={"code": "boom", "message": "conflict"},
    )

    assert payload["status"] == "failed"
    assert card.content.status == "failed"
    assert card.content.error["code"] == "boom"
    assert card.content.error["message"] == "conflict"


def test_tool_result_builder_non_serializable_result_is_normalized() -> None:
    payload, card = _build_builder().success(result={"bad": object()})

    assert payload["status"] == "failed"
    assert card.content.status == "failed"
    assert card.content.result is None
    assert card.content.error["code"] == "result_not_serializable"


def test_tool_result_builder_non_serializable_error_is_normalized() -> None:
    payload, card = _build_builder().build(
        status="failed",
        result={"error_code": "x", "error_message": "y"},
        error={"code": "bad_error", "message": "bad payload", "detail": {"raw": object()}},
    )

    assert payload["status"] == "failed"
    assert card.content.status == "failed"
    assert card.content.error["code"] == "error_not_serializable"


def test_tool_result_builder_explicit_after_execution_override_wins() -> None:
    payload, card = _build_builder().build(
        status="success",
        result={"__cg_control": {"after_execution": "suspend"}},
        after_execution="terminate",
    )

    assert payload["after_execution"] == "terminate"
    assert card.content.after_execution == "terminate"
    assert payload["status"] == "success"


def test_tool_result_builder_invalid_after_execution_override_is_failed() -> None:
    payload, card = _build_builder().build(
        status="success",
        result={"ok": True},
        after_execution="pause",
    )

    assert payload["after_execution"] == "suspend"
    assert payload["status"] == "failed"
    assert card.content.status == "failed"
    assert card.content.error["code"] == "invalid_after_execution"
