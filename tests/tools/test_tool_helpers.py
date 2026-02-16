from __future__ import annotations

from services.tools.tool_helpers import build_tool_result_package


def test_build_tool_result_package_l2_facade() -> None:
    payload, card = build_tool_result_package(
        project_id="proj_1",
        cmd_data={
            "tool_call_id": "call_1",
            "agent_turn_id": "turn_1",
            "turn_epoch": 2,
            "agent_id": "agent_1",
            "tool_name": "demo.tool",
            "after_execution": "suspend",
        },
        tool_call_meta={"step_id": "step_1", "trace_id": "trace_1"},
        status="success",
        result={"ok": True},
        author_id="tool.demo",
        function_name="demo.tool",
    )

    assert payload["status"] == "success"
    assert payload["tool_call_id"] == "call_1"
    assert payload["after_execution"] == "suspend"
    assert card.project_id == "proj_1"
    assert card.content.status == "success"
    assert card.metadata["function_name"] == "demo.tool"
