from services.pmo.l1_orchestrators.batch_manager import BatchManager


def test_resolve_resume_tool_name_from_metadata() -> None:
    assert BatchManager._resolve_resume_tool_name({"tool_name": "fork_join"}) == "fork_join"
    assert BatchManager._resolve_resume_tool_name({"tool_suffix": "internal.fork_join"}) == "fork_join"
    assert BatchManager._resolve_resume_tool_name({"tool_suffix": "internal.anything"}) == "fork_join"


def test_build_fork_join_result_payload() -> None:
    tasks = [
        {
            "status": "success",
            "output_box_id": "box_xxx",
            "error_detail": None,
            "metadata": {"task_index": 2, "result_view": {"digest": "found 5 papers"}},
        },
        {
            "status": "failed",
            "output_box_id": "box_yyy",
            "error_detail": "anti-bot blocked",
            "metadata": {"task_index": 0, "result_view": {}},
        },
        {
            "status": "timeout",
            "output_box_id": None,
            "error_detail": None,
            "metadata": {"task_index": 1, "result_view": {"error_code": "missing_deliverable_card_id"}},
        },
    ]

    result = BatchManager._build_fork_join_result(tasks=tasks, final_status="partial")

    assert result == {
        "status": "partial",
        "results": [
            {
                "task_index": 0,
                "status": "failed",
                "output_box_id": "box_yyy",
                "error": "anti-bot blocked",
            },
            {
                "task_index": 1,
                "status": "timeout",
                "error": "missing_deliverable_card_id",
            },
            {
                "task_index": 2,
                "status": "success",
                "summary": "found 5 papers",
                "output_box_id": "box_xxx",
            },
        ],
    }
