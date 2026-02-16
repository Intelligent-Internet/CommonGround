from services.agent_worker.react_response import (
    normalize_tool_calls,
    split_submit_result_calls,
)


def test_normalize_tool_calls_filters_non_dict_and_assigns_ids() -> None:
    tool_calls = [
        {"id": "tc_1", "function": {"name": "a"}},
        {"function": {"name": "b"}},
        "not_dict",
    ]
    normalized = normalize_tool_calls(tool_calls)
    assert len(normalized) == 2
    assert normalized[0]["id"] == "tc_1"
    assert normalized[1]["id"].startswith("tc_")


def test_split_submit_result_calls_partitions_calls() -> None:
    calls = [
        {"id": "1", "function": {"name": "submit_result"}},
        {"id": "2", "function": {"name": "search"}},
    ]
    submit_calls, non_submit_calls = split_submit_result_calls(calls)
    assert [c["id"] for c in submit_calls] == ["1"]
    assert [c["id"] for c in non_submit_calls] == ["2"]
