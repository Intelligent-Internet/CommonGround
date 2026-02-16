from __future__ import annotations

import uuid6
from typing import Any, Dict, List, Tuple


def normalize_tool_calls(tool_calls: Any) -> List[Dict[str, Any]]:
    normalized_calls: List[Dict[str, Any]] = []
    for tool_call in tool_calls or []:
        if not isinstance(tool_call, dict):
            continue
        tool_call_copy = dict(tool_call)
        if not tool_call_copy.get("id"):
            tool_call_copy["id"] = f"tc_{uuid6.uuid7().hex}"
        normalized_calls.append(tool_call_copy)
    return normalized_calls


def split_submit_result_calls(
    tool_calls: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    submit_calls: List[Dict[str, Any]] = []
    non_submit_calls: List[Dict[str, Any]] = []
    for tool_call in tool_calls:
        fn_name = (tool_call.get("function") or {}).get("name")
        if fn_name == "submit_result":
            submit_calls.append(tool_call)
        else:
            non_submit_calls.append(tool_call)
    return submit_calls, non_submit_calls
