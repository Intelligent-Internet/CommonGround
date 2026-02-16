from __future__ import annotations

from typing import Any, Dict, Optional

from core.status import STATUS_SUCCESS
from core.utils import safe_str


def build_fallback_deliverable_data(
    *,
    reason: str,
    source: str,
    last_thought: str = "",
    tool_result_count: int = 0,
    terminal_status: Optional[str] = None,
    resume_status: Optional[str] = None,
    task_status: Optional[str] = None,
    tool_call_id: Optional[str] = None,
    headline: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_reason = safe_str(reason) or "unknown"
    normalized_source = safe_str(source) or "unknown"
    normalized_terminal_status = safe_str(terminal_status)
    normalized_resume_status = safe_str(resume_status)
    normalized_task_status = safe_str(task_status)
    normalized_tool_call_id = safe_str(tool_call_id)
    thought_text = safe_str(last_thought)
    partial = normalized_terminal_status != STATUS_SUCCESS
    if normalized_task_status:
        partial = normalized_task_status != STATUS_SUCCESS

    if headline:
        title = str(headline)
    elif partial:
        title = "Partial result (turn did not finish cleanly)."
    else:
        title = "Final result (fallback deliverable; no explicit task.deliverable was produced)."

    message_lines = [
        title,
        f"Reason: {normalized_reason}",
        f"Collected tool results: {max(0, int(tool_result_count))}",
        "",
    ]
    if thought_text:
        message_lines.append("Last agent thought (may be incomplete):")
        message_lines.append(thought_text)
    else:
        message_lines.append("No agent thought content available.")

    summary = "Partial result available." if partial else "Final fallback result available."
    output_text = thought_text or f"reason={normalized_reason}"

    fields = [
        {"name": "summary", "value": summary},
        {"name": "output", "value": output_text},
        {"name": "reason", "value": normalized_reason},
    ]

    diagnostics: Dict[str, Any] = {
        "source": normalized_source,
        "tool_result_count": max(0, int(tool_result_count)),
        "last_agent_thought": thought_text or "",
        "partial": partial,
    }
    if normalized_terminal_status:
        diagnostics["terminal_status"] = normalized_terminal_status
    if normalized_resume_status:
        diagnostics["resume_status"] = normalized_resume_status
    if normalized_task_status:
        diagnostics["task_status"] = normalized_task_status
    if normalized_tool_call_id:
        diagnostics["tool_call_id"] = normalized_tool_call_id

    return {
        "schema_version": 1,
        "kind": "fallback_deliverable",
        "status": "partial" if partial else "success",
        "partial": partial,
        "message": "\n".join(message_lines),
        "fields": fields,
        "diagnostics": diagnostics,
    }
