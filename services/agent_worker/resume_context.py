from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.status import STATUS_FAILED, TOOL_RESULT_STATUS_SET
from core.utils import safe_str


@dataclass(frozen=True)
class ResumeCommand:
    tool_call_id: Optional[str]
    status: Optional[str]
    after_execution: Optional[str]
    tool_result_card_id: Optional[str]
    step_id_hint: Optional[str]
    source_agent_id: Optional[str]
    has_inline_result: bool


def parse_resume_command(data: Dict[str, Any]) -> ResumeCommand:
    payload = data or {}
    return ResumeCommand(
        tool_call_id=safe_str(payload.get("tool_call_id")),
        status=safe_str(payload.get("status")),
        after_execution=safe_str(payload.get("after_execution")),
        tool_result_card_id=safe_str(payload.get("tool_result_card_id")),
        step_id_hint=safe_str(payload.get("step_id")),
        source_agent_id=safe_str(payload.get("source_agent_id")),
        has_inline_result="result" in payload,
    )


def normalize_resume_status(status: Optional[str]) -> str:
    return status if status in TOOL_RESULT_STATUS_SET else STATUS_FAILED


def is_valid_after_execution(after_execution: Optional[str]) -> bool:
    return after_execution in ("suspend", "terminate")


def has_tool_result_card_id(card_id: Optional[str]) -> bool:
    return bool(card_id and card_id.strip())
