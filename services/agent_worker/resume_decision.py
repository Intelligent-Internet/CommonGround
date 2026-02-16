from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence

from core.status import STATUS_FAILED, STATUS_SUCCESS


@dataclass(frozen=True)
class ResumeResultInfo:
    card: Any
    status: str
    after_execution: Optional[str]
    payload: Any


@dataclass(frozen=True)
class ResumeOutcome:
    aggregate_after_execution: str
    task_status: str
    primary_payload: Any


def resolve_resume_outcome(result_infos: Sequence[ResumeResultInfo]) -> ResumeOutcome:
    terminate_infos = [info for info in result_infos if info.after_execution == "terminate"]
    aggregate_after_execution = "terminate" if terminate_infos else "suspend"
    if aggregate_after_execution == "suspend":
        return ResumeOutcome(
            aggregate_after_execution="suspend",
            task_status=STATUS_SUCCESS,
            primary_payload=None,
        )
    failed_info = next((info for info in terminate_infos if info.status != STATUS_SUCCESS), None)
    primary_info = failed_info or (terminate_infos[0] if terminate_infos else None)
    task_status = primary_info.status if primary_info else STATUS_FAILED
    primary_payload = primary_info.payload if primary_info else None
    return ResumeOutcome(
        aggregate_after_execution="terminate",
        task_status=task_status,
        primary_payload=primary_payload,
    )
