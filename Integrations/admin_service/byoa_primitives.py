from __future__ import annotations

from dataclasses import dataclass


BYOA_INVITE_CODE_APPROVAL_MODE = "invite_code.auto_approval.v1"
BYOA_PROFILE_WORK_MEMORY_REPORTER_V1 = "byoa.work_memory_reporter.v1"
BYOA_PROFILE_CONVERSATION_WORKER_V1 = "byoa.conversation_worker.v1"
BYOA_PROFILE_KINDS = frozenset((BYOA_PROFILE_WORK_MEMORY_REPORTER_V1, BYOA_PROFILE_CONVERSATION_WORKER_V1))


@dataclass(frozen=True, slots=True)
class ByoaInviteApproval:
    invite_id: str
    issued_by_user_id: str
    approval_mode: str = BYOA_INVITE_CODE_APPROVAL_MODE

    def to_raw_request(self) -> dict[str, str]:
        return {
            "invite_id": self.invite_id,
            "issued_by_user_id": self.issued_by_user_id,
            "approval_mode": self.approval_mode,
        }


__all__ = [
    "BYOA_INVITE_CODE_APPROVAL_MODE",
    "BYOA_PROFILE_CONVERSATION_WORKER_V1",
    "BYOA_PROFILE_KINDS",
    "BYOA_PROFILE_WORK_MEMORY_REPORTER_V1",
    "ByoaInviteApproval",
]
