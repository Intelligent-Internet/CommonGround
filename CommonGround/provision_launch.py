from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from CommonGround.contracts import ConflictError


PROVISION_LAUNCH_STARTED_ROLE = "provision_launch_started"
PROVISION_LAUNCH_STARTED_KIND_V1 = "provision_launch_started.v1"


@dataclass(frozen=True, slots=True)
class ProvisionLaunchStarted:
    assigned_agent_id: str
    role: str


def provision_launch_started_payload(
    *,
    assigned_agent_id: str,
    role: str,
) -> dict[str, Any]:
    return {
        "kind": PROVISION_LAUNCH_STARTED_KIND_V1,
        "assigned_agent_id": assigned_agent_id,
        "role": role,
    }


def parse_provision_launch_started(payload: Any) -> ProvisionLaunchStarted:
    if not isinstance(payload, dict):
        raise ConflictError("provision launch started payload must be an object")
    kind = payload.get("kind")
    if kind != PROVISION_LAUNCH_STARTED_KIND_V1:
        raise ConflictError("provision launch started kind mismatch")
    return ProvisionLaunchStarted(
        assigned_agent_id=_required_str(payload, "assigned_agent_id"),
        role=_required_str(payload, "role"),
    )


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ConflictError(f"{key} must be a non-empty string")
    return value
