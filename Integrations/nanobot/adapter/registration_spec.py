from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Mapping


REGISTRATION_SPEC_ENV = "CG_AGENT_REGISTRATION_SPEC"


@dataclass(frozen=True, slots=True)
class AgentRegistrationSpec:
    role: str
    capabilities: tuple[str, ...]
    grants: tuple[str, ...]
    description: str | None = None
    accepts_work: bool = True


def encode_registration_spec(spec: AgentRegistrationSpec) -> str:
    return json.dumps(
        {
            "role": spec.role,
            "capabilities": list(spec.capabilities),
            "grants": list(spec.grants),
            "description": spec.description,
            "accepts_work": spec.accepts_work,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def decode_registration_spec(raw: str | None) -> AgentRegistrationSpec:
    if raw is None or raw == "":
        raise ValueError(f"{REGISTRATION_SPEC_ENV} is required")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{REGISTRATION_SPEC_ENV} must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{REGISTRATION_SPEC_ENV} must decode to an object")
    return AgentRegistrationSpec(
        role=_required_str(payload, "role"),
        capabilities=_string_tuple(payload.get("capabilities"), "capabilities"),
        grants=_string_tuple(payload.get("grants"), "grants"),
        description=_optional_str(payload, "description"),
        accepts_work=_required_bool(payload, "accepts_work"),
    )


def _optional_str(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string when provided")
    return value


def _required_str(data: Mapping[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _required_bool(data: Mapping[str, Any], key: str) -> bool:
    value = data.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean")
    return value


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    if any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field_name} must contain non-empty strings")
    return tuple(value)

__all__ = [
    "AgentRegistrationSpec",
    "REGISTRATION_SPEC_ENV",
    "decode_registration_spec",
    "encode_registration_spec",
]
