from __future__ import annotations

from typing import Any, Mapping

from core.errors import ProtocolViolationError
from core.utils import safe_str


L0_CONTROL_FIELDS = frozenset(
    {
        "project_id",
        "channel_id",
        "channel",
        "agent_id",
        "agent_turn_id",
        "turn_epoch",
        "step_id",
        "tool_call_id",
        "trace_id",
        "traceparent",
        "tracestate",
        "parent_agent_id",
        "parent_agent_turn_id",
        "parent_step_id",
        "parent_traceparent",
        "recursion_depth",
    }
)

RESUME_FORBIDDEN_FIELDS = frozenset(set(L0_CONTROL_FIELDS) | {"source_agent_id"})


def payload_control_keys(
    payload: Mapping[str, Any] | None,
    *,
    control_fields: frozenset[str] = L0_CONTROL_FIELDS,
) -> tuple[str, ...]:
    keys: set[str] = set()
    for key in dict(payload or {}).keys():
        key_text = safe_str(key)
        if key_text and key_text in control_fields:
            keys.add(str(key_text))
    return tuple(sorted(keys))


def reject_payload_control_fields(
    payload: Mapping[str, Any] | None,
    *,
    boundary: str,
    control_fields: frozenset[str] = L0_CONTROL_FIELDS,
) -> None:
    present = payload_control_keys(payload, control_fields=control_fields)
    if present:
        raise ProtocolViolationError(
            f"{boundary}: payload contains forbidden control fields: {', '.join(present)}"
        )


def sanitize_payload_control_fields(
    payload: Mapping[str, Any] | None,
    *,
    control_fields: frozenset[str] = L0_CONTROL_FIELDS,
) -> dict[str, Any]:
    safe_payload = dict(payload or {})
    for key in control_fields:
        safe_payload.pop(key, None)
    return safe_payload
