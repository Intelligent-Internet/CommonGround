from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.payload_control import reject_payload_control_fields, sanitize_payload_control_fields
from core.headers import (
    CG_AGENT_ID,
    CG_AGENT_TURN_ID,
    CG_CHANNEL_ID,
    CG_PARENT_AGENT_ID,
    CG_PARENT_AGENT_TURN_ID,
    CG_PARENT_STEP_ID,
    CG_PROJECT_ID,
    CG_STEP_ID,
    CG_TOOL_CALL_ID,
    CG_TURN_EPOCH,
)
from core.trace import trace_id_from_headers
from core.utils import safe_str

def _subject_agent_id(subject_parts: Optional[Any]) -> Optional[str]:
    if not subject_parts:
        return None
    suffix = safe_str(getattr(subject_parts, "suffix", None)) or ""
    # cmd.agent.<worker_target>.wakeup targets worker instance, not logical agent_id.
    if suffix == "wakeup":
        return None
    component = safe_str(getattr(subject_parts, "component", None))
    if component != "agent":
        return None
    return safe_str(getattr(subject_parts, "target", None))


def _subject_category(subject_parts: Any) -> str:
    category = safe_str(getattr(subject_parts, "category", None))
    if category:
        return category
    component = safe_str(getattr(subject_parts, "component", None))
    target = safe_str(getattr(subject_parts, "target", None))
    tool_suffix = safe_str(getattr(subject_parts, "tool_suffix", None))
    if component == "sys" and target == "pmo" and tool_suffix is not None:
        return "cmd"
    return "cmd"

def build_nats_ingress_context(
    headers: Dict[str, str] | None,
    raw_payload: Dict[str, Any],
    subject_parts: Any,
) -> Tuple[CGContext, Dict[str, Any]]:
    payload = dict(raw_payload or {}) if isinstance(raw_payload, dict) else {}
    if subject_parts is None:
        raise ProtocolViolationError("missing subject parts for ingress context")

    category = _subject_category(subject_parts)
    if category == "cmd":
        reject_payload_control_fields(payload, boundary="cmd ingress")

    source_headers = dict(headers or {})
    header_map, _, recursion_depth = CGContext.normalize_transport_headers(
        source_headers,
        trace_id=trace_id_from_headers(source_headers),
    )

    subject_project_id = safe_str(getattr(subject_parts, "project_id", None))
    if not subject_project_id:
        raise ProtocolViolationError("missing subject project_id")
    subject_channel_id = safe_str(getattr(subject_parts, "channel_id", None))
    if not subject_channel_id:
        raise ProtocolViolationError("missing subject channel_id")
    header_project_id = safe_str(header_map.get(CG_PROJECT_ID))
    header_channel_id = safe_str(header_map.get(CG_CHANNEL_ID))
    if header_project_id is not None and header_project_id != subject_project_id:
        raise ProtocolViolationError(f"{CG_PROJECT_ID} mismatch")
    if header_channel_id is not None and header_channel_id != subject_channel_id:
        raise ProtocolViolationError(f"{CG_CHANNEL_ID} mismatch")

    header_agent_id = safe_str(header_map.get(CG_AGENT_ID))
    subject_agent_id = _subject_agent_id(subject_parts)
    if subject_agent_id is not None:
        if header_agent_id is not None and header_agent_id != subject_agent_id:
            raise ProtocolViolationError(f"{CG_AGENT_ID} mismatch")
        agent_id = subject_agent_id
    else:
        if not header_agent_id:
            raise ProtocolViolationError(f"missing required {CG_AGENT_ID}")
        agent_id = header_agent_id

    turn_epoch = CGContext.parse_optional_int(header_map.get(CG_TURN_EPOCH), field_name=CG_TURN_EPOCH) or 0
    trace_id = trace_id_from_headers(header_map)
    ctx = CGContext.from_raw_fields(
        project_id=subject_project_id,
        channel_id=subject_channel_id,
        agent_id=agent_id,
        agent_turn_id=safe_str(header_map.get(CG_AGENT_TURN_ID)) or "",
        turn_epoch=turn_epoch,
        step_id=safe_str(header_map.get(CG_STEP_ID)),
        tool_call_id=safe_str(header_map.get(CG_TOOL_CALL_ID)),
        trace_id=trace_id,
        recursion_depth=int(recursion_depth),
        parent_agent_id=safe_str(header_map.get(CG_PARENT_AGENT_ID)),
        parent_agent_turn_id=safe_str(header_map.get(CG_PARENT_AGENT_TURN_ID)),
        parent_step_id=safe_str(header_map.get(CG_PARENT_STEP_ID)),
        headers=header_map,
    )
    return ctx, sanitize_payload_control_fields(payload)
