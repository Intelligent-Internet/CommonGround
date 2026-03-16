from __future__ import annotations

import json
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
    RECURSION_DEPTH_HEADER,
    TRACEPARENT_HEADER,
)
from core.utils import safe_str

def _resolve_replay_agent_turn_id(
    *,
    row: Dict[str, Any],
    header_map: Dict[str, str],
    message_type: str,
) -> str:
    agent_turn_id = safe_str(row.get("agent_turn_id"))
    if not agent_turn_id and message_type == "turn":
        agent_turn_id = safe_str(row.get("correlation_id")) or safe_str(row.get("inbox_id"))
    if not agent_turn_id:
        agent_turn_id = safe_str(header_map.get(CG_AGENT_TURN_ID)) or ""
    if message_type == "turn" and not agent_turn_id:
        raise ProtocolViolationError("missing required agent_turn_id for replay turn message")
    return agent_turn_id


def parse_context_headers(raw_value: Any) -> Dict[str, str]:
    if raw_value is None:
        raise ProtocolViolationError("missing required context_headers")

    parsed = raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            raise ProtocolViolationError("context_headers cannot be empty")
        try:
            parsed = json.loads(text)
        except Exception as exc:  # noqa: BLE001
            raise ProtocolViolationError("invalid context_headers JSON") from exc

    if not isinstance(parsed, dict):
        raise ProtocolViolationError("context_headers must be a dict or JSON object string")

    headers = {str(k): str(v) for k, v in parsed.items() if k and v is not None}
    if TRACEPARENT_HEADER not in headers:
        raise ProtocolViolationError("context_headers missing required traceparent")
    if RECURSION_DEPTH_HEADER not in headers:
        raise ProtocolViolationError("context_headers missing required CG-Recursion-Depth")
    return headers


def build_replay_context(
    raw_payload: Dict[str, Any],
    db_row: Dict[str, Any] | None,
) -> Tuple[CGContext, Dict[str, Any]]:
    payload = dict(raw_payload or {}) if isinstance(raw_payload, dict) else {}
    reject_payload_control_fields(payload, boundary="replay ingress")
    row = dict(db_row or {})
    if not row:
        raise ProtocolViolationError("replay context requires db_row")

    context_headers = parse_context_headers(row.get("context_headers"))
    header_map, normalized_trace_id, normalized_depth = CGContext.normalize_transport_headers(context_headers)

    project_id = safe_str(row.get("project_id"))
    if not project_id:
        raise ProtocolViolationError("missing required project_id")
    agent_id = safe_str(row.get("agent_id"))
    if not agent_id:
        raise ProtocolViolationError("missing required agent_id")
    message_type = safe_str(row.get("message_type")) or ""
    channel_id = safe_str(row.get("channel_id")) or safe_str(header_map.get(CG_CHANNEL_ID))
    if not channel_id:
        raise ProtocolViolationError("missing required channel_id")
    agent_turn_id = _resolve_replay_agent_turn_id(
        row=row,
        header_map=header_map,
        message_type=message_type,
    )
    turn_epoch = (
        CGContext.parse_optional_int(row.get("turn_epoch"), field_name="turn_epoch")
        or CGContext.parse_optional_int(header_map.get(CG_TURN_EPOCH), field_name=CG_TURN_EPOCH)
        or 0
    )
    recursion_depth = int(normalized_depth)
    trace_id = normalized_trace_id
    step_id = safe_str(header_map.get(CG_STEP_ID))
    tool_call_id = safe_str(header_map.get(CG_TOOL_CALL_ID))
    parent_agent_id = safe_str(header_map.get(CG_PARENT_AGENT_ID))
    parent_agent_turn_id = safe_str(header_map.get(CG_PARENT_AGENT_TURN_ID))
    parent_step_id = safe_str(header_map.get(CG_PARENT_STEP_ID))

    header_map[CG_PROJECT_ID] = project_id
    header_map[CG_AGENT_ID] = agent_id
    header_map[CG_CHANNEL_ID] = channel_id
    header_map[CG_TURN_EPOCH] = str(int(turn_epoch))
    if agent_turn_id:
        header_map[CG_AGENT_TURN_ID] = agent_turn_id
    else:
        header_map.pop(CG_AGENT_TURN_ID, None)

    ctx = CGContext.from_raw_fields(
        project_id=project_id,
        channel_id=channel_id,
        agent_id=agent_id,
        agent_turn_id=agent_turn_id,
        turn_epoch=turn_epoch,
        step_id=step_id,
        tool_call_id=tool_call_id,
        trace_id=trace_id,
        recursion_depth=int(recursion_depth),
        parent_agent_id=parent_agent_id,
        parent_agent_turn_id=parent_agent_turn_id,
        parent_step_id=parent_step_id,
        headers=header_map,
    )
    return ctx, sanitize_payload_control_fields(payload)


def build_state_head_context(
    head: Any,
    *,
    headers: Optional[Dict[str, str]] = None,
) -> CGContext:
    project_id = safe_str(getattr(head, "project_id", None))
    if not project_id:
        raise ProtocolViolationError("state_head missing required project_id")
    channel_id = safe_str(getattr(head, "active_channel_id", None))
    if not channel_id:
        raise ProtocolViolationError("state_head missing required active_channel_id")
    agent_id = safe_str(getattr(head, "agent_id", None))
    if not agent_id:
        raise ProtocolViolationError("state_head missing required agent_id")
    agent_turn_id = safe_str(getattr(head, "active_agent_turn_id", None))
    if not agent_turn_id:
        raise ProtocolViolationError("state_head missing required active_agent_turn_id")
    try:
        turn_epoch = int(getattr(head, "turn_epoch", 0) or 0)
    except Exception as exc:  # noqa: BLE001
        raise ProtocolViolationError("state_head has invalid turn_epoch") from exc
    try:
        recursion_depth = int(getattr(head, "active_recursion_depth", 0) or 0)
    except Exception as exc:  # noqa: BLE001
        raise ProtocolViolationError("state_head has invalid active_recursion_depth") from exc
    return CGContext.from_raw_fields(
        project_id=project_id,
        channel_id=channel_id,
        agent_id=agent_id,
        agent_turn_id=agent_turn_id,
        turn_epoch=turn_epoch,
        trace_id=safe_str(getattr(head, "trace_id", None)),
        recursion_depth=recursion_depth,
        parent_step_id=safe_str(getattr(head, "parent_step_id", None)),
        headers=dict(headers or {}),
    )
