from __future__ import annotations

from typing import Any, Dict, Optional

from core.errors import ProtocolViolationError
from core.utils import safe_str
from infra.cardbox_client import CardBoxClient


def _require_value(value: Optional[str], *, field_name: str) -> str:
    if not value or not str(value).strip():
        raise ProtocolViolationError(f"missing {field_name}")
    return str(value)


async def ensure_output_box(
    *,
    cardbox: CardBoxClient,
    project_id: str,
    output_box_id: Optional[str],
    conn: Any = None,
) -> str:
    if not cardbox:
        raise ProtocolViolationError("missing cardbox client")
    return str(
        await cardbox.ensure_box_id(
            project_id=project_id,
            box_id=output_box_id,
            conn=conn,
        )
    )


def prepare_turn_payload(
    *,
    agent_id: str,
    channel_id: str,
    profile_box_id: str,
    context_box_id: str,
    output_box_id: str,
    parent_agent_turn_id: Optional[str] = None,
    parent_tool_call_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    display_name: Optional[str] = None,
    runtime_config: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    agent_turn_id: Optional[str] = None,
    turn_epoch: Optional[int] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "agent_id": _require_value(agent_id, field_name="agent_id"),
        "channel_id": _require_value(channel_id, field_name="channel_id"),
        "profile_box_id": _require_value(profile_box_id, field_name="profile_box_id"),
        "context_box_id": _require_value(context_box_id, field_name="context_box_id"),
        "output_box_id": _require_value(output_box_id, field_name="output_box_id"),
        "runtime_config": dict(runtime_config or {}),
    }
    if metadata:
        payload["metadata"] = dict(metadata)
    if parent_agent_turn_id:
        payload["parent_agent_turn_id"] = safe_str(parent_agent_turn_id)
    if parent_tool_call_id:
        payload["parent_tool_call_id"] = safe_str(parent_tool_call_id)
    if parent_step_id:
        payload["parent_step_id"] = safe_str(parent_step_id)
    if trace_id:
        payload["trace_id"] = safe_str(trace_id)
    if display_name:
        payload["display_name"] = safe_str(display_name)
    if agent_turn_id:
        payload["agent_turn_id"] = safe_str(agent_turn_id)
    if turn_epoch is not None:
        try:
            payload["turn_epoch"] = int(turn_epoch)
        except Exception as exc:  # noqa: BLE001
            raise ProtocolViolationError("invalid turn_epoch") from exc
    return payload
