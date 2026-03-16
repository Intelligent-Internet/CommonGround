from __future__ import annotations

from typing import Any, Dict, Optional

from core.cg_context import CGContext
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
    ctx: CGContext,
    output_box_id: Optional[str],
    conn: Any = None,
) -> str:
    if not cardbox:
        raise ProtocolViolationError("missing cardbox client")
    return str(
        await cardbox.ensure_box_id(
            project_id=ctx.project_id,
            box_id=output_box_id,
            conn=conn,
        )
    )


def prepare_turn_payload(
    *,
    profile_box_id: str,
    context_box_id: str,
    output_box_id: str,
    display_name: Optional[str] = None,
    runtime_config: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "profile_box_id": _require_value(profile_box_id, field_name="profile_box_id"),
        "context_box_id": _require_value(context_box_id, field_name="context_box_id"),
        "output_box_id": _require_value(output_box_id, field_name="output_box_id"),
        "runtime_config": dict(runtime_config or {}),
    }
    if metadata:
        payload["metadata"] = dict(metadata)
    if display_name:
        payload["display_name"] = safe_str(display_name)
    return payload
