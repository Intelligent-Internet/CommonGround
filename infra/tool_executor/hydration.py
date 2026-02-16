from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

from core.errors import BadRequestError, NotFoundError, ProtocolViolationError
from core.trace import trace_id_from_headers
from core.utp_protocol import extract_tool_call_args

from .context import ToolCallContext


def extract_tool_call_metadata(tool_call_card: Any) -> Dict[str, Any]:
    meta = getattr(tool_call_card, "metadata", None)
    return dict(meta) if isinstance(meta, dict) else {}


def extract_tool_call_lineage(tool_call_meta: Dict[str, Any]) -> Dict[str, Optional[str]]:
    def _maybe_str(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value)
        return text if text else None

    return {
        "step_id": _maybe_str(tool_call_meta.get("step_id")),
        "trace_id": _maybe_str(tool_call_meta.get("trace_id")),
        "parent_step_id": _maybe_str(tool_call_meta.get("parent_step_id")),
    }


@dataclass
class ToolCallHydrationError(Exception):
    cause: Exception
    tool_call_meta: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return str(self.cause)


async def hydrate_tool_call_context(
    *,
    cardbox: Any,
    subject: str,
    parts: Any,
    data: Mapping[str, Any],
    headers: Mapping[str, str],
    payload: Any,
    require_tool_call_type: bool = False,
    validate_tool_call_id_match: bool = False,
) -> ToolCallContext:
    project_id = str(getattr(parts, "project_id", "") or "")
    tool_call_card_id = getattr(payload, "tool_call_card_id", None)
    if not tool_call_card_id:
        raise ToolCallHydrationError(BadRequestError("missing required field: tool_call_card_id"), {})
    cards = await cardbox.get_cards([str(tool_call_card_id)], project_id=project_id)
    tool_call_card = cards[0] if cards else None
    if not tool_call_card:
        raise ToolCallHydrationError(
            NotFoundError(f"tool_call_card_id not found: {tool_call_card_id}"),
            {},
        )

    tool_call_meta = extract_tool_call_metadata(tool_call_card)

    if require_tool_call_type and getattr(tool_call_card, "type", None) != "tool.call":
        raise ToolCallHydrationError(
            ProtocolViolationError(
                "tool_call_card_id type mismatch: expected 'tool.call',"
                f" got {getattr(tool_call_card, 'type', None)!r}"
            ),
            tool_call_meta,
        )

    if validate_tool_call_id_match:
        payload_tool_call_id = getattr(payload, "tool_call_id", None)
        card_tool_call_id = getattr(tool_call_card, "tool_call_id", None)
        if card_tool_call_id not in (None, "", payload_tool_call_id):
            raise ToolCallHydrationError(
                ProtocolViolationError("tool_call_id mismatch between payload and tool.call card"),
                tool_call_meta,
            )

    try:
        args = extract_tool_call_args(tool_call_card)
    except ProtocolViolationError as exc:
        raise ToolCallHydrationError(exc, tool_call_meta) from exc

    headers_dict = dict(headers or {})
    if "trace_id" not in tool_call_meta or not tool_call_meta.get("trace_id"):
        fallback_trace_id = trace_id_from_headers(headers_dict)
        if fallback_trace_id:
            tool_call_meta = dict(tool_call_meta)
            tool_call_meta["trace_id"] = str(fallback_trace_id)

    return ToolCallContext(
        project_id=project_id,
        cmd_data=dict(data or {}),
        tool_call_meta=tool_call_meta,
        subject=subject,
        parts=parts,
        headers=headers_dict,
        payload=payload,
        raw_data=dict(data or {}),
        tool_call_card=tool_call_card,
        args=args,
    )
