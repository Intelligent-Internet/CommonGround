from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.errors import ProtocolViolationError
from core.utp_protocol import extract_tool_result_payload


@dataclass(frozen=True, slots=True)
class ToolResultReadResult:
    card: Optional[Any] = None
    payload: Optional[Dict[str, Any]] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error_code is None


def _extract_tool_call_id(card: Any) -> str:
    direct = getattr(card, "tool_call_id", None)
    if direct:
        return str(direct)
    meta = getattr(card, "metadata", None)
    if isinstance(meta, dict) and meta.get("tool_call_id"):
        return str(meta.get("tool_call_id"))
    return ""


async def fetch_and_parse_tool_result(
    *,
    cardbox: Any,
    project_id: str,
    card_id: Any,
    expected_tool_call_id: Optional[str] = None,
    require_tool_result_type: bool = False,
) -> ToolResultReadResult:
    card_id_text = str(card_id or "").strip()
    if not card_id_text:
        return ToolResultReadResult(
            error_code="missing_card_id",
            error_message="tool_result_card_id is required",
        )
    try:
        cards = await cardbox.get_cards([card_id_text], project_id=project_id)
    except Exception as exc:  # noqa: BLE001
        return ToolResultReadResult(
            error_code="card_fetch_failed",
            error_message=f"failed to load card: {exc}",
        )

    card = cards[0] if cards else None
    if not card:
        return ToolResultReadResult(
            error_code="card_not_found",
            error_message=f"tool.result card not found: {card_id_text}",
        )

    if require_tool_result_type and getattr(card, "type", None) != "tool.result":
        return ToolResultReadResult(
            error_code="invalid_card_type",
            error_message="card type must be tool.result",
        )

    if expected_tool_call_id:
        actual_call_id = _extract_tool_call_id(card)
        if actual_call_id != str(expected_tool_call_id):
            return ToolResultReadResult(
                error_code="tool_call_id_mismatch",
                error_message=(
                    "tool_call_id mismatch: "
                    f"expected={expected_tool_call_id} actual={actual_call_id or '<empty>'}"
                ),
            )

    try:
        payload = extract_tool_result_payload(card)
    except ProtocolViolationError as exc:
        return ToolResultReadResult(
            card=card,
            error_code="protocol_violation",
            error_message=str(exc),
        )

    return ToolResultReadResult(card=card, payload=payload)
