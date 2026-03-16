from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import ValidationError

from core.cg_context import CGContext
from core.errors import BadRequestError, ProtocolViolationError
from core.utils import safe_str
from core.utp_protocol import ToolCommandPayload
from infra.tool_executor import (
    ToolResultContext,
    extract_tool_call_metadata,
    ToolResultBuilder,
    extract_tool_call_args,
)


def _normalize_validation_exception(exc: Exception) -> Exception:
    if not isinstance(exc, ValidationError):
        return exc
    missing = ToolCommandPayload.missing_fields_from_error(exc)
    return BadRequestError(f"missing required fields {missing or ['unknown']}")


async def recover_validation_failure_context(
    *,
    cardbox: Any,
    ingress_ctx: Optional[CGContext],
    data: Dict[str, Any],
) -> Optional[CGContext]:
    """Recover the minimum ctx needed to publish a failed tool.result.

    Recovery must stay within kernel-owned facts:
    1. ingress ctx
    2. tool.call card metadata

    Payload control fields are intentionally not trusted here.
    """
    if ingress_ctx is None:
        return None

    candidate_ctx = ingress_ctx
    try:
        _ = candidate_ctx.require_tool_call_id
        _ = candidate_ctx.require_agent_turn_id
        return candidate_ctx
    except ProtocolViolationError:
        pass

    tool_call_card_id = safe_str((data or {}).get("tool_call_card_id"))
    if not tool_call_card_id:
        return None

    try:
        cards = await cardbox.get_cards([tool_call_card_id], project_id=ingress_ctx.project_id)
    except Exception:
        return None
    tool_call_card = cards[0] if cards else None
    if not tool_call_card or getattr(tool_call_card, "type", None) != "tool.call":
        return None

    tool_call_meta = extract_tool_call_metadata(tool_call_card)
    recovered_turn_id = safe_str(tool_call_meta.get("agent_turn_id")) or candidate_ctx.agent_turn_id
    recovered_tool_call_id = safe_str(getattr(tool_call_card, "tool_call_id", None)) or safe_str(
        tool_call_meta.get("tool_call_id")
    )
    recovered_ctx = candidate_ctx.with_lineage_meta(tool_call_meta)
    if recovered_turn_id and recovered_turn_id != recovered_ctx.agent_turn_id:
        recovered_ctx = recovered_ctx.with_turn(recovered_turn_id, recovered_ctx.turn_epoch)
    if recovered_tool_call_id and recovered_tool_call_id != recovered_ctx.tool_call_id:
        recovered_ctx = recovered_ctx.with_tool_call(recovered_tool_call_id)

    try:
        _ = recovered_ctx.require_tool_call_id
        _ = recovered_ctx.require_agent_turn_id
    except ProtocolViolationError:
        return None
    return recovered_ctx


async def build_validation_failure_package(
    *,
    cardbox: Any,
    ingress_ctx: Optional[CGContext],
    data: Dict[str, Any],
    exc: Exception,
    default_tool_name: str,
    author_id: str,
    default_function_name: Optional[str] = None,
    allowed_after_execution: Optional[set[str]] = None,
) -> Optional[tuple[CGContext, Dict[str, Any], Any]]:
    validation_ctx = await recover_validation_failure_context(
        cardbox=cardbox,
        ingress_ctx=ingress_ctx,
        data=dict(data or {}),
    )
    if validation_ctx is None:
        return None
    tool_name = safe_str((data or {}).get("tool_name")) or str(default_tool_name)
    after_execution = safe_str((data or {}).get("after_execution")) or "suspend"
    if allowed_after_execution is not None and after_execution not in allowed_after_execution:
        after_execution = "suspend"
    result_context = ToolResultContext(
        ctx=validation_ctx,
        cmd_data={
            "tool_name": tool_name,
            "after_execution": after_execution,
        },
    )
    payload, card = ToolResultBuilder(
        result_context,
        author_id=str(author_id),
        function_name=tool_name or default_function_name,
        error_source="tool",
    ).fail(
        _normalize_validation_exception(exc),
        function_name=tool_name or default_function_name,
        after_execution=after_execution,
    )
    return validation_ctx, payload, card


__all__ = [
    "build_validation_failure_package",
    "recover_validation_failure_context",
    "extract_tool_call_args",
    "extract_tool_call_metadata",
]
