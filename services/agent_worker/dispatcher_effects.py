from __future__ import annotations

from typing import Any, Dict, Optional

from core.cg_context import CGContext
from core.utp_protocol import Card
from infra.tool_executor import ToolResultBuilder, ToolResultContext

from .models import ActionOutcome

async def create_error_outcome(
    *,
    cardbox: Any,
    ctx: CGContext,
    fn_name: str,
    exc: Exception,
    tool_call_meta: Optional[Dict[str, Any]] = None,
    tool_call_card: Optional[Card] = None,
) -> ActionOutcome:
    _ = ctx.require_agent_turn_id
    tool_call_id = ctx.require_tool_call_id

    result_context = ToolResultContext(
        ctx=ctx.with_lineage_meta(tool_call_meta),
        cmd_data={"after_execution": "suspend", "tool_name": fn_name},
    )
    _, card = ToolResultBuilder(
        result_context,
        author_id=ctx.agent_id,
        function_name=fn_name,
        error_source="worker",
    ).fail(exc, after_execution="suspend")
    await cardbox.save_card(card)
    cards = [tool_call_card, card] if tool_call_card else [card]
    return ActionOutcome(cards=cards, suspend=False, mark_complete=False)
