from __future__ import annotations

import uuid6
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.cg_context import CGContext
from .models import ActionOutcome
from .react_tool_cards import save_failed_tool_result_card


async def normalize_action_outcomes(
    *,
    cardbox: Any,
    ctx: CGContext,
    turn_epoch: int,
    tool_calls: Sequence[Dict[str, Any]],
    outcomes: Sequence[Any],
    logger: Any,
) -> Tuple[List[ActionOutcome], int]:
    ordered_outcomes: List[ActionOutcome] = []
    error_count = 0
    for tool_call, outcome in zip(tool_calls, outcomes):
        if isinstance(outcome, Exception):
            logger.error("Tool execution failed: %s", outcome)
            error_count += 1
            tool_call_id = tool_call.get("id") or f"tc_{uuid6.uuid7().hex}"
            function_name = (tool_call.get("function") or {}).get("name") or "unknown"
            error_card = await save_failed_tool_result_card(
                cardbox=cardbox,
                ctx=ctx,
                turn_epoch=turn_epoch,
                tool_call_id=str(tool_call_id),
                function_name=function_name,
                error=outcome,
            )
            ordered_outcomes.append(
                ActionOutcome(cards=[error_card], suspend=False, mark_complete=False)
            )
            continue

        if not outcome.cards and not outcome.suspend and not outcome.mark_complete:
            error_count += 1
            tool_call_id = tool_call.get("id") or f"tc_{uuid6.uuid7().hex}"
            logger.error(
                "Tool returned no cards without suspend/complete; tool=%s",
                (tool_call.get("function") or {}).get("name"),
            )
            function_name = (tool_call.get("function") or {}).get("name") or "unknown"
            error_card = await save_failed_tool_result_card(
                cardbox=cardbox,
                ctx=ctx,
                turn_epoch=turn_epoch,
                tool_call_id=str(tool_call_id),
                function_name=function_name,
                error="tool returned no result cards and did not suspend or complete",
            )
            outcome = ActionOutcome(cards=[error_card], suspend=False, mark_complete=False)
        ordered_outcomes.append(outcome)
    return ordered_outcomes, error_count
