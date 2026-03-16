from __future__ import annotations

from typing import Any, Optional

from core.cg_context import CGContext
from core.status import STATUS_FAILED
from core.utp_protocol import Card
from infra.tool_executor import ToolResultBuilder, ToolResultContext


async def save_failed_tool_result_card(
    *,
    cardbox: Any,
    ctx: CGContext,
    function_name: str,
    error: Any,
) -> Card:
    _ = ctx.require_agent_turn_id
    _ = ctx.require_step_id
    tool_call_id = ctx.require_tool_call_id

    result_context = ToolResultContext(
        ctx=ctx,
        cmd_data={"after_execution": "suspend", "tool_name": function_name},
    )
    _, error_card = ToolResultBuilder(
        result_context,
        author_id=ctx.agent_id,
        function_name=function_name,
        error_source="worker",
    ).build(
        status=STATUS_FAILED,
        result=None,
        error=error,
        after_execution="suspend",
    )
    await cardbox.save_card(error_card)
    return error_card
