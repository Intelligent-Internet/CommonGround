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
    turn_epoch: int,
    tool_call_id: str,
    function_name: str,
    error: Any,
) -> Card:
    project_id = str(ctx.project_id or "")
    agent_id = str(ctx.agent_id or "")
    agent_turn_id = str(ctx.agent_turn_id or "")
    step_id = str(ctx.step_id or "")
    trace_id: Optional[str] = ctx.trace_id
    parent_step_id: Optional[str] = ctx.parent_step_id

    if not project_id or not agent_id or not agent_turn_id or not step_id:
        raise ValueError("save_failed_tool_result_card requires ctx with project/agent/turn/step ids")

    cmd_data = {
        "tool_call_id": tool_call_id,
        "agent_turn_id": agent_turn_id,
        "turn_epoch": turn_epoch,
        "agent_id": agent_id,
        "after_execution": "suspend",
        "tool_name": function_name,
    }
    tool_call_meta = {
        "step_id": step_id,
        **({"trace_id": trace_id} if trace_id else {}),
        **({"parent_step_id": parent_step_id} if parent_step_id else {}),
    }
    result_context = ToolResultContext.from_cmd_data(
        project_id=project_id,
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta,
    )
    _, error_card = ToolResultBuilder(
        result_context,
        author_id=agent_id,
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
