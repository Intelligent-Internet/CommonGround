from __future__ import annotations

import uuid6
from typing import Any, Dict, Optional

from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.headers import require_recursion_depth
from core.trace import trace_id_from_headers
from core.utp_protocol import Card
from infra.tool_executor import ToolResultBuilder, ToolResultContext

from .models import ActionOutcome


async def create_error_outcome(
    *,
    cardbox: Any,
    ctx: CGContext,
    tool_call_id: str,
    fn_name: str,
    exc: Exception,
    tool_call_meta: Optional[Dict[str, Any]] = None,
    tool_call_card: Optional[Card] = None,
) -> ActionOutcome:
    project_id = str(ctx.project_id or "")
    agent_id = str(ctx.agent_id or "")
    agent_turn_id = str(ctx.agent_turn_id or "")
    if not project_id or not agent_id or not agent_turn_id:
        raise ValueError("create_error_outcome requires ctx with project/agent/turn ids")

    cmd_data = {
        "tool_call_id": tool_call_id,
        "agent_turn_id": agent_turn_id,
        "turn_epoch": 0,
        "agent_id": agent_id,
        "after_execution": "suspend",
        "tool_name": fn_name,
    }
    result_context = ToolResultContext.from_cmd_data(
        project_id=project_id,
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta or {},
    )
    _, card = ToolResultBuilder(
        result_context,
        author_id=agent_id,
        function_name=fn_name,
        error_source="worker",
    ).fail(exc, after_execution="suspend")
    await cardbox.save_card(card)
    cards = [tool_call_card, card] if tool_call_card else [card]
    return ActionOutcome(cards=cards, suspend=False, mark_complete=False)


async def record_tool_call_edge(
    *,
    execution_store: Any,
    ctx: CGContext,
    tool_call_id: str,
    tool_name: str,
    logger: Any,
) -> None:
    if not execution_store:
        return
    project_id = str(ctx.project_id or "")
    channel_id = str(ctx.channel_id or "")
    agent_id = str(ctx.agent_id or "")
    agent_turn_id = str(ctx.agent_turn_id or "")
    step_id = str(ctx.step_id or "")
    headers: Dict[str, str] = dict(ctx.headers or {})
    trace_id: Optional[str] = ctx.trace_id
    parent_step_id: Optional[str] = ctx.parent_step_id
    if not project_id or not channel_id or not agent_id or not agent_turn_id or not step_id:
        raise ValueError("record_tool_call_edge requires ctx with project/channel/agent/turn/step ids")
    try:
        depth = require_recursion_depth(headers)
    except ProtocolViolationError as exc:
        logger.warning("Tool call edge skipped (invalid recursion_depth): %s", exc)
        return
    trace_id_value = trace_id or trace_id_from_headers(headers)
    edge_id = f"edge_{uuid6.uuid7().hex}"
    try:
        await execution_store.insert_execution_edge(
            edge_id=edge_id,
            project_id=project_id,
            channel_id=channel_id,
            primitive="tool_call",
            edge_phase="request",
            source_agent_id=agent_id,
            source_agent_turn_id=agent_turn_id,
            source_step_id=step_id,
            target_agent_id=agent_id,
            target_agent_turn_id=agent_turn_id,
            correlation_id=tool_call_id,
            enqueue_mode=None,
            recursion_depth=int(depth),
            trace_id=str(trace_id_value) if trace_id_value else None,
            parent_step_id=parent_step_id,
            metadata={
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
                "step_id": step_id,
                "recursion_depth": int(depth),
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Tool call edge insert failed tool_call_id=%s: %s", tool_call_id, exc)
