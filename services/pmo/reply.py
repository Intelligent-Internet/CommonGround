from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from core.cg_context import CGContext
from infra.cardbox_client import CardBoxClient
from infra.tool_executor import (
    ToolResultBuilder,
    ToolResultContext,
    extract_tool_call_metadata,
)

logger = logging.getLogger("PMOReply")


async def load_tool_call_lineage(
    *,
    cardbox: CardBoxClient,
    project_id: str,
    tool_call_card_id: Optional[str],
    warn_context: str,
) -> Dict[str, Any]:
    if not tool_call_card_id:
        return {}
    try:
        cards = await cardbox.get_cards([str(tool_call_card_id)], project_id=project_id)
        tool_call = cards[0] if cards else None
        lineage = extract_tool_call_metadata(tool_call)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to load tool_call card for %s: %s", warn_context, exc)
        return {}
    return dict(lineage or {})


async def build_and_save_tool_result_reply(
    *,
    cardbox: CardBoxClient,
    ctx: CGContext,
    tool_call_card_id: Optional[str],
    cmd_data: Dict[str, Any],
    status: str,
    result: Any,
    error: Optional[Any],
    function_name: str,
    after_execution: str,
    warn_context: str,
) -> Tuple[Dict[str, Any], Any]:
    tool_call_meta = await load_tool_call_lineage(
        cardbox=cardbox,
        project_id=ctx.project_id,
        tool_call_card_id=tool_call_card_id,
        warn_context=warn_context,
    )
    result_context = ToolResultContext(
        ctx=ctx,
        cmd_data=cmd_data,
    )
    payload, result_card = ToolResultBuilder(
        result_context,
        author_id="sys.pmo",
        function_name=function_name,
        error_source="worker",
    ).build(
        status=status,
        result=result,
        error=error,
        after_execution=after_execution,
    )
    lineage_meta = ctx.with_lineage_meta(tool_call_meta).to_lineage_meta()
    if isinstance(getattr(result_card, "metadata", None), dict):
        result_card.metadata.update(lineage_meta)
    await cardbox.save_card(result_card)
    return payload, result_card
