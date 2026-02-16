from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from core.utils import safe_str
from infra.cardbox_client import CardBoxClient
from infra.tool_executor import (
    ToolResultBuilder,
    ToolResultContext,
    extract_tool_call_lineage,
    extract_tool_call_metadata,
)

logger = logging.getLogger("PMOReply")


@dataclass(frozen=True)
class BestEffortResumeContext:
    tool_call_id: str
    agent_turn_id: str
    agent_id: str
    turn_epoch: int
    after_execution: str
    tool_name: str
    tool_call_card_id: Optional[str]


def parse_best_effort_resume_context(data: Dict[str, Any]) -> Optional[BestEffortResumeContext]:
    tool_call_id = safe_str((data or {}).get("tool_call_id"))
    agent_turn_id = safe_str((data or {}).get("agent_turn_id"))
    agent_id = safe_str((data or {}).get("agent_id"))
    turn_epoch_raw = (data or {}).get("turn_epoch")
    after_execution = safe_str((data or {}).get("after_execution")) or "suspend"
    if not tool_call_id or not agent_turn_id or not agent_id:
        return None
    if after_execution not in ("suspend", "terminate"):
        return None
    try:
        turn_epoch = int(turn_epoch_raw)
    except (TypeError, ValueError):
        return None
    return BestEffortResumeContext(
        tool_call_id=tool_call_id,
        agent_turn_id=agent_turn_id,
        agent_id=agent_id,
        turn_epoch=turn_epoch,
        after_execution=after_execution,
        tool_name=safe_str((data or {}).get("tool_name")) or "unknown",
        tool_call_card_id=safe_str((data or {}).get("tool_call_card_id")),
    )


async def load_tool_call_lineage(
    *,
    cardbox: CardBoxClient,
    project_id: str,
    tool_call_card_id: Optional[str],
    warn_context: str,
) -> Dict[str, str]:
    if not tool_call_card_id:
        return {}
    try:
        cards = await cardbox.get_cards([str(tool_call_card_id)], project_id=project_id)
        tool_call = cards[0] if cards else None
        lineage = extract_tool_call_lineage(extract_tool_call_metadata(tool_call))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to load tool_call card for %s: %s", warn_context, exc)
        return {}
    return {key: value for key, value in lineage.items() if value}


async def build_and_save_tool_result_reply(
    *,
    cardbox: CardBoxClient,
    project_id: str,
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
        project_id=project_id,
        tool_call_card_id=tool_call_card_id,
        warn_context=warn_context,
    )
    result_context = ToolResultContext.from_cmd_data(
        project_id=project_id,
        cmd_data=cmd_data,
        tool_call_meta=tool_call_meta,
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
    await cardbox.save_card(result_card)
    return payload, result_card
