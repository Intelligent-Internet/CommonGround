from typing import Any, Dict, List, Optional, TYPE_CHECKING

from core.utils import safe_str
from infra.event_emitter import emit_agent_step

if TYPE_CHECKING:
    from infra.cardbox_client import CardBoxClient
    from infra.nats_client import NATSClient

    from .models import AgentTurnWorkItem


async def resolve_box_id(
    *,
    cardbox: "CardBoxClient",
    box_id: Optional[str],
    project_id: str,
) -> Optional[str]:
    if box_id is None:
        return None
    box = await cardbox.get_box(box_id, project_id=project_id)
    if not box:
        return None
    return safe_str(box.box_id)


async def emit_step_event(
    *,
    nats: "NATSClient",
    item: "AgentTurnWorkItem",
    step_id: str,
    phase: str,
    timing_enabled: bool,
    new_card_ids: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    ctx = item.to_cg_context(step_id=step_id)
    await emit_agent_step(
        nats=nats,
        ctx=ctx,
        step_id=step_id,
        phase=phase,
        metadata=metadata,
        new_card_ids=new_card_ids,
        timing=item.timing if timing_enabled else None,
        include_llm_meta=timing_enabled,
    )
