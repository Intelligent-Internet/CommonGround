from __future__ import annotations

from typing import Any, Dict, Optional

from core.cg_context import CGContext
from infra.event_emitter import emit_agent_state, emit_agent_task


async def finish_turn_idle_and_emit(
    *,
    state_store: Any,
    nats: Any,
    ctx: CGContext,
    last_output_box_id: Optional[str],
    expect_status: Optional[str] = None,
    bump_epoch: bool = False,
    emit_state: bool = True,
    state_metadata: Optional[Dict[str, Any]] = None,
    task_status: Optional[str] = None,
    deliverable_card_id: Optional[str] = None,
    task_error: Optional[str] = None,
    task_stats: Optional[Dict[str, Any]] = None,
) -> Optional[CGContext]:
    transition = await state_store.finish_turn_idle_transition(
        ctx=ctx,
        expect_status=expect_status,
        last_output_box_id=last_output_box_id,
        bump_epoch=bool(bump_epoch),
    )
    if transition is None:
        return None
    committed_ctx = transition.committed_ctx
    if emit_state:
        await emit_agent_state(
            nats=nats,
            ctx=committed_ctx,
            status="idle",
            output_box_id=last_output_box_id,
            metadata=state_metadata,
        )
    if task_status:
        await emit_agent_task(
            nats=nats,
            ctx=committed_ctx,
            status=task_status,
            output_box_id=last_output_box_id,
            deliverable_card_id=deliverable_card_id,
            error=task_error,
            stats=dict(task_stats or {}),
        )
    return committed_ctx
