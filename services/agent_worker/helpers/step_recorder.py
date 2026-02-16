from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..models import AgentTurnWorkItem
from ..shared import emit_step_event as emit_step_event_helper


class StepRecorder:
    def __init__(self, step_store: Any, nats: Any, timing_step_event: bool = False) -> None:
        self.step_store = step_store
        self.nats = nats
        self.timing_step_event = bool(timing_step_event)

    async def start_step(
        self,
        item: AgentTurnWorkItem,
        step_id: str,
        status: str = "started",
        agent_turn_id: Optional[str] = None,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
    ) -> None:
        await self.step_store.insert_step(
            project_id=item.project_id,
            agent_id=item.agent_id,
            step_id=step_id,
            agent_turn_id=agent_turn_id if agent_turn_id is not None else item.agent_turn_id,
            channel_id=item.channel_id,
            parent_step_id=item.parent_step_id,
            trace_id=item.trace_id,
            status=status,
            profile_box_id=profile_box_id if profile_box_id is not None else item.profile_box_id,
            context_box_id=context_box_id if context_box_id is not None else item.context_box_id,
            output_box_id=output_box_id if output_box_id is not None else item.output_box_id,
        )
        await self._emit_step(
            item=item,
            step_id=step_id,
            phase="started",
        )

    async def complete_step(
        self,
        item: AgentTurnWorkItem,
        step_id: str,
        new_card_ids: Optional[List[str]] = None,
    ) -> None:
        await self.set_step_status(
            item=item,
            step_id=step_id,
            status="completed",
            phase="completed",
            ended=True,
            new_card_ids=new_card_ids,
        )

    async def fail_step(
        self,
        item: AgentTurnWorkItem,
        step_id: str,
        error: str,
        new_card_ids: Optional[List[str]] = None,
    ) -> None:
        await self.set_step_status(
            item=item,
            step_id=step_id,
            status="failed",
            phase="failed",
            ended=True,
            error=error,
            new_card_ids=new_card_ids,
        )

    async def set_step_status(
        self,
        *,
        item: AgentTurnWorkItem,
        step_id: str,
        status: Optional[str] = None,
        phase: Optional[str] = None,
        ended: bool = False,
        error: Optional[str] = None,
        new_card_ids: Optional[List[str]] = None,
        update_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        update_kwargs: Dict[str, Any] = {
            "project_id": item.project_id,
            "agent_id": item.agent_id,
            "step_id": step_id,
            "status": status,
            "ended": ended,
            "error": error,
        }
        if update_fields:
            update_kwargs.update(update_fields)
        await self.step_store.update_step(
            **update_kwargs,
        )

        if not phase:
            return
        await self._emit_step(
            item=item,
            step_id=step_id,
            phase=phase,
            new_card_ids=new_card_ids,
        )

    async def _emit_step(
        self,
        *,
        item: AgentTurnWorkItem,
        step_id: str,
        phase: str,
        new_card_ids: Optional[List[str]] = None,
    ) -> None:
        await emit_step_event_helper(
            nats=self.nats,
            item=item,
            step_id=step_id,
            phase=phase,
            timing_enabled=self.timing_step_event,
            new_card_ids=new_card_ids,
        )
