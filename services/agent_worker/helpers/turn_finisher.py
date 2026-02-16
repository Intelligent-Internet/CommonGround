from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional

from core.status import STATUS_FAILED, STATUS_SUCCESS
from core.utp_protocol import Card, JsonContent

from ..deliverable_utils import ensure_terminal_deliverable
from ..models import AgentTurnWorkItem
from ..tx_utils import call_with_optional_conn
from .step_recorder import StepRecorder
import uuid6


@dataclass(frozen=True, slots=True)
class TurnFinishEffects:
    item: AgentTurnWorkItem
    step_id: str
    status: str
    deliverable_card_id: Optional[str]
    new_card_ids: List[str] = field(default_factory=list)
    error: Optional[str] = None
    error_code: Optional[str] = None
    stats: Dict[str, Any] = field(default_factory=dict)


class TurnFinisher:
    def __init__(
        self,
        *,
        state_store: Any,
        resource_store: Any,
        cardbox: Any,
        nats: Any,
        step_recorder: StepRecorder,
        timing_task_event: bool,
        card_metadata_builder: Callable[..., Dict[str, Any]],
        logger: logging.Logger,
        emit_state_fn: Callable[..., Awaitable[None]],
        emit_task_fn: Callable[..., Awaitable[None]],
        idle_wakeup_fn: Callable[..., Awaitable[None]],
    ) -> None:
        self.state_store = state_store
        self.resource_store = resource_store
        self.cardbox = cardbox
        self.nats = nats
        self.step_recorder = step_recorder
        self.timing_task_event = bool(timing_task_event)
        self.logger = logger
        self.emit_state_fn = emit_state_fn
        self.emit_task_fn = emit_task_fn
        self.idle_wakeup_fn = idle_wakeup_fn
        self.card_metadata_builder = card_metadata_builder

    async def fail_turn(
        self,
        item: AgentTurnWorkItem,
        step_id: str,
        reason: str,
        expect_turn_epoch: int,
        expect_agent_turn_id: str,
        error_code: Optional[str] = None,
        conn: Any = None,
        bump_epoch: bool = False,
        append_error_card: bool = False,
        error_author_id: str = "system_pmo",
        fallback_source: str = "agent_worker.react_step.ensure_terminal_deliverable",
        deliverable_card_id: Optional[str] = None,
        new_card_ids: Optional[List[str]] = None,
        stats: Optional[Dict[str, Any]] = None,
    ) -> Optional[TurnFinishEffects]:
        collected_card_ids: List[str] = list(new_card_ids or [])
        if append_error_card and item.output_box_id:
            try:
                item.output_box_id = await call_with_optional_conn(
                    self.cardbox.ensure_box_id,
                    project_id=item.project_id,
                    box_id=item.output_box_id,
                    conn=conn,
                )
                error_card = Card(
                    card_id=uuid6.uuid7().hex,
                    project_id=item.project_id,
                    type="agent.error",
                    content=JsonContent(data={"error": reason, "status": "failed"}),
                    created_at=datetime.now(UTC),
                    author_id=error_author_id,
                    metadata=self.card_metadata_builder(item, step_id=step_id, role="system"),
                )
                await call_with_optional_conn(self.cardbox.save_card, error_card, conn=conn)
                item.output_box_id = await call_with_optional_conn(
                    self.cardbox.append_to_box,
                    str(item.output_box_id),
                    [error_card.card_id],
                    project_id=item.project_id,
                    conn=conn,
                )
                collected_card_ids.append(error_card.card_id)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("TurnFinisher: append error card failed: %s", exc)

        if deliverable_card_id is None:
            deliverable_card_id = await ensure_terminal_deliverable(
                cardbox=self.cardbox,
                item=item,
                step_id=step_id,
                terminal_status=STATUS_FAILED,
                reason=reason,
                source=fallback_source,
                card_metadata_builder=self.card_metadata_builder,
                logger=self.logger,
                conn=conn,
            )
        if deliverable_card_id and deliverable_card_id not in collected_card_ids:
            collected_card_ids.append(deliverable_card_id)

        updated = await call_with_optional_conn(
            self.state_store.finish_turn_idle,
            project_id=item.project_id,
            agent_id=item.agent_id,
            expect_turn_epoch=expect_turn_epoch,
            expect_agent_turn_id=expect_agent_turn_id,
            last_output_box_id=item.output_box_id,
            bump_epoch=bool(bump_epoch),
            conn=conn,
        )
        if not updated:
            return None
        return TurnFinishEffects(
            item=item,
            step_id=step_id,
            status=STATUS_FAILED,
            deliverable_card_id=deliverable_card_id,
            new_card_ids=list(collected_card_ids),
            error=reason,
            error_code=error_code,
            stats=dict(stats or {}),
        )

    async def complete_turn(
        self,
        item: AgentTurnWorkItem,
        step_id: str,
        expect_turn_epoch: int,
        expect_agent_turn_id: str,
        deliverable_card_id: Optional[str] = None,
        conn: Any = None,
        bump_epoch: bool = False,
        terminal_reason: str = "completed_without_explicit_deliverable",
        fallback_source: str = "agent_worker.react_step.ensure_terminal_deliverable",
        new_card_ids: Optional[List[str]] = None,
        stats: Optional[Dict[str, Any]] = None,
    ) -> Optional[TurnFinishEffects]:
        collected_card_ids: List[str] = list(new_card_ids or [])
        if deliverable_card_id is None:
            deliverable_card_id = await ensure_terminal_deliverable(
                cardbox=self.cardbox,
                item=item,
                step_id=step_id,
                terminal_status=STATUS_SUCCESS,
                reason=terminal_reason,
                source=fallback_source,
                card_metadata_builder=self.card_metadata_builder,
                logger=self.logger,
                conn=conn,
            )
        if deliverable_card_id and deliverable_card_id not in collected_card_ids:
            collected_card_ids.append(deliverable_card_id)

        updated = await call_with_optional_conn(
            self.state_store.finish_turn_idle,
            project_id=item.project_id,
            agent_id=item.agent_id,
            expect_turn_epoch=expect_turn_epoch,
            expect_agent_turn_id=expect_agent_turn_id,
            last_output_box_id=item.output_box_id,
            bump_epoch=bool(bump_epoch),
            conn=conn,
        )
        if not updated:
            return None
        return TurnFinishEffects(
            item=item,
            step_id=step_id,
            status=STATUS_SUCCESS,
            deliverable_card_id=deliverable_card_id,
            new_card_ids=list(collected_card_ids),
            stats=dict(stats or {}),
        )

    async def publish_finish_effects(self, effects: TurnFinishEffects) -> None:
        item = effects.item
        ctx = item.to_cg_context()
        state_metadata = {"error": effects.error} if effects.error else None
        await self.emit_state_fn(
            nats=self.nats,
            ctx=ctx,
            turn_epoch=item.turn_epoch,
            status="idle",
            output_box_id=item.output_box_id,
            metadata=state_metadata,
        )
        if effects.status == STATUS_FAILED:
            await self.step_recorder.fail_step(
                item=item,
                step_id=effects.step_id,
                error=str(effects.error or "unknown_error"),
                new_card_ids=effects.new_card_ids,
            )
        else:
            await self.step_recorder.complete_step(
                item=item,
                step_id=effects.step_id,
                new_card_ids=effects.new_card_ids,
            )
        await self.emit_task_fn(
            nats=self.nats,
            ctx=ctx,
            status=effects.status,
            output_box_id=item.output_box_id,
            deliverable_card_id=effects.deliverable_card_id,
            error=effects.error,
            error_code=effects.error_code,
            stats=dict(effects.stats or {}),
            timing=item.timing if self.timing_task_event else None,
            include_llm_meta=self.timing_task_event,
        )
        await self._publish_idle_wakeup(item)

    async def _publish_idle_wakeup(self, item: AgentTurnWorkItem) -> None:
        await self.idle_wakeup_fn(
            nats=self.nats,
            resource_store=self.resource_store,
            state_store=self.state_store,
            project_id=item.project_id,
            channel_id=item.channel_id,
            agent_id=item.agent_id,
            headers=item.headers,
            retry_count=3,
            retry_delay=1.0,
            logger=self.logger,
            warn_prefix="Idle wakeup",
        )
