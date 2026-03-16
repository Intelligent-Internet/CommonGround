from __future__ import annotations

import logging
import uuid6
from datetime import datetime, UTC
from typing import Any, Optional

from core.cg_context import CGContext
from core.utp_protocol import AgentTaskPayload
from core.utp_protocol import Card, JsonContent, TextContent
from core.status import STATUS_FAILED, STATUS_TIMEOUT
from core.time_utils import to_iso, utc_now
from infra.event_emitter import emit_agent_state, emit_agent_task
from core.utils import safe_str
from core.errors import ProtocolViolationError
from infra.cardbox_client import CardBoxClient
from infra.stores.context_hydration import build_replay_context, build_state_head_context
from infra.l0_engine import AddressIntent, L0Engine, TurnRef
from infra.l1_helpers import finish_turn_idle_and_emit
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore

logger = logging.getLogger("PMO.L0Guard")


class L0Guard:
    """L0 control plane responsible for head retransmission and recovery (no L1 orchestration semantics)."""

    def __init__(
        self,
        *,
        state_store: StateStore,
        resource_store: ResourceStore,
        cardbox: CardBoxClient,
        nats: NATSClient,
        execution_store: ExecutionStore,
        dispatched_retry_seconds: float,
        dispatched_timeout_seconds: float,
        active_reap_seconds: float,
        pending_wakeup_seconds: float,
    ) -> None:
        self.state_store = state_store
        self.resource_store = resource_store
        self.cardbox = cardbox
        self.nats = nats
        self.execution_store = execution_store
        self.l0 = L0Engine(
            nats=self.nats,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            state_store=self.state_store,
        )
        self.dispatched_retry_seconds = dispatched_retry_seconds
        self.dispatched_timeout_seconds = dispatched_timeout_seconds
        self.active_reap_seconds = active_reap_seconds
        self.pending_wakeup_seconds = pending_wakeup_seconds

    def _normalize_watchdog_ctx(
        self,
        *,
        ctx: CGContext,
    ) -> Optional[CGContext]:
        try:
            return ctx.with_trace_transport(
                base_headers=ctx.headers,
                trace_id=ctx.trace_id,
                default_depth=int(ctx.recursion_depth),
            )
        except ProtocolViolationError as exc:
            logger.warning("Watchdog context normalization failed: %s", exc)
            return None

    def _head_turn_ctx(self, head: Any) -> Optional[CGContext]:
        try:
            return build_state_head_context(head)
        except ProtocolViolationError as exc:
            logger.warning(
                "Watchdog invalid state_head context agent=%s turn=%s err=%s",
                getattr(head, "agent_id", None),
                getattr(head, "active_agent_turn_id", None),
                exc,
            )
            return None

    def _normalize_head_ctx(self, head: Any) -> Optional[CGContext]:
        head_ctx = self._head_turn_ctx(head)
        if head_ctx is None:
            return None
        return self._normalize_watchdog_ctx(ctx=head_ctx)

    @staticmethod
    def _force_termination_ctx_from_head(head: Any) -> CGContext:
        return CGContext(
            project_id=head.project_id,
            agent_id=head.agent_id,
            agent_turn_id=head.active_agent_turn_id or "",
            channel_id=head.active_channel_id or "public",
        )

    def _normalize_head_ctx_for_action(self, *, head: Any, action: str) -> Optional[CGContext]:
        normalized_head_ctx = self._normalize_head_ctx(head)
        if normalized_head_ctx is None:
            logger.warning(
                "Watchdog skip %s: invalid head context, agent=%s turn=%s",
                action,
                head.agent_id,
                head.active_agent_turn_id,
            )
            return None
        return normalized_head_ctx

    @staticmethod
    def _build_watchdog_retransmit_correlation_id(head: Any) -> str:
        turn_id = safe_str(getattr(head, "active_agent_turn_id", None))
        nonce = uuid6.uuid7().hex
        if turn_id:
            return f"{turn_id}:watchdog_retransmit:{nonce}"
        return f"watchdog_retransmit:{nonce}"

    async def _mark_pending_inbox_error(
        self,
        *,
        inbox_id: Any,
        project_id: Any,
        agent_id: Any,
        error_code: str,
        detail: Optional[str] = None,
    ) -> None:
        patch: dict[str, Any] = {
            "watchdog_error": error_code,
            "watchdog_at": to_iso(utc_now()),
        }
        if detail:
            patch["watchdog_detail"] = detail
        try:
            await self.execution_store.patch_inbox_payload(
                inbox_id=inbox_id,
                project_id=project_id,
                patch=patch,
            )
        except Exception:  # noqa: BLE001
            pass
        await self.execution_store.update_inbox_status(
            inbox_id=inbox_id,
            project_id=project_id,
            status="error",
        )
        if detail:
            logger.warning(
                "Watchdog mark invalid pending inbox as error inbox=%s agent=%s code=%s detail=%s",
                inbox_id,
                agent_id,
                error_code,
                detail,
            )
            return
        logger.warning(
            "Watchdog mark invalid pending inbox as error inbox=%s agent=%s code=%s",
            inbox_id,
            agent_id,
            error_code,
        )

    async def _ensure_watchdog_deliverable(
        self,
        head: Any,
        *,
        reason: str,
    ) -> Optional[str]:
        # Business-level safety net (must keep): when a turn times out/reaped and no
        # explicit task.deliverable exists, PMO watchdog must synthesize one so
        # downstream consumers can always rely on output_box terminal artifact.
        # This is intentionally NOT part of protocol fallback cleanup.
        deliverable_card_id: Optional[str] = None
        if not head.output_box_id:
            return None
        try:
            box = await self.cardbox.get_box(str(head.output_box_id), project_id=head.project_id)
            if box:
                cards = []
                if box.card_ids:
                    cards = await self.cardbox.get_cards(list(box.card_ids), project_id=head.project_id)
                for c in reversed(cards):
                    if getattr(c, "type", "") == "task.deliverable":
                        deliverable_card_id = getattr(c, "card_id", None)
                        break
                if deliverable_card_id is None:
                    last_thought = ""
                    tool_results = 0
                    for c in reversed(cards):
                        if getattr(c, "type", "") == "tool.result":
                            tool_results += 1
                        if not last_thought and getattr(c, "type", "") == "agent.thought":
                            content = getattr(c, "content", "")
                            if isinstance(content, TextContent) and content.text.strip():
                                last_thought = content.text.strip()
                            elif isinstance(content, str) and content.strip():
                                last_thought = content.strip()

                    from core.deliverable_fallback import build_fallback_deliverable_data

                    fallback_data = build_fallback_deliverable_data(
                        reason=reason,
                        source="pmo.l0_guard.watchdog",
                        last_thought=last_thought,
                        tool_result_count=tool_results,
                        terminal_status=STATUS_FAILED,
                    )

                    deliverable = Card(
                        card_id=uuid6.uuid7().hex,
                        project_id=head.project_id,
                        type="task.deliverable",
                        content=JsonContent(data=fallback_data),
                        created_at=datetime.now(UTC),
                        author_id=head.agent_id,
                        metadata={
                            "agent_turn_id": head.active_agent_turn_id,
                            "role": "assistant",
                            "partial": True,
                            "stop_reason": reason,
                        },
                    )
                    await self.cardbox.save_card(deliverable)
                    await self.cardbox.append_to_box(
                        str(head.output_box_id),
                        [deliverable.card_id],
                        project_id=head.project_id,
                    )
                    deliverable_card_id = deliverable.card_id
        except Exception as exc:  # noqa: BLE001
            logger.warning("Watchdog failed to generate fallback deliverable, agent=%s: %s", head.agent_id, exc)
        return deliverable_card_id

    async def retry_stuck_dispatched(self) -> None:
        heads = await self.state_store.list_stuck_dispatched(
            older_than_seconds=self.dispatched_retry_seconds, limit=200
        )
        if not heads:
            return
        for head in heads:
            if not head.active_agent_turn_id or not head.active_channel_id:
                logger.warning(
                    "Watchdog skip resend: head missing turn or channel, agent=%s",
                    head.agent_id,
                )
                continue
            head_ctx = self._head_turn_ctx(head)
            if head_ctx is None:
                continue
            claimed = await self.state_store.update(
                ctx=head_ctx,
                new_status="dispatched",
            )
            if not claimed:
                continue

            payload = AgentTaskPayload(
                profile_box_id=safe_str(head.profile_box_id),
                context_box_id=safe_str(head.context_box_id),
                output_box_id=safe_str(head.output_box_id),
                runtime_config={},
            )
            enqueue_ctx = self._normalize_head_ctx_for_action(head=head, action="resend")
            if enqueue_ctx is None:
                continue
            async with self.execution_store.pool.connection() as conn:
                async with conn.transaction():
                    enqueue_result = await self.l0.enqueue_intent(
                        source_ctx=enqueue_ctx.evolve(
                            agent_id="sys.pmo.l0_guard",
                            agent_turn_id="",
                            step_id=None,
                            tool_call_id=None,
                        ),
                        intent=AddressIntent(
                            target=TurnRef(
                                project_id=head.project_id,
                                agent_id=head.agent_id,
                                agent_turn_id=str(head.active_agent_turn_id),
                                expected_turn_epoch=int(head.turn_epoch) if int(head.turn_epoch or 0) > 0 else None,
                            ),
                            message_type="turn",
                            payload=payload.model_dump(),
                            correlation_id=self._build_watchdog_retransmit_correlation_id(head),
                        ),
                        conn=conn,
                        wakeup=True,
                    )
            if enqueue_result.status != "accepted":
                logger.warning(
                    "Watchdog retransmit enqueue rejected: status=%s code=%s agent=%s turn=%s",
                    enqueue_result.status,
                    enqueue_result.error_code,
                    head.agent_id,
                    head.active_agent_turn_id,
                )
                continue
            wakeup_signals = list(enqueue_result.wakeup_signals or ())
            if wakeup_signals:
                await self.l0.publish_wakeup_signals(wakeup_signals)
            else:
                wakeup_ctx = enqueue_ctx.with_trace_transport(
                    trace_id=safe_str(enqueue_result.payload.get("trace_id")) or enqueue_ctx.trace_id,
                    default_depth=int(enqueue_ctx.recursion_depth),
                ).evolve(
                    channel_id=head.active_channel_id,
                )
                await self.l0.wakeup(
                    target_ctx=wakeup_ctx,
                    reason="watchdog_retransmit",
                    metadata={"agent_turn_id": safe_str(head.active_agent_turn_id)},
                )
            logger.warning(
                "Watchdog retransmitting wake-up: agent=%s turn=%s epoch=%s",
                head.agent_id,
                head.active_agent_turn_id,
                head.turn_epoch,
            )

    async def reap_stuck_dispatched_timeout(self) -> None:
        if self.dispatched_timeout_seconds <= 0:
            return
        heads = await self.state_store.list_stuck_dispatched(
            older_than_seconds=self.dispatched_timeout_seconds,
            limit=200,
        )
        if not heads:
            return
        for head in heads:
            if not head.active_agent_turn_id:
                continue
            head_ctx = self._head_turn_ctx(head)
            if head_ctx is None:
                continue
            output_box_id = str(head.output_box_id) if head.output_box_id else None
            committed_ctx = await finish_turn_idle_and_emit(
                state_store=self.state_store,
                nats=self.nats,
                ctx=head_ctx,
                last_output_box_id=output_box_id,
                expect_status="dispatched",
                bump_epoch=True,
                state_metadata={"error": "dispatch_timeout"},
            )
            if committed_ctx is None:
                continue
            logger.warning(
                "Watchdog handling dispatch timeout: agent=%s turn=%s epoch=%s",
                head.agent_id,
                head.active_agent_turn_id,
                head.turn_epoch,
            )

            await self.state_store.record_force_termination(
                ctx=self._force_termination_ctx_from_head(head),
                reason="dispatch_timeout",
                output_box_id=head.output_box_id,
            )

            deliverable_card_id = await self._ensure_watchdog_deliverable(
                head,
                reason="dispatch_timeout",
            )

            normalized_committed_ctx = self._normalize_watchdog_ctx(ctx=committed_ctx)
            if normalized_committed_ctx is not None:
                await emit_agent_task(
                    nats=self.nats,
                    ctx=normalized_committed_ctx,
                    status=STATUS_TIMEOUT,
                    output_box_id=output_box_id,
                    deliverable_card_id=str(deliverable_card_id) if deliverable_card_id else None,
                    error="dispatch_timeout",
                    stats={},
                )

            # Try to move forward in the queue after forcing idle.
            try:
                wakeup_ctx = self._normalize_head_ctx_for_action(head=head, action="wakeup")
                if wakeup_ctx is not None:
                    await self.l0.wakeup(
                        target_ctx=wakeup_ctx,
                        mode="dispatch_next",
                        reason="watchdog_forced_idle",
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Watchdog wake-up (dispatch_next) failed agent=%s: %s", head.agent_id, exc)

    async def reap_stuck_active(self) -> None:
        heads = await self.state_store.list_stuck_active(
            statuses=["running"],
            older_than_seconds=self.active_reap_seconds,
            limit=200,
        )
        if not heads:
            return
        for head in heads:
            if not head.active_agent_turn_id:
                continue
            head_ctx = self._head_turn_ctx(head)
            if head_ctx is None:
                continue
            output_box_id = str(head.output_box_id) if head.output_box_id else None
            committed_ctx = await finish_turn_idle_and_emit(
                state_store=self.state_store,
                nats=self.nats,
                ctx=head_ctx,
                last_output_box_id=output_box_id,
                bump_epoch=True,
                emit_state=False,
            )
            if committed_ctx is not None:
                logger.warning(
                    "Watchdog cleaning stale turn: agent=%s turn=%s epoch=%s prev_status=%s",
                    head.agent_id,
                    head.active_agent_turn_id,
                    head.turn_epoch,
                    head.status,
                )

                # Try to move forward in the queue after forcing idle.
                normalized_committed_ctx = self._normalize_watchdog_ctx(ctx=committed_ctx)
                if normalized_committed_ctx is not None:
                    try:
                        await self.l0.wakeup(
                            target_ctx=normalized_committed_ctx,
                            mode="dispatch_next",
                            reason="watchdog_forced_idle",
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Watchdog wake-up (dispatch_next) failed agent=%s: %s", head.agent_id, exc)

                if normalized_committed_ctx is not None:
                    await emit_agent_state(
                        nats=self.nats,
                        ctx=normalized_committed_ctx,
                        status="idle",
                        output_box_id=output_box_id,
                        metadata={"error": "timeout_reaped_by_watchdog"},
                    )

                # 1. Record audit
                await self.state_store.record_force_termination(
                    ctx=self._force_termination_ctx_from_head(head),
                    reason="timeout_reaped_by_watchdog",
                    output_box_id=head.output_box_id,
                )

                # 1b. Best-effort complete deliverable to avoid downstream scanning on output_box.
                deliverable_card_id = await self._ensure_watchdog_deliverable(
                    head,
                    reason="timeout_reaped_by_watchdog",
                )
                
                # 2. Publish event
                if normalized_committed_ctx is not None:
                    await emit_agent_task(
                        nats=self.nats,
                        ctx=normalized_committed_ctx,
                        status=STATUS_FAILED,
                        output_box_id=output_box_id,
                        deliverable_card_id=str(deliverable_card_id) if deliverable_card_id else None,
                        error="timeout_reaped_by_watchdog",
                        stats={},
                    )

    async def poke_stale_inbox(self) -> None:
        if self.pending_wakeup_seconds <= 0:
            return
        rows = await self.execution_store.list_stale_pending_inbox(
            older_than_seconds=self.pending_wakeup_seconds,
            limit=200,
        )
        if not rows:
            return
        for row in rows:
            project_id = row.get("project_id")
            agent_id = row.get("agent_id")
            inbox_id = row.get("inbox_id")
            if not project_id or not agent_id:
                continue
            channel_id = safe_str(row.get("channel_id"))
            if not channel_id:
                await self._mark_pending_inbox_error(
                    inbox_id=inbox_id,
                    project_id=project_id,
                    agent_id=agent_id,
                    error_code="missing_channel",
                )
                continue

            replay_row = dict(row)
            replay_row["project_id"] = project_id
            replay_row["agent_id"] = agent_id
            replay_row["channel_id"] = channel_id
            try:
                wakeup_ctx, _ = build_replay_context(
                    raw_payload=row.get("payload") if isinstance(row.get("payload"), dict) else {},
                    db_row=replay_row,
                )
            except ProtocolViolationError as exc:
                await self._mark_pending_inbox_error(
                    inbox_id=inbox_id,
                    project_id=project_id,
                    agent_id=agent_id,
                    error_code="invalid_replay_context",
                    detail=str(exc),
                )
                continue
            await self.l0.wakeup(
                target_ctx=wakeup_ctx,
                reason="watchdog_pending_inbox",
            )
