from __future__ import annotations

import logging
import uuid6
from datetime import datetime, UTC
from typing import Any, Optional

from core.cg_context import CGContext
from core.subject import format_subject
from core.utp_protocol import AgentTaskPayload
from core.utp_protocol import Card, JsonContent, TextContent
from core.status import STATUS_FAILED, STATUS_TIMEOUT
from core.time_utils import to_iso, utc_now
from infra.event_emitter import emit_agent_state, emit_agent_task
from core.utils import safe_str
from core.errors import ProtocolViolationError
from infra.agent_routing import resolve_agent_target
from infra.cardbox_client import CardBoxClient
from infra.l0_engine import L0Engine
from infra.nats_client import NATSClient
from infra.primitives import enqueue as enqueue_primitive
from infra.stores import ExecutionStore, ResourceStore, StateStore
from infra.worker_helpers import normalize_headers

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
        pending_wakeup_skip_seconds: float,
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
        self.pending_wakeup_skip_seconds = pending_wakeup_skip_seconds

    def _normalize_watchdog_headers(
        self,
        *,
        trace_id: Optional[str] = None,
        default_depth: int = 0,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[dict[str, str], Optional[str], int]:
        merged_headers = dict(headers or {})
        try:
            normalized, resolved_trace_id, depth = normalize_headers(
                nats=self.nats,
                headers=merged_headers,
                trace_id=trace_id,
                default_depth=default_depth,
            )
        except ProtocolViolationError as exc:
            logger.warning("Watchdog default header normalization fallback: %s", exc)
            normalized, resolved_trace_id, depth = normalize_headers(
                nats=self.nats,
                headers={},
                trace_id=trace_id,
                default_depth=default_depth,
            )
        return normalized, resolved_trace_id, int(depth or default_depth or 0)

    async def _ensure_watchdog_deliverable(
        self,
        head: Any,
        *,
        reason: str,
    ) -> Optional[str]:
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
            claimed = await self.state_store.update(
                project_id=head.project_id,
                agent_id=head.agent_id,
                expect_turn_epoch=head.turn_epoch,
                expect_agent_turn_id=head.active_agent_turn_id,
                new_status="dispatched",
            )
            if not claimed:
                continue

            payload = AgentTaskPayload(
                agent_turn_id=head.active_agent_turn_id,
                agent_id=head.agent_id,
                channel_id=head.active_channel_id,
                profile_box_id=safe_str(head.profile_box_id),
                context_box_id=safe_str(head.context_box_id),
                output_box_id=safe_str(head.output_box_id),
                turn_epoch=head.turn_epoch,
                parent_agent_turn_id=None,
                runtime_config={},
            )
            default_depth = int(getattr(head, "active_recursion_depth", 0) or 0)
            headers, trace_id, depth = self._normalize_watchdog_headers(
                trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                default_depth=default_depth,
            )
            async with self.execution_store.pool.connection() as conn:
                async with conn.transaction():
                    enqueue_result = await enqueue_primitive(
                        store=self.execution_store,
                        project_id=head.project_id,
                        channel_id=head.active_channel_id,
                        target_agent_id=head.agent_id,
                        message_type="turn",
                        payload=payload.model_dump(),
                        enqueue_mode="call",
                        correlation_id=None,
                        recursion_depth=depth,
                        headers=headers,
                        traceparent=headers.get("traceparent"),
                        tracestate=headers.get("tracestate"),
                        trace_id=trace_id,
                        parent_step_id=head.parent_step_id,
                        source_agent_id=None,
                        source_agent_turn_id=None,
                        source_step_id=head.parent_step_id,
                        target_agent_turn_id=head.active_agent_turn_id,
                        conn=conn,
                    )
            headers = dict(headers)
            headers["traceparent"] = enqueue_result.traceparent
            target = await resolve_agent_target(
                resource_store=self.resource_store,
                project_id=head.project_id,
                agent_id=head.agent_id,
            )
            if not target:
                logger.warning("Watchdog skip wake-up: missing worker_target, agent=%s", head.agent_id)
                continue
            wakeup_subject = format_subject(
                head.project_id,
                head.active_channel_id,
                "cmd",
                "agent",
                target,
                "wakeup",
            )
            await self.nats.publish_event(
                wakeup_subject,
                {"agent_id": head.agent_id, "agent_turn_id": head.active_agent_turn_id},
                headers=headers,
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
            updated = await self.state_store.finish_turn_idle(
                project_id=head.project_id,
                agent_id=head.agent_id,
                expect_turn_epoch=head.turn_epoch,
                expect_agent_turn_id=head.active_agent_turn_id,
                expect_status="dispatched",
                last_output_box_id=safe_str(head.output_box_id),
                bump_epoch=True,
            )
            if not updated:
                continue
            logger.warning(
                "Watchdog handling dispatch timeout: agent=%s turn=%s epoch=%s",
                head.agent_id,
                head.active_agent_turn_id,
                head.turn_epoch,
            )

            headers, _, _ = self._normalize_watchdog_headers(
                trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                default_depth=int(getattr(head, "active_recursion_depth", 0) or 0),
            )
            state_ctx = CGContext(
                project_id=head.project_id,
                channel_id=head.active_channel_id or "public",
                agent_id=head.agent_id,
                agent_turn_id=head.active_agent_turn_id,
                trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                headers=dict(headers or {}),
            )
            await emit_agent_state(
                nats=self.nats,
                ctx=state_ctx,
                turn_epoch=head.turn_epoch,
                status="idle",
                output_box_id=str(head.output_box_id) if head.output_box_id else None,
                metadata={"error": "dispatch_timeout"},
            )

            await self.state_store.record_force_termination(
                project_id=head.project_id,
                agent_id=head.agent_id,
                agent_turn_id=head.active_agent_turn_id,
                reason="dispatch_timeout",
                output_box_id=head.output_box_id,
            )

            deliverable_card_id = await self._ensure_watchdog_deliverable(
                head,
                reason="dispatch_timeout",
            )

            headers, _, _ = self._normalize_watchdog_headers(
                trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                default_depth=int(getattr(head, "active_recursion_depth", 0) or 0),
            )
            task_ctx = CGContext(
                project_id=head.project_id,
                channel_id=head.active_channel_id or "public",
                agent_id=head.agent_id,
                agent_turn_id=head.active_agent_turn_id,
                trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                headers=dict(headers or {}),
            )
            await emit_agent_task(
                nats=self.nats,
                ctx=task_ctx,
                status=STATUS_TIMEOUT,
                output_box_id=str(head.output_box_id) if head.output_box_id else None,
                deliverable_card_id=str(deliverable_card_id) if deliverable_card_id else None,
                error="dispatch_timeout",
                stats={},
            )

            # Try to move forward in the queue after forcing idle.
            try:
                await self.l0.wakeup(
                    project_id=head.project_id,
                    channel_id=head.active_channel_id or "public",
                    target_agent_id=head.agent_id,
                    mode="dispatch_next",
                    reason="watchdog_forced_idle",
                    headers=headers,
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
            updated = await self.state_store.finish_turn_idle(
                project_id=head.project_id,
                agent_id=head.agent_id,
                expect_turn_epoch=head.turn_epoch,
                expect_agent_turn_id=head.active_agent_turn_id,
                last_output_box_id=safe_str(head.output_box_id),
                bump_epoch=True,
            )
            if updated:
                logger.warning(
                    "Watchdog cleaning stale turn: agent=%s turn=%s epoch=%s prev_status=%s",
                    head.agent_id,
                    head.active_agent_turn_id,
                    head.turn_epoch,
                    head.status,
                )

                # Try to move forward in the queue after forcing idle.
                headers, _, _ = self._normalize_watchdog_headers(
                    trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                    default_depth=int(getattr(head, "active_recursion_depth", 0) or 0),
                )
                try:
                    await self.l0.wakeup(
                        project_id=head.project_id,
                        channel_id=head.active_channel_id or "public",
                        target_agent_id=head.agent_id,
                        mode="dispatch_next",
                        reason="watchdog_forced_idle",
                        headers=headers,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Watchdog wake-up (dispatch_next) failed agent=%s: %s", head.agent_id, exc)

                headers, _, _ = self._normalize_watchdog_headers(
                    trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                    default_depth=int(getattr(head, "active_recursion_depth", 0) or 0),
                )
                state_ctx = CGContext(
                    project_id=head.project_id,
                    channel_id=head.active_channel_id or "public",
                    agent_id=head.agent_id,
                    agent_turn_id=head.active_agent_turn_id,
                    trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                    headers=dict(headers or {}),
                )
                await emit_agent_state(
                    nats=self.nats,
                    ctx=state_ctx,
                    turn_epoch=head.turn_epoch,
                    status="idle",
                    output_box_id=str(head.output_box_id) if head.output_box_id else None,
                    metadata={"error": "timeout_reaped_by_watchdog"},
                )

                # 1. Record audit
                await self.state_store.record_force_termination(
                    project_id=head.project_id,
                    agent_id=head.agent_id,
                    agent_turn_id=head.active_agent_turn_id,
                    reason="timeout_reaped_by_watchdog",
                    output_box_id=head.output_box_id,
                )

                # 1b. Best-effort complete deliverable to avoid downstream scanning on output_box.
                deliverable_card_id = await self._ensure_watchdog_deliverable(
                    head,
                    reason="timeout_reaped_by_watchdog",
                )
                
                # 2. Publish event
                headers, _, _ = self._normalize_watchdog_headers(
                    trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                    default_depth=int(getattr(head, "active_recursion_depth", 0) or 0),
                )
                task_ctx = CGContext(
                    project_id=head.project_id,
                    channel_id=head.active_channel_id or "public",
                    agent_id=head.agent_id,
                    agent_turn_id=head.active_agent_turn_id,
                    trace_id=str(head.trace_id) if getattr(head, "trace_id", None) else None,
                    headers=dict(headers or {}),
                )
                await emit_agent_task(
                    nats=self.nats,
                    ctx=task_ctx,
                    status=STATUS_FAILED,
                    output_box_id=str(head.output_box_id) if head.output_box_id else None,
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
            message_type = row.get("message_type")
            payload = row.get("payload") or {}
            if not project_id or not agent_id:
                continue
            channel_id = None
            if message_type == "turn":
                channel_id = safe_str(payload.get("channel_id"))
            if not channel_id:
                state = await self.state_store.fetch(project_id, agent_id)
                channel_id = safe_str(state.active_channel_id) if state else None
            if not channel_id:
                created_at = row.get("created_at")
                age_seconds: Optional[float] = None
                if isinstance(created_at, datetime):
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=UTC)
                    age_seconds = (datetime.now(UTC) - created_at).total_seconds()
                if (
                    self.pending_wakeup_skip_seconds > 0
                    and age_seconds is not None
                    and age_seconds >= self.pending_wakeup_skip_seconds
                ):
                    try:
                        await self.execution_store.patch_inbox_payload(
                            inbox_id=inbox_id,
                            project_id=project_id,
                            patch={
                                "watchdog_error": "missing_channel",
                                "watchdog_at": to_iso(utc_now()),
                            },
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    await self.execution_store.update_inbox_status(
                        inbox_id=inbox_id,
                        project_id=project_id,
                        status="skipped",
                    )
                    logger.warning(
                        "Watchdog skipping pending wake-up: missing channel, inbox=%s agent=%s age=%.1fs",
                        inbox_id,
                        agent_id,
                        age_seconds,
                    )
                    continue
                logger.warning(
                    "Watchdog skipping pending wake-up: missing channel, inbox=%s agent=%s",
                    inbox_id,
                    agent_id,
                )
                continue

            target = await resolve_agent_target(
                resource_store=self.resource_store,
                project_id=project_id,
                agent_id=agent_id,
            )
            if not target:
                logger.warning("Watchdog skipping pending wake-up: missing worker_target, agent=%s", agent_id)
                continue
            subject = format_subject(
                project_id,
                channel_id,
                "cmd",
                "agent",
                target,
                "wakeup",
            )
            headers: dict[str, str] = {}
            traceparent = row.get("traceparent")
            if traceparent:
                headers["traceparent"] = str(traceparent)
            tracestate = row.get("tracestate")
            if tracestate:
                headers["tracestate"] = str(tracestate)
            headers, _, _ = self._normalize_watchdog_headers(
                headers=headers,
                trace_id=str(row.get("trace_id")) if row.get("trace_id") else None,
                default_depth=int(row.get("recursion_depth") or 0),
            )

            await self.nats.publish_event(
                subject,
                {
                    "agent_id": agent_id,
                    "agent_turn_id": safe_str(payload.get("agent_turn_id")) or None,
                },
                headers=headers,
            )
