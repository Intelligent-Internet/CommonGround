from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

import uuid6

from core.config import MAX_RECURSION_DEPTH
from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.headers import require_recursion_depth
from core.trace import ensure_trace_headers
from core.utils import safe_str
from infra.agent_routing import resolve_agent_target
from infra.cardbox_client import CardBoxClient
from infra.l0_engine import L0Engine, WakeupSignal
from infra.l1_helpers.turn_payload import ensure_output_box, prepare_turn_payload
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore
from infra.event_emitter import emit_agent_state


DispatchStatus = Literal["accepted", "pending", "busy", "rejected", "error"]


@dataclass(frozen=True, slots=True, kw_only=True)
class DispatchRequest:
    project_id: str
    channel_id: str
    agent_id: str
    profile_box_id: Optional[str]
    context_box_id: Optional[str]
    output_box_id: Optional[str] = None
    agent_turn_id: Optional[str] = None
    publish_turn: bool = True
    parent_agent_turn_id: Optional[str] = None
    parent_tool_call_id: Optional[str] = None
    parent_step_id: Optional[str] = None
    trace_id: Optional[str] = None
    display_name: Optional[str] = None
    runtime_config: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    emit_state_event: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class DispatchResult:
    status: DispatchStatus
    error_code: Optional[str] = None
    agent_id: Optional[str] = None
    agent_turn_id: Optional[str] = None
    turn_epoch: Optional[int] = None
    output_box_id: Optional[str] = None
    trace_id: Optional[str] = None


class AgentDispatcher:
    def __init__(
        self,
        *,
        resource_store: ResourceStore,
        state_store: StateStore,
        cardbox: CardBoxClient,
        nats: NATSClient,
        execution_store: Optional[ExecutionStore] = None,
        l0_engine: Optional[L0Engine] = None,
    ) -> None:
        self.resource_store = resource_store
        self.state_store = state_store
        self.cardbox = cardbox
        self.nats = nats
        self.execution_store = execution_store or ExecutionStore(pool=self.state_store.pool)
        self.l0 = l0_engine or L0Engine(
            nats=self.nats,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            state_store=self.state_store,
        )

    async def dispatch(self, req: DispatchRequest) -> DispatchResult:
        async with self.execution_store.pool.connection() as conn:
            async with conn.transaction():
                dispatch_result, wakeup_signals = await self.dispatch_transactional(req, conn=conn)

        for signal in wakeup_signals:
            await self.l0.publish_wakeup_signal(signal)

        if (
            req.emit_state_event
            and dispatch_result.status == "accepted"
            and safe_str(dispatch_result.agent_turn_id)
            and dispatch_result.turn_epoch is not None
        ):
            headers = self.nats.merge_headers(req.headers)
            try:
                headers, _, resolved_trace_id = ensure_trace_headers(
                    headers,
                    trace_id=dispatch_result.trace_id or req.trace_id,
                )
            except ProtocolViolationError:
                resolved_trace_id = dispatch_result.trace_id or req.trace_id
                headers = dict(req.headers or {})

            ctx = CGContext(
                project_id=req.project_id,
                channel_id=req.channel_id,
                agent_id=req.agent_id,
                agent_turn_id=safe_str(dispatch_result.agent_turn_id),
                trace_id=resolved_trace_id,
                headers=dict(headers or {}),
                parent_step_id=req.parent_step_id,
            )
            await emit_agent_state(
                nats=self.nats,
                ctx=ctx,
                turn_epoch=int(dispatch_result.turn_epoch),
                status="dispatched",
                output_box_id=dispatch_result.output_box_id,
            )

        return dispatch_result

    async def dispatch_transactional(
        self,
        req: DispatchRequest,
        *,
        conn: Any,
    ) -> tuple[DispatchResult, list[WakeupSignal]]:
        if not req.project_id:
            return DispatchResult(status="rejected", error_code="missing_project_id"), []
        if not req.agent_id:
            return DispatchResult(status="rejected", error_code="missing_agent_id"), []
        if not req.profile_box_id:
            return DispatchResult(status="rejected", error_code="missing_profile_box_id"), []
        if not req.context_box_id:
            return DispatchResult(status="rejected", error_code="missing_context_box_id"), []
        if not req.publish_turn:
            return DispatchResult(status="rejected", error_code="l0_required"), []

        headers = self.nats.merge_headers(req.headers)
        try:
            headers, _, trace_id = ensure_trace_headers(headers, trace_id=req.trace_id)
        except ProtocolViolationError:
            return DispatchResult(
                status="rejected",
                error_code="protocol_violation",
                trace_id=req.trace_id,
            ), []
        try:
            depth = require_recursion_depth(headers)
        except ProtocolViolationError:
            return DispatchResult(
                status="rejected",
                error_code="protocol_violation",
                trace_id=trace_id,
            ), []
        if depth >= MAX_RECURSION_DEPTH:
            return DispatchResult(
                status="rejected",
                error_code="recursion_depth_exceeded",
                trace_id=trace_id,
            ), []

        output_box_id = await ensure_output_box(
            cardbox=self.cardbox,
            project_id=req.project_id,
            output_box_id=req.output_box_id,
            conn=conn,
        )

        leased_turn_epoch: Optional[int] = None
        agent_turn_id = safe_str(req.agent_turn_id) or f"turn_{uuid6.uuid7().hex}"

        target = await resolve_agent_target(
            resource_store=self.resource_store,
            project_id=req.project_id,
            agent_id=req.agent_id,
            conn=conn,
        )
        if not target:
            return DispatchResult(
                status="error",
                error_code="worker_target_missing",
                agent_id=req.agent_id,
                output_box_id=output_box_id,
                trace_id=trace_id,
            ), []

        payload = prepare_turn_payload(
            agent_id=req.agent_id,
            channel_id=req.channel_id,
            profile_box_id=safe_str(req.profile_box_id),
            context_box_id=safe_str(req.context_box_id),
            output_box_id=safe_str(output_box_id),
            parent_agent_turn_id=req.parent_agent_turn_id,
            parent_tool_call_id=req.parent_tool_call_id,
            parent_step_id=req.parent_step_id,
            trace_id=trace_id,
            display_name=req.display_name,
            runtime_config=req.runtime_config,
            agent_turn_id=agent_turn_id,
            turn_epoch=leased_turn_epoch,
        )

        try:
            publish_result = await self.l0.enqueue(
                project_id=req.project_id,
                channel_id=req.channel_id,
                target_agent_id=req.agent_id,
                message_type="turn",
                payload=payload,
                enqueue_mode="call",
                correlation_id=agent_turn_id,
                recursion_depth=depth,
                headers=headers,
                wakeup=True,
                trace_id=trace_id,
                parent_step_id=req.parent_step_id,
                source_agent_id=None,
                source_agent_turn_id=req.parent_agent_turn_id,
                source_step_id=req.parent_step_id,
                target_agent_turn_id=agent_turn_id,
                conn=conn,
            )
            wakeup_signals = list(publish_result.wakeup_signals or ())
        except ProtocolViolationError:
            return DispatchResult(
                status="rejected",
                error_code="protocol_violation",
                trace_id=trace_id,
            ), []
        except Exception:  # noqa: BLE001
            return DispatchResult(
                status="error",
                error_code="internal_error",
                trace_id=trace_id,
            ), []

        ack_status = publish_result.ack_status
        status: DispatchStatus = "accepted"
        error_code = publish_result.ack_error_code
        if ack_status in ("busy", "rejected", "error"):
            status = ack_status  # type: ignore[assignment]

        ack_agent_turn_id = safe_str((publish_result.ack_payload or {}).get("agent_turn_id"))
        ack_turn_epoch = None
        ack_epoch_raw = (publish_result.ack_payload or {}).get("turn_epoch")
        if ack_epoch_raw is not None:
            try:
                ack_turn_epoch = int(ack_epoch_raw)
            except (TypeError, ValueError):
                ack_turn_epoch = None

        return (
            DispatchResult(
                status=status,
                error_code=error_code,
                agent_id=req.agent_id,
                agent_turn_id=ack_agent_turn_id or agent_turn_id,
                turn_epoch=ack_turn_epoch,
                output_box_id=output_box_id,
                trace_id=trace_id,
            ),
            wakeup_signals,
        )
