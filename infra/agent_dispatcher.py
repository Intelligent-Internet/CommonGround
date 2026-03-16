from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

from core.config import MAX_RECURSION_DEPTH
from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.utils import safe_str
from infra.agent_routing import resolve_agent_target
from infra.cardbox_client import CardBoxClient
from infra.l0_engine import DepthPolicy, L0Engine, SpawnIntent, WakeupSignal
from infra.l1_helpers.turn_payload import ensure_output_box, prepare_turn_payload
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore
from infra.event_emitter import emit_agent_state


DispatchStatus = Literal["accepted", "pending", "busy", "rejected", "error"]


@dataclass(frozen=True, slots=True, kw_only=True)
class DispatchRequest:
    source_ctx: CGContext
    target_agent_id: str
    profile_box_id: Optional[str]
    context_box_id: Optional[str]
    target_channel_id: Optional[str] = None
    correlation_id: Optional[str] = None
    lineage_ctx: Optional[CGContext] = None
    depth_policy: Optional[DepthPolicy] = None
    output_box_id: Optional[str] = None
    publish_turn: bool = True
    display_name: Optional[str] = None
    runtime_config: Dict[str, Any] = field(default_factory=dict)
    emit_state_event: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class DispatchResult:
    status: DispatchStatus
    error_code: Optional[str] = None
    agent_id: Optional[str] = None
    agent_turn_id: Optional[str] = None
    turn_epoch: Optional[int] = None
    recursion_depth: Optional[int] = None
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
            cardbox=self.cardbox,
        )

    @staticmethod
    def _resolve_lineage_ctx(req: DispatchRequest) -> CGContext:
        return req.lineage_ctx or req.source_ctx

    def _build_dispatch_base_ctx(self, req: DispatchRequest) -> CGContext:
        lineage_ctx = self._resolve_lineage_ctx(req)
        target_agent_id = safe_str(req.target_agent_id)
        target_channel_id = safe_str(req.target_channel_id) or lineage_ctx.channel_id
        base_ctx = lineage_ctx.evolve(
            channel_id=target_channel_id,
            agent_id=target_agent_id,
            agent_turn_id="",
            turn_epoch=0,
            step_id=None,
            tool_call_id=None,
            parent_agent_id=None,
            parent_agent_turn_id=None,
            parent_step_id=None,
            recursion_depth=int(lineage_ctx.recursion_depth),
        )
        return base_ctx.with_trace_transport(
            base_headers=lineage_ctx.headers,
            trace_id=lineage_ctx.trace_id,
            default_depth=int(lineage_ctx.recursion_depth),
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
            base_ctx = self._build_dispatch_base_ctx(req).evolve(
                recursion_depth=(
                    int(dispatch_result.recursion_depth)
                    if dispatch_result.recursion_depth is not None
                    else self._resolve_lineage_ctx(req).recursion_depth
                ),
            )
            ctx = base_ctx.with_turn(
                safe_str(dispatch_result.agent_turn_id),
                int(dispatch_result.turn_epoch or 0),
            ).with_transport(
                trace_id=dispatch_result.trace_id or base_ctx.trace_id,
            )
            await emit_agent_state(
                nats=self.nats,
                ctx=ctx,
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
        lineage_ctx = self._resolve_lineage_ctx(req)
        target_agent_id = safe_str(req.target_agent_id)
        if not target_agent_id:
            return DispatchResult(status="rejected", error_code="missing_agent_id"), []
        if not req.profile_box_id:
            return DispatchResult(status="rejected", error_code="missing_profile_box_id"), []
        if not req.context_box_id:
            return DispatchResult(status="rejected", error_code="missing_context_box_id"), []
        if not req.publish_turn:
            return DispatchResult(status="rejected", error_code="l0_required"), []
        target_ctx = self._build_dispatch_base_ctx(req)

        depth_policy = req.depth_policy or DepthPolicy.DESCEND
        if depth_policy == DepthPolicy.DESCEND:
            depth = int(lineage_ctx.recursion_depth) + 1
        elif depth_policy == DepthPolicy.PEER:
            depth = int(lineage_ctx.recursion_depth)
        else:
            depth = 0
        if depth >= MAX_RECURSION_DEPTH:
            return DispatchResult(
                status="rejected",
                error_code="recursion_depth_exceeded",
                trace_id=target_ctx.trace_id,
            ), []

        output_box_id = await ensure_output_box(
            cardbox=self.cardbox,
            ctx=target_ctx,
            output_box_id=req.output_box_id,
            conn=conn,
        )

        target = await resolve_agent_target(
            resource_store=self.resource_store,
            ctx=target_ctx,
            conn=conn,
        )
        if not target:
            return DispatchResult(
                status="error",
                error_code="worker_target_missing",
                agent_id=target_agent_id,
                output_box_id=output_box_id,
                trace_id=target_ctx.trace_id,
            ), []

        payload = prepare_turn_payload(
            profile_box_id=safe_str(req.profile_box_id),
            context_box_id=safe_str(req.context_box_id),
            output_box_id=safe_str(output_box_id),
            display_name=req.display_name,
            runtime_config=req.runtime_config,
        )

        try:
            publish_result = await self.l0.enqueue_intent(
                source_ctx=req.source_ctx,
                intent=SpawnIntent(
                    target_agent_id=target_agent_id,
                    message_type="turn",
                    payload=payload,
                    correlation_id=safe_str(req.correlation_id) or None,
                    depth_policy=depth_policy,
                    lineage_ctx=lineage_ctx,
                ),
                wakeup=True,
                conn=conn,
            )
            wakeup_signals = list(publish_result.wakeup_signals or ())
        except ProtocolViolationError:
            return DispatchResult(
                status="rejected",
                error_code="protocol_violation",
                trace_id=target_ctx.trace_id,
            ), []
        except Exception:  # noqa: BLE001
            return DispatchResult(
                status="error",
                error_code="internal_error",
                trace_id=target_ctx.trace_id,
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
        ack_depth = None
        ack_depth_raw = (publish_result.ack_payload or {}).get("recursion_depth")
        if ack_depth_raw is not None:
            try:
                ack_depth = int(ack_depth_raw)
            except (TypeError, ValueError):
                ack_depth = None
        resolved_trace_id = safe_str((publish_result.ack_payload or {}).get("trace_id")) or target_ctx.trace_id

        return (
            DispatchResult(
                status=status,
                error_code=error_code,
                agent_id=target_agent_id,
                agent_turn_id=ack_agent_turn_id or None,
                turn_epoch=ack_turn_epoch,
                recursion_depth=ack_depth,
                output_box_id=output_box_id,
                trace_id=resolved_trace_id,
            ),
            wakeup_signals,
        )
