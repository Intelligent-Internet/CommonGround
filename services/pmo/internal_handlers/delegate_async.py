from __future__ import annotations

from typing import Any

from core.cg_context import CGContext
from core.errors import BadRequestError, ProtocolViolationError
from core.utils import safe_str
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest
from infra.db.uow import UnitOfWork
from infra.event_emitter import emit_agent_state
from infra.l0_engine import DepthPolicy

from .base import InternalHandlerDeps, InternalHandlerResult
from .orchestration_primitives import (
    find_current_output_box_id,
    materialize_target,
    pack_context_with_instruction,
    preview_target,
    resolve_current_output_box_id,
    validate_box_ids_exist,
)


class DelegateAsyncHandler:
    name = "delegate_async"

    async def handle(
        self,
        *,
        deps: InternalHandlerDeps,
        ctx: CGContext,
        cmd: Any,
        parent_after_execution: str,
    ) -> InternalHandlerResult:
        _ = parent_after_execution
        if cmd.tool_name != self.name:
            raise ProtocolViolationError(
                f"tool_name mismatch: expected {self.name}, got {safe_str(cmd.tool_name)}"
            )

        args = cmd.arguments if isinstance(cmd.arguments, dict) else {}
        target_strategy = safe_str(args.get("target_strategy")).lower()
        target_ref = safe_str(args.get("target_ref"))
        instruction = safe_str(args.get("instruction"))
        context_box_id = safe_str(args.get("context_box_id"))

        if not target_strategy:
            raise BadRequestError("delegate_async: args.target_strategy is required")
        if not target_ref:
            raise BadRequestError("delegate_async: args.target_ref is required")
        if not instruction:
            raise BadRequestError("delegate_async: args.instruction is required")

        wakeup_signals = []
        dispatch_result = None
        resolved = None
        packed_context_box_id = ""
        reused_output_box_id = ""

        class _DispatchRejectedInTx(Exception):
            def __init__(self, dispatch_payload: Any, resolved_target: Any) -> None:
                self.dispatch_payload = dispatch_payload
                self.resolved_target = resolved_target
                super().__init__(f"dispatch rejected in tx: {dispatch_payload.status}")

        dispatcher = AgentDispatcher(
            resource_store=deps.resource_store,
            state_store=deps.state_store,
            cardbox=deps.cardbox,
            nats=deps.nats,
        )
        try:
            async with UnitOfWork(deps.state_store.pool).transaction() as tx:
                preview = await preview_target(
                    deps=deps,
                    ctx=ctx,
                    target_strategy=target_strategy,
                    target_ref=target_ref,
                    conn=tx.conn,
                )
                inherit_box_ids: list[str] = []
                extra_authorized_box_ids: list[str] = []
                if context_box_id:
                    inherit_box_ids.append(context_box_id)
                if target_strategy == "clone":
                    clone_source_agent_id = (
                        preview.clone_source_agent_id if preview.clone_source_agent_id else target_ref
                    )
                    clone_output_box_id = await resolve_current_output_box_id(
                        deps=deps,
                        ctx=ctx.evolve(agent_id=clone_source_agent_id),
                        conn=tx.conn,
                    )
                    inherit_box_ids.append(clone_output_box_id)
                    extra_authorized_box_ids.append(clone_output_box_id)
                inherit_box_ids = await validate_box_ids_exist(
                    deps=deps,
                    ctx=ctx,
                    box_ids=inherit_box_ids,
                    authorized_box_ids=extra_authorized_box_ids,
                    conn=tx.conn,
                )
                packed_context_box_id = await pack_context_with_instruction(
                    deps=deps,
                    ctx=ctx,
                    tool_suffix=safe_str(cmd.tool_name) or self.name,
                    profile_name=preview.profile_name,
                    instruction=instruction,
                    inherit_box_ids=inherit_box_ids,
                    conn=tx.conn,
                )
                resolved = await materialize_target(
                    deps=deps,
                    ctx=ctx,
                    preview=preview,
                    conn=tx.conn,
                )
                if target_strategy == "reuse":
                    # `reuse` is about reusing the target identity. If the target
                    # has no prior output yet, let the dispatcher allocate one.
                    reused_output_box_id = (
                        await find_current_output_box_id(
                            deps=deps,
                            ctx=ctx.evolve(agent_id=resolved.agent_id),
                            conn=tx.conn,
                        )
                        or ""
                    )
                dispatch_result, wakeup_signals = await dispatcher.dispatch_transactional(
                    DispatchRequest(
                        source_ctx=ctx.evolve(agent_id="sys.pmo", agent_turn_id="", step_id=None),
                        target_agent_id=resolved.agent_id,
                        profile_box_id=resolved.profile_box_id,
                        context_box_id=packed_context_box_id,
                        target_channel_id=ctx.channel_id,
                        output_box_id=reused_output_box_id or None,
                        display_name=resolved.display_name,
                        lineage_ctx=ctx,
                        depth_policy=DepthPolicy.DESCEND,
                        runtime_config={},
                        emit_state_event=False,
                    ),
                    conn=tx.conn,
                )
                if dispatch_result.status in ("busy", "rejected", "error"):
                    raise _DispatchRejectedInTx(dispatch_result, resolved)
        except _DispatchRejectedInTx as exc:
            return (
                False,
                {
                    "status": exc.dispatch_payload.status,
                    "error_code": exc.dispatch_payload.error_code,
                    "agent_id": exc.resolved_target.agent_id,
                },
            )

        if dispatch_result is None or resolved is None:
            raise RuntimeError("delegate_async: transactional dispatch produced empty result")
        if wakeup_signals:
            await dispatcher.l0.publish_wakeup_signals(list(wakeup_signals))
        dispatch_recursion_depth = getattr(dispatch_result, "recursion_depth", None)
        dispatched_ctx = ctx.evolve(
            agent_id=resolved.agent_id,
            agent_turn_id="",
            turn_epoch=0,
            step_id=None,
            tool_call_id=None,
            parent_agent_id=None,
            parent_agent_turn_id=None,
            parent_step_id=None,
            recursion_depth=(
                int(dispatch_recursion_depth)
                if dispatch_recursion_depth is not None
                else int(ctx.recursion_depth)
            ),
        )
        resolved_trace_id = safe_str(dispatch_result.trace_id) or dispatched_ctx.trace_id
        if resolved_trace_id and resolved_trace_id != dispatched_ctx.trace_id:
            dispatched_ctx = dispatched_ctx.with_transport(
                headers=dict(dispatched_ctx.headers or {}),
                trace_id=resolved_trace_id,
            )
        resolved_turn_id = safe_str(dispatch_result.agent_turn_id)
        if not resolved_turn_id:
            raise RuntimeError("delegate_async: dispatch accepted without agent_turn_id")
        resolved_turn_epoch = int(
            dispatch_result.turn_epoch
            if dispatch_result.turn_epoch is not None
            else dispatched_ctx.turn_epoch
        )
        if (
            resolved_turn_id != dispatched_ctx.agent_turn_id
            or resolved_turn_epoch != int(dispatched_ctx.turn_epoch)
        ):
            dispatched_ctx = dispatched_ctx.with_turn(resolved_turn_id, resolved_turn_epoch)
        await emit_agent_state(
            nats=deps.nats,
            ctx=dispatched_ctx,
            status="dispatched",
            output_box_id=dispatch_result.output_box_id,
        )
        return (
            False,
            {
                "status": "accepted",
                "agent_id": resolved.agent_id,
                "agent_turn_id": dispatch_result.agent_turn_id,
                "turn_epoch": dispatch_result.turn_epoch,
                "context_box_id": packed_context_box_id,
                "output_box_id": dispatch_result.output_box_id,
            },
        )
