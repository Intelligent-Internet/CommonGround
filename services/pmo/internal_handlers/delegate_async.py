from __future__ import annotations

from typing import Any

from core.errors import BadRequestError, ProtocolViolationError
from core.utils import safe_str
from infra.db.uow import UnitOfWork

from .base import InternalHandlerResult
from .context import InternalHandlerContext
from .orchestration_primitives import (
    dispatch_to_agent_transactional,
    materialize_target,
    pack_context_with_instruction,
    publish_dispatched_state_event,
    publish_wakeup_signals,
    preview_target,
    resolve_current_output_box_id,
    validate_box_ids_exist,
)


class DelegateAsyncHandler:
    name = "delegate_async"

    async def handle(
        self,
        *,
        ctx: InternalHandlerContext,
        parent_after_execution: str,
    ) -> InternalHandlerResult:
        _ = parent_after_execution
        cmd = ctx.cmd
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

        ctx.bump_recursion_depth()
        wakeup_signals = []
        dispatch_result = None
        resolved = None
        packed_context_box_id = ""

        class _DispatchRejectedInTx(Exception):
            def __init__(self, dispatch_payload: Any, resolved_target: Any) -> None:
                self.dispatch_payload = dispatch_payload
                self.resolved_target = resolved_target
                super().__init__(f"dispatch rejected in tx: {dispatch_payload.status}")

        try:
            async with UnitOfWork(ctx.deps.state_store.pool).transaction() as tx:
                preview = await preview_target(
                    ctx=ctx,
                    target_strategy=target_strategy,
                    target_ref=target_ref,
                    conn=tx.conn,
                )
                inherit_box_ids: list[str] = []
                if context_box_id:
                    inherit_box_ids.append(context_box_id)
                if target_strategy == "clone":
                    clone_source_agent_id = (
                        preview.clone_source_agent_id if preview.clone_source_agent_id else target_ref
                    )
                    clone_output_box_id = await resolve_current_output_box_id(
                        deps=ctx.deps,
                        project_id=ctx.meta.project_id,
                        agent_id=clone_source_agent_id,
                        conn=tx.conn,
                    )
                    inherit_box_ids.append(clone_output_box_id)
                inherit_box_ids = await validate_box_ids_exist(
                    deps=ctx.deps,
                    project_id=ctx.meta.project_id,
                    box_ids=inherit_box_ids,
                    conn=tx.conn,
                )
                packed_context_box_id = await pack_context_with_instruction(
                    deps=ctx.deps,
                    project_id=ctx.meta.project_id,
                    source_agent_id=cmd.source_agent_id,
                    tool_suffix=ctx.meta.tool_suffix,
                    profile_name=preview.profile_name,
                    instruction=instruction,
                    inherit_box_ids=inherit_box_ids,
                    conn=tx.conn,
                )
                resolved = await materialize_target(ctx=ctx, preview=preview, conn=tx.conn)
                dispatch_result, wakeup_signals = await dispatch_to_agent_transactional(
                    deps=ctx.deps,
                    project_id=ctx.meta.project_id,
                    channel_id=ctx.meta.channel_id,
                    target_agent_id=resolved.agent_id,
                    profile_box_id=resolved.profile_box_id,
                    context_box_id=packed_context_box_id,
                    parent_agent_turn_id=safe_str(cmd.agent_turn_id),
                    parent_tool_call_id=safe_str(cmd.tool_call_id),
                    parent_step_id=safe_str(cmd.step_id),
                    trace_id=ctx.trace_id,
                    display_name=resolved.display_name,
                    headers=ctx.headers,
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
        await publish_wakeup_signals(deps=ctx.deps, signals=wakeup_signals)
        await publish_dispatched_state_event(
            deps=ctx.deps,
            project_id=ctx.meta.project_id,
            channel_id=ctx.meta.channel_id,
            agent_id=resolved.agent_id,
            agent_turn_id=dispatch_result.agent_turn_id,
            turn_epoch=dispatch_result.turn_epoch,
            output_box_id=dispatch_result.output_box_id,
            trace_id=dispatch_result.trace_id or ctx.trace_id,
            parent_step_id=safe_str(cmd.step_id),
            headers=ctx.headers,
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
