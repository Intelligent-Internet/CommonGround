from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from core.cg_context import CGContext
from core.errors import BadRequestError, ProtocolViolationError
from core.utils import safe_str

from .base import InternalHandlerDeps, InternalHandlerResult
from .delegate_async import DelegateAsyncHandler
from .orchestration_primitives import resolve_current_output_box_id


class AskExpertHandler:
    name = "ask_expert"

    async def handle(
        self,
        *,
        deps: InternalHandlerDeps,
        ctx: CGContext,
        cmd: Any,
        parent_after_execution: str,
    ) -> InternalHandlerResult:
        if cmd.tool_name != self.name:
            raise ProtocolViolationError(
                f"tool_name mismatch: expected {self.name}, got {safe_str(cmd.tool_name)}"
            )

        args = cmd.arguments if isinstance(cmd.arguments, dict) else {}
        expert_profile = safe_str(args.get("expert_profile"))
        question = safe_str(args.get("question"))
        include_history = bool(args.get("include_history", False))

        if not expert_profile:
            raise BadRequestError("ask_expert: args.expert_profile is required")
        if not question:
            raise BadRequestError("ask_expert: args.question is required")

        delegate_args = {
            "target_strategy": "new",
            "target_ref": expert_profile,
            "instruction": question,
        }
        if include_history:
            output_box_id = await resolve_current_output_box_id(
                deps=deps,
                ctx=ctx,
            )
            delegate_args["context_box_id"] = output_box_id

        delegate_cmd = SimpleNamespace(
            tool_name="delegate_async",
            arguments=delegate_args,
            source_agent_id=ctx.agent_id,
            agent_turn_id=ctx.agent_turn_id,
            tool_call_id=cmd.tool_call_id,
            step_id=ctx.step_id,
        )
        return await DelegateAsyncHandler().handle(
            deps=deps,
            ctx=ctx,
            cmd=delegate_cmd,
            parent_after_execution=parent_after_execution,
        )
