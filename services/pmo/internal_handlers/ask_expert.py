from __future__ import annotations

from types import SimpleNamespace

from core.errors import BadRequestError, ProtocolViolationError
from core.utils import safe_str

from .base import InternalHandlerResult
from .context import InternalHandlerContext
from .delegate_async import DelegateAsyncHandler
from .orchestration_primitives import resolve_current_output_box_id


class AskExpertHandler:
    name = "ask_expert"

    async def handle(
        self,
        *,
        ctx: InternalHandlerContext,
        parent_after_execution: str,
    ) -> InternalHandlerResult:
        cmd = ctx.cmd
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
                deps=ctx.deps,
                project_id=ctx.meta.project_id,
                agent_id=cmd.source_agent_id,
            )
            delegate_args["context_box_id"] = output_box_id

        delegate_cmd = SimpleNamespace(
            tool_name="delegate_async",
            arguments=delegate_args,
            source_agent_id=cmd.source_agent_id,
            agent_turn_id=cmd.agent_turn_id,
            tool_call_id=cmd.tool_call_id,
            step_id=cmd.step_id,
        )
        delegate_ctx = ctx.fork(cmd=delegate_cmd)
        return await DelegateAsyncHandler().handle(
            ctx=delegate_ctx,
            parent_after_execution=parent_after_execution,
        )
