from __future__ import annotations

from types import SimpleNamespace

from core.errors import BadRequestError, ProtocolViolationError
from core.utils import safe_str

from .base import InternalHandlerResult
from .context import InternalHandlerContext
from .delegate_async import DelegateAsyncHandler


class LaunchPrincipalHandler:
    name = "launch_principal"

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
        profile_name = safe_str(args.get("profile_name"))
        instruction = safe_str(args.get("instruction"))
        if not profile_name or not instruction:
            raise BadRequestError("launch_principal: args.profile_name and args.instruction are required")

        delegate_cmd = SimpleNamespace(
            tool_name="delegate_async",
            arguments={
                "target_strategy": "new",
                "target_ref": profile_name,
                "instruction": instruction,
            },
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
