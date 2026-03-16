from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from core.cg_context import CGContext
from core.errors import BadRequestError, ProtocolViolationError
from core.utils import safe_str

from .base import InternalHandlerDeps, InternalHandlerResult
from .delegate_async import DelegateAsyncHandler


class LaunchPrincipalHandler:
    name = "launch_principal"

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
