import asyncio
import logging
import time
import uuid6
from datetime import datetime, UTC
from typing import Any, Dict, Optional

from core.cg_context import CGContext
from infra.cardbox_client import CardBoxClient
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore
from core.utp_protocol import Card, ToolCallContent
from core.status import STATUS_FAILED
from core.errors import (
    BadRequestError,
    InternalError,
    NotFoundError,
    ToolTimeoutError,
)
from infra.tool_executor import ToolResultBuilder, ToolResultContext
from infra.observability.otel import get_tracer, mark_span_error, traced
from .models import ActionOutcome
from .builtin_tools import BUILTIN_AFTER_EXEC, try_execute_builtin_tool
from .dispatcher_context import apply_tool_arg_options, parse_tool_args
from .dispatcher_effects import create_error_outcome, record_tool_call_edge
from .timing import monotonic_ms, to_iso, utc_now

logger = logging.getLogger("UTPDispatcher")
_TRACER = get_tracer("services.agent_worker.utp_dispatcher")


class UTPDispatcher:
    # Global timeout for tool execution
    DEFAULT_TIMEOUT = 30.0

    def __init__(
        self,
        cardbox: CardBoxClient,
        nats: NATSClient,
        resource_store: ResourceStore,
        *,
        execution_store: ExecutionStore | None = None,
        timing_tool_dispatch: bool = False,
    ):
        self.cardbox = cardbox
        self.nats = nats
        self.resource_store = resource_store
        self.execution_store = execution_store
        self.timing_tool_dispatch = timing_tool_dispatch

    async def _check_inbox_guard(
        self,
        *,
        project_id: str,
        agent_id: str,
        inbox_id: Optional[str],
        fn_name: str,
        tool_call_meta: Dict[str, Any],
        cmd_data: Dict[str, Any],
        tool_call_card: Optional[Card] = None,
    ) -> Optional[ActionOutcome]:
        if not self.execution_store:
            return None
        try:
            has_open_inbox = await self.execution_store.has_open_inbox(
                project_id=project_id,
                agent_id=agent_id,
                exclude_inbox_id=inbox_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Terminal tool inbox check failed: %s", exc)
            result_context = ToolResultContext.from_cmd_data(
                project_id=project_id,
                cmd_data=cmd_data,
                tool_call_meta=tool_call_meta,
            )
            _, error_card = ToolResultBuilder(
                result_context,
                author_id=agent_id,
                function_name=fn_name,
                error_source="worker",
            ).build(
                status=STATUS_FAILED,
                result=None,
                error={
                    "code": "inbox_check_failed",
                    "message": "Termination tool blocked: failed to verify inbox state. Retry after inbox is healthy.",
                },
                after_execution="suspend",
            )
            await self.cardbox.save_card(error_card)
            cards = [error_card] if tool_call_card is None else [tool_call_card, error_card]
            return ActionOutcome(cards=cards, suspend=False, mark_complete=False)

        if has_open_inbox:
            result_context = ToolResultContext.from_cmd_data(
                project_id=project_id,
                cmd_data=cmd_data,
                tool_call_meta=tool_call_meta,
            )
            _, error_card = ToolResultBuilder(
                result_context,
                author_id=agent_id,
                function_name=fn_name,
                error_source="worker",
            ).build(
                status=STATUS_FAILED,
                result=None,
                error={
                    "code": "inbox_not_empty",
                    "message": "Termination tool blocked: inbox has pending messages. Wait for inbox to drain, then retry.",
                },
                after_execution="suspend",
            )
            await self.cardbox.save_card(error_card)
            cards = [error_card] if tool_call_card is None else [tool_call_card, error_card]
            return ActionOutcome(cards=cards, suspend=False, mark_complete=False)
        return None

    @traced(_TRACER, "utp_dispatcher.execute", span_arg="_span")
    async def execute(
        self,
        *,
        tool_call: Dict[str, Any],
        ctx: CGContext,
        turn_epoch: int,
        context_box_id: Optional[str] = None,
        inbox_id: Optional[str] = None,
        guard_inbox_id: Optional[str] = None,
        _span: Any = None,
    ) -> ActionOutcome:
        """
        Safe wrapper for tool execution.
        Handles parsing, timeouts, and exceptions by returning error cards instead of crashing.
        """
        tool_call_id = tool_call.get("id") or f"tc_{uuid6.uuid7().hex}"
        fn_name = (tool_call.get("function") or {}).get("name") or "unknown"
        tool_call_card: Optional[Card] = None

        project_id = str(ctx.project_id or "")
        channel_id = str(ctx.channel_id or "")
        agent_id = str(ctx.agent_id or "")
        agent_turn_id = str(ctx.agent_turn_id or "")
        step_id = str(ctx.step_id or "")
        headers = dict(ctx.headers or {})
        trace_id = ctx.trace_id
        parent_step_id = ctx.parent_step_id
        if not project_id or not channel_id or not agent_id or not agent_turn_id or not step_id:
            raise ValueError("UTPDispatcher.execute requires ctx with project/channel/agent/turn/step ids")

        span = _span
        op_ctx = CGContext(
            project_id=project_id,
            channel_id=channel_id,
            agent_id=agent_id,
            agent_turn_id=agent_turn_id,
            trace_id=trace_id,
            headers=dict(headers or {}),
            step_id=step_id,
            parent_step_id=parent_step_id,
        )
        if span is not None:
            span.set_attribute("cg.project_id", str(project_id))
            span.set_attribute("cg.channel_id", str(channel_id))
            span.set_attribute("cg.agent_id", str(agent_id))
            span.set_attribute("cg.agent_turn_id", str(agent_turn_id))
            span.set_attribute("cg.step_id", str(step_id))
            span.set_attribute("cg.tool.name", str(fn_name))
            span.set_attribute("cg.tool_call_id", str(tool_call_id))

            try:
                args, parse_error = parse_tool_args(tool_call)
                if parse_error:
                    tool_call_card = await self._save_tool_call_card(
                        project_id=project_id,
                        agent_id=agent_id,
                        agent_turn_id=agent_turn_id,
                        step_id=step_id,
                        tool_call_id=tool_call_id,
                        fn_name=fn_name,
                        args=args,
                        target_subject=None,
                        trace_id=trace_id,
                        parent_step_id=parent_step_id,
                    )
                    return await create_error_outcome(
                        cardbox=self.cardbox,
                        ctx=op_ctx,
                        tool_call_id=tool_call_id,
                        fn_name=fn_name,
                        exc=BadRequestError(parse_error),
                        tool_call_meta=getattr(tool_call_card, "metadata", {}) or {},
                        tool_call_card=tool_call_card,
                    )

                builtin_after_exec = BUILTIN_AFTER_EXEC.get(fn_name)
                if builtin_after_exec == "terminate" and self.execution_store:
                    exclude_inbox_id = guard_inbox_id or inbox_id
                    tool_call_meta = {
                        "step_id": step_id,
                        **({"trace_id": trace_id} if trace_id else {}),
                        **({"parent_step_id": parent_step_id} if parent_step_id else {}),
                    }
                    cmd_data = {
                        "tool_call_id": tool_call_id,
                        "agent_turn_id": agent_turn_id,
                        "turn_epoch": turn_epoch,
                        "agent_id": agent_id,
                        "after_execution": "suspend",
                        "tool_name": fn_name,
                    }
                    guard_outcome = await self._check_inbox_guard(
                        project_id=project_id,
                        agent_id=agent_id,
                        inbox_id=exclude_inbox_id,
                        fn_name=fn_name,
                        tool_call_meta=tool_call_meta,
                        cmd_data=cmd_data,
                        tool_call_card=None,
                    )
                    if guard_outcome is not None:
                        return guard_outcome

                outcome = await try_execute_builtin_tool(
                    fn_name=fn_name,
                    args=args,
                    cardbox=self.cardbox,
                    ctx=op_ctx,
                    tool_call_id=tool_call_id,
                    context_box_id=context_box_id,
                )
                if outcome is not None:
                    return outcome

                tool_def = await self.resource_store.fetch_tool_definition_model(project_id, fn_name)
                if not tool_def:
                    tool_call_card = await self._save_tool_call_card(
                        project_id=project_id,
                        agent_id=agent_id,
                        agent_turn_id=agent_turn_id,
                        step_id=step_id,
                        tool_call_id=tool_call_id,
                        fn_name=fn_name,
                        args=args,
                        target_subject=None,
                        trace_id=trace_id,
                        parent_step_id=parent_step_id,
                    )
                    return await create_error_outcome(
                        cardbox=self.cardbox,
                        ctx=op_ctx,
                        tool_call_id=tool_call_id,
                        fn_name=fn_name,
                        exc=NotFoundError(f"Tool definition not found: {fn_name}"),
                        tool_call_meta=getattr(tool_call_card, "metadata", {}) or {},
                        tool_call_card=tool_call_card,
                    )

                target_subject_tpl = tool_def.target_subject
                after_exec = tool_def.after_execution
                if not target_subject_tpl or not after_exec:
                    tool_call_card = await self._save_tool_call_card(
                        project_id=project_id,
                        agent_id=agent_id,
                        agent_turn_id=agent_turn_id,
                        step_id=step_id,
                        tool_call_id=tool_call_id,
                        fn_name=fn_name,
                        args=args,
                        target_subject=target_subject_tpl,
                        trace_id=trace_id,
                        parent_step_id=parent_step_id,
                    )
                    return await create_error_outcome(
                        cardbox=self.cardbox,
                        ctx=op_ctx,
                        tool_call_id=tool_call_id,
                        fn_name=fn_name,
                        exc=InternalError(f"Invalid tool definition (missing subject/lifecycle): {fn_name}"),
                        tool_call_meta=getattr(tool_call_card, "metadata", {}) or {},
                        tool_call_card=tool_call_card,
                    )
                span.set_attribute("cg.after_execution", str(after_exec))

                target_subject = target_subject_tpl.replace("{project_id}", project_id).replace(
                    "{channel_id}", channel_id
                )
                arg_context = {
                    "project_id": project_id,
                    "channel_id": channel_id,
                    "agent_id": agent_id,
                    "agent_turn_id": agent_turn_id,
                    "step_id": step_id,
                    "tool_call_id": tool_call_id,
                    "parent_step_id": parent_step_id,
                    "trace_id": trace_id,
                    "turn_epoch": turn_epoch,
                    "context_box_id": context_box_id,
                }
                dispatch_requested_at = to_iso(utc_now()) if self.timing_tool_dispatch else None
                tool_call_card = await self._save_tool_call_card(
                    project_id=project_id,
                    agent_id=agent_id,
                    agent_turn_id=agent_turn_id,
                    step_id=step_id,
                    tool_call_id=tool_call_id,
                    fn_name=fn_name,
                    args=args,
                    target_subject=target_subject,
                    trace_id=trace_id,
                    parent_step_id=parent_step_id,
                    tool_def=tool_def,
                    arg_context=arg_context,
                    dispatch_requested_at=dispatch_requested_at,
                )

                if after_exec == "terminate" and self.execution_store:
                    exclude_inbox_id = guard_inbox_id or inbox_id
                    tool_call_meta = getattr(tool_call_card, "metadata", {}) or {
                        "step_id": step_id,
                        "trace_id": trace_id,
                        "parent_step_id": parent_step_id,
                    }
                    cmd_data = {
                        "tool_call_id": tool_call_id,
                        "agent_turn_id": agent_turn_id,
                        "turn_epoch": turn_epoch,
                        "agent_id": agent_id,
                        "after_execution": "suspend",
                        "tool_name": fn_name,
                    }
                    guard_outcome = await self._check_inbox_guard(
                        project_id=project_id,
                        agent_id=agent_id,
                        inbox_id=exclude_inbox_id,
                        fn_name=fn_name,
                        tool_call_meta=tool_call_meta,
                        cmd_data=cmd_data,
                        tool_call_card=tool_call_card,
                    )
                    if guard_outcome is not None:
                        return guard_outcome

                await record_tool_call_edge(
                    execution_store=self.execution_store,
                    ctx=op_ctx,
                    tool_call_id=str(tool_call_id),
                    tool_name=str(fn_name),
                    logger=logger,
                )

                payload = {
                    "project_id": project_id,
                    "channel": channel_id,
                    "agent_id": agent_id,
                    "agent_turn_id": agent_turn_id,
                    "turn_epoch": turn_epoch,
                    "step_id": step_id,
                    "tool_call_id": tool_call_id,
                    "tool_name": fn_name,
                    "after_execution": after_exec,
                    "context_box_id": context_box_id,
                    "tool_call_card_id": tool_call_card.card_id,
                }
                if dispatch_requested_at:
                    payload["dispatch_requested_at"] = dispatch_requested_at

                dispatch_start_ts = time.perf_counter()
                logger.info("📤 UTP Dispatch: %s -> %s (after=%s)", fn_name, target_subject, after_exec)
                await asyncio.wait_for(
                    self.nats.publish_event(target_subject, payload, headers=headers),
                    timeout=self.DEFAULT_TIMEOUT,
                )
                if self.timing_tool_dispatch:
                    dispatch_duration_ms = monotonic_ms(dispatch_start_ts)
                    logger.info(
                        "📤 UTP Dispatch sent: tool_call_id=%s duration_ms=%s",
                        tool_call_id,
                        dispatch_duration_ms,
                    )
                return ActionOutcome(cards=[tool_call_card], suspend=True, mark_complete=False)

            except asyncio.TimeoutError as exc:
                mark_span_error(span, exc)
                logger.warning(f"⏳ Tool '{fn_name}' timed out after {self.DEFAULT_TIMEOUT}s")
                return await create_error_outcome(
                    cardbox=self.cardbox,
                    ctx=op_ctx,
                    tool_call_id=tool_call_id,
                    fn_name=fn_name,
                    exc=ToolTimeoutError(f"Execution timed out ({self.DEFAULT_TIMEOUT}s)"),
                    tool_call_meta=getattr(tool_call_card, "metadata", {}) or {
                        "step_id": step_id,
                        "trace_id": trace_id,
                        "parent_step_id": parent_step_id,
                    },
                    tool_call_card=tool_call_card,
                )
            except Exception as e:
                mark_span_error(span, e)
                logger.error(f"💥 Tool '{fn_name}' failed: {e}")
                return await create_error_outcome(
                    cardbox=self.cardbox,
                    ctx=op_ctx,
                    tool_call_id=tool_call_id,
                    fn_name=fn_name,
                    exc=InternalError(f"Execution error: {str(e)}"),
                    tool_call_meta=getattr(tool_call_card, "metadata", {}) or {
                        "step_id": step_id,
                        "trace_id": trace_id,
                        "parent_step_id": parent_step_id,
                    },
                    tool_call_card=tool_call_card,
                )
    async def _save_tool_call_card(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        step_id: str,
        tool_call_id: str,
        fn_name: str,
        args: Dict[str, Any],
        target_subject: Optional[str],
        trace_id: Optional[str],
        parent_step_id: Optional[str],
        tool_def: Optional[Any] = None,
        arg_context: Optional[Dict[str, Any]] = None,
        dispatch_requested_at: Optional[str] = None,
    ) -> Card:
        merged_args = args
        metadata = {
            "agent_turn_id": agent_turn_id,
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            **({"trace_id": trace_id} if trace_id else {}),
            **({"parent_step_id": parent_step_id} if parent_step_id else {}),
        }
        if dispatch_requested_at:
            metadata["dispatch_requested_at"] = dispatch_requested_at
        if tool_def and arg_context:
            merged_args, applied_defaults, applied_meta = apply_tool_arg_options(
                args=args,
                tool_options=getattr(tool_def, "options", None),
                context=arg_context,
                logger=logger,
            )
            applied_fixed = applied_meta.get("fixed") if isinstance(applied_meta, dict) else {}
            envs_meta = applied_meta.get("envs") if isinstance(applied_meta, dict) else {}
            if applied_defaults:
                metadata["tool_args_defaults_applied"] = sorted(applied_defaults.keys())
            if applied_fixed:
                metadata["tool_args_fixed_applied"] = sorted(applied_fixed.keys())
            if envs_meta:
                metadata["tool_args_envs"] = envs_meta
        card = Card(
            card_id=uuid6.uuid7().hex,
            project_id=project_id,
            type="tool.call",
            content=ToolCallContent(
                tool_name=fn_name,
                arguments=merged_args,
                status="called",
                target_subject=target_subject,
            ),
            created_at=datetime.now(UTC),
            author_id=agent_id,
            metadata=metadata,
            tool_call_id=tool_call_id,
        )
        await self.cardbox.save_card(card)
        return card
