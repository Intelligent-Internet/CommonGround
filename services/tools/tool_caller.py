from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, Dict, Optional

import uuid6

from core.cg_context import CGContext
from core.errors import BadRequestError, InternalError, NotFoundError
from core.headers import CG_AGENT_ID
from core.utp_protocol import Card, ToolCallContent
from core.utils import safe_str
from infra.cardbox_client import CardBoxClient
from infra.agent_routing import resolve_agent_target
from infra.l0_engine import CommandIntent, L0Engine
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore
from infra.tool_executor import fetch_and_parse_tool_result

logger = logging.getLogger("ToolCaller")


@dataclass(frozen=True)
class ToolCallResult:
    tool_call_id: str
    tool_call_card_id: str
    tool_result_card_id: Optional[str]
    status: Optional[str]
    payload: Optional[Dict[str, Any]]
    step_id: str
    agent_turn_id: str
    trace_id: Optional[str]


class ToolCaller:
    """Helper for non-agent services to call PMO/internal tools via standard tool.call."""

    DEFAULT_TIMEOUT_S = 3.0
    DEFAULT_POLL_INTERVAL_S = 0.5

    def __init__(
        self,
        *,
        cardbox: CardBoxClient,
        nats: NATSClient,
        resource_store: ResourceStore,
        execution_store: ExecutionStore,
    ) -> None:
        self.cardbox = cardbox
        self.nats = nats
        self.resource_store = resource_store
        self.execution_store = execution_store
        self.l0 = L0Engine(
            nats=self.nats,
            execution_store=self.execution_store,
            resource_store=self.resource_store,
            state_store=None,
        )
        self._bg_tasks: set[asyncio.Task] = set()

    async def call(
        self,
        *,
        caller_ctx: CGContext,
        tool_name: str,
        args: Dict[str, Any],
        context_box_id: Optional[str] = None,
        after_execution_override: Optional[str] = None,
        tool_call_id: Optional[str] = None,
        step_id: Optional[str] = None,
        agent_turn_id: Optional[str] = None,
        await_result: bool = False,
        ack_result: bool = True,
        timeout_s: Optional[float] = None,
        poll_interval_s: Optional[float] = None,
    ) -> ToolCallResult:
        if not isinstance(caller_ctx, CGContext):
            raise BadRequestError("ToolCaller: caller_ctx is required")
        if not tool_name:
            raise BadRequestError("ToolCaller: tool_name is required")
        if not isinstance(args, dict):
            raise BadRequestError("ToolCaller: args must be a dict")

        tool_def = await self.resource_store.fetch_tool_definition_model(caller_ctx.project_id, tool_name)
        if not tool_def:
            raise NotFoundError(f"ToolCaller: tool definition not found: {tool_name}")

        target_subject_tpl = safe_str(getattr(tool_def, "target_subject", None))
        tool_after_execution = safe_str(getattr(tool_def, "after_execution", None))
        if not target_subject_tpl or not tool_after_execution:
            raise BadRequestError(f"ToolCaller: invalid tool definition for {tool_name}")
        resolved_after_execution = safe_str(after_execution_override) or tool_after_execution
        if resolved_after_execution not in {"suspend", "terminate"}:
            raise BadRequestError(
                f"ToolCaller: invalid after_execution={resolved_after_execution!r} for {tool_name}"
            )

        resolved_tool_call_id = safe_str(tool_call_id) or f"tc_{uuid6.uuid7().hex}"
        resolved_step_id = safe_str(step_id) or f"step_sys_{uuid6.uuid7().hex}"
        resolved_agent_turn_id = safe_str(agent_turn_id) or f"turn_sys_{uuid6.uuid7().hex}"

        target_subject = caller_ctx.render_subject_template(target_subject_tpl)
        default_depth = await self.execution_store.get_active_recursion_depth(
            ctx=caller_ctx,
        )
        call_ctx = (
            caller_ctx.with_turn(
                resolved_agent_turn_id,
                0,
            )
            .with_new_step(resolved_step_id)
            .with_tool_call(resolved_tool_call_id)
            .with_trace_transport(
                base_headers=caller_ctx.headers,
                trace_id=caller_ctx.trace_id,
                default_depth=int(default_depth or 0),
            )
        )

        tool_call_card = Card(
            card_id=uuid6.uuid7().hex,
            project_id=call_ctx.project_id,
            type="tool.call",
            content=ToolCallContent(
                tool_name=tool_name,
                arguments=args,
                status="called",
                target_subject=target_subject,
            ),
            created_at=datetime.now(UTC),
            author_id=call_ctx.agent_id,
            metadata={
                "agent_turn_id": call_ctx.agent_turn_id,
                "step_id": call_ctx.step_id,
                "tool_call_id": resolved_tool_call_id,
                "context_box_id": safe_str(context_box_id) or None,
                **call_ctx.to_lineage_meta(exclude_step_id=True),
            },
            tool_call_id=resolved_tool_call_id,
        )
        await self.cardbox.save_card(tool_call_card)

        payload = {
            "tool_name": tool_name,
            "after_execution": resolved_after_execution,
            "tool_call_card_id": tool_call_card.card_id,
        }
        command_signals = []
        async with self.execution_store.pool.connection() as conn:
            async with conn.transaction():
                result = await self.l0.command_intent(
                    source_ctx=call_ctx,
                    intent=CommandIntent(
                        subject=target_subject,
                        payload=payload,
                        correlation_id=resolved_tool_call_id,
                        primitive="tool_call",
                        metadata={
                            "tool_call_id": resolved_tool_call_id,
                            "tool_name": tool_name,
                            "step_id": call_ctx.step_id,
                            "recursion_depth": call_ctx.recursion_depth,
                        },
                    ),
                    conn=conn,
                )
                if result.status != "accepted":
                    raise InternalError(
                        f"ToolCaller dispatch rejected: status={result.status} code={result.error_code}"
                    )
                command_signals = list(result.command_signals or ())
        if command_signals:
            await self.l0.publish_command_signals(command_signals)

        timeout = float(timeout_s or self.DEFAULT_TIMEOUT_S)
        interval = float(poll_interval_s or self.DEFAULT_POLL_INTERVAL_S)
        if await_result:
            payload = await self._await_tool_result(
                caller_ctx=call_ctx,
                timeout_s=timeout,
                poll_interval_s=interval,
                ack_result=ack_result,
            )
            return ToolCallResult(
                tool_call_id=resolved_tool_call_id,
                tool_call_card_id=tool_call_card.card_id,
                tool_result_card_id=safe_str((payload or {}).get("tool_result_card_id")),
                status=safe_str((payload or {}).get("status")),
                payload=payload,
                step_id=call_ctx.step_id or resolved_step_id,
                agent_turn_id=call_ctx.agent_turn_id,
                trace_id=call_ctx.trace_id,
            )

        if ack_result:
            task = asyncio.create_task(
                self._await_tool_result(
                    caller_ctx=call_ctx,
                    timeout_s=timeout,
                    poll_interval_s=interval,
                    ack_result=True,
                ),
                name=f"tool_caller_ack:{resolved_tool_call_id}",
            )
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)

        return ToolCallResult(
            tool_call_id=resolved_tool_call_id,
            tool_call_card_id=tool_call_card.card_id,
            tool_result_card_id=None,
            status=None,
            payload=None,
            step_id=call_ctx.step_id or resolved_step_id,
            agent_turn_id=call_ctx.agent_turn_id,
            trace_id=call_ctx.trace_id,
        )

    async def _await_tool_result(
        self,
        *,
        caller_ctx: CGContext,
        timeout_s: float,
        poll_interval_s: float,
        ack_result: bool,
    ) -> Optional[Dict[str, Any]]:
        deadline = asyncio.get_event_loop().time() + float(timeout_s)
        wakeup_event = asyncio.Event()
        subscription = None
        try:
            target = await resolve_agent_target(
                resource_store=self.resource_store,
                ctx=caller_ctx,
            )
            if not target:
                logger.warning(
                    "ToolCaller: wakeup subscription skipped (missing worker_target) agent=%s",
                    caller_ctx.agent_id,
                )
                subscription = None
            else:
                subject = caller_ctx.subject("cmd", "agent", target, "wakeup")

                async def _on_wakeup(msg) -> None:
                    msg_headers = dict(getattr(msg, "headers", {}) or {})
                    if safe_str(msg_headers.get(CG_AGENT_ID)) != caller_ctx.agent_id:
                        return
                    wakeup_event.set()

                subscription = await self.nats.subscribe_core(subject, _on_wakeup)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ToolCaller: wakeup subscription failed: %s", exc)
            subscription = None

        try:
            payload = await self._fetch_tool_result(
                caller_ctx=caller_ctx,
                ack_result=ack_result,
            )
            if payload:
                return payload

            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None

            if subscription:
                while asyncio.get_event_loop().time() <= deadline:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        wait_timeout = min(float(poll_interval_s), max(0.0, remaining))
                        if wait_timeout <= 0.0:
                            break
                        await asyncio.wait_for(wakeup_event.wait(), timeout=wait_timeout)
                    except asyncio.TimeoutError:
                        payload = await self._fetch_tool_result(
                            caller_ctx=caller_ctx,
                            ack_result=ack_result,
                        )
                        if payload:
                            return payload
                        continue
                    wakeup_event.clear()
                    payload = await self._fetch_tool_result(
                        caller_ctx=caller_ctx,
                        ack_result=ack_result,
                    )
                    if payload:
                        return payload
                return await self._fetch_tool_result(
                    caller_ctx=caller_ctx,
                    ack_result=ack_result,
                )

            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    break
                sleep_s = min(float(poll_interval_s), max(0.0, remaining))
                if sleep_s <= 0.0:
                    break
                await asyncio.sleep(sleep_s)
                payload = await self._fetch_tool_result(
                    caller_ctx=caller_ctx,
                    ack_result=ack_result,
                )
                if payload:
                    return payload
            return await self._fetch_tool_result(caller_ctx=caller_ctx, ack_result=ack_result)
        finally:
            if subscription:
                await subscription.unsubscribe()

    async def _fetch_tool_result(
        self,
        *,
        caller_ctx: CGContext,
        ack_result: bool,
    ) -> Optional[Dict[str, Any]]:
        correlation_id = caller_ctx.require_tool_call_id
        rows = await self.execution_store.list_inbox_by_correlation(
            ctx=caller_ctx,
            correlation_id=correlation_id,
            limit=5,
        )
        row = self._pick_tool_result_row(rows)
        if not row:
            return None
        payload = row.get("payload") or {}
        tool_result_card_id = safe_str(payload.get("tool_result_card_id"))
        if tool_result_card_id:
            read_result = await fetch_and_parse_tool_result(
                cardbox=self.cardbox,
                project_id=caller_ctx.project_id,
                card_id=tool_result_card_id,
                require_tool_result_type=True,
            )
            if read_result.error_code == "card_fetch_failed":
                raise RuntimeError(read_result.error_message or "tool_result_fetch_failed")
            if read_result.ok and isinstance(read_result.payload, dict):
                payload = {**payload, **read_result.payload}
        if ack_result:
            await self._consume_inbox_row(row)
        return payload

    @staticmethod
    def _pick_tool_result_row(rows: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for row in rows or []:
            if row.get("message_type") != "tool_result":
                continue
            payload = row.get("payload") or {}
            if payload.get("tool_result_card_id"):
                return row
        return None

    async def _consume_inbox_row(self, row: Dict[str, Any]) -> None:
        inbox_id = safe_str(row.get("inbox_id"))
        project_id = safe_str(row.get("project_id"))
        status = safe_str(row.get("status"))
        if not inbox_id or not project_id:
            return
        if status == "pending":
            await self.execution_store.update_inbox_status(
                inbox_id=inbox_id,
                project_id=project_id,
                status="processing",
                expected_status="pending",
            )
        await self.execution_store.update_inbox_status(
            inbox_id=inbox_id,
            project_id=project_id,
            status="consumed",
        )
