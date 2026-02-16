from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, Dict, Optional

import uuid6

from core.errors import BadRequestError, NotFoundError
from core.subject import format_subject
from core.utp_protocol import Card, ToolCallContent, extract_tool_result_payload
from core.utils import safe_str
from infra.cardbox_client import CardBoxClient
from infra.agent_routing import resolve_agent_target
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore
from infra.worker_helpers import normalize_headers

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
        self._bg_tasks: set[asyncio.Task] = set()

    async def call(
        self,
        *,
        project_id: str,
        channel_id: str,
        caller_agent_id: str,
        tool_name: str,
        args: Dict[str, Any],
        context_box_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        turn_epoch: int = 0,
        await_result: bool = False,
        ack_result: bool = True,
        timeout_s: Optional[float] = None,
        poll_interval_s: Optional[float] = None,
    ) -> ToolCallResult:
        if not project_id:
            raise BadRequestError("ToolCaller: project_id is required")
        if not channel_id:
            raise BadRequestError("ToolCaller: channel_id is required")
        if not caller_agent_id:
            raise BadRequestError("ToolCaller: caller_agent_id is required")
        if not tool_name:
            raise BadRequestError("ToolCaller: tool_name is required")
        if not isinstance(args, dict):
            raise BadRequestError("ToolCaller: args must be a dict")

        tool_def = await self.resource_store.fetch_tool_definition_model(project_id, tool_name)
        if not tool_def:
            raise NotFoundError(f"ToolCaller: tool definition not found: {tool_name}")

        target_subject_tpl = safe_str(getattr(tool_def, "target_subject", None))
        after_execution = safe_str(getattr(tool_def, "after_execution", None))
        if not target_subject_tpl or not after_execution:
            raise BadRequestError(f"ToolCaller: invalid tool definition for {tool_name}")

        default_depth = await self.execution_store.get_active_recursion_depth(
            project_id=project_id,
            agent_id=caller_agent_id,
        )
        headers, trace_id, depth = normalize_headers(
            nats=self.nats,
            headers=headers,
            trace_id=trace_id,
            default_depth=int(default_depth or 0),
        )

        tool_call_id = f"tc_{uuid6.uuid7().hex}"
        step_id = f"step_sys_{uuid6.uuid7().hex}"
        agent_turn_id = f"turn_sys_{uuid6.uuid7().hex}"

        target_subject = (
            target_subject_tpl.replace("{project_id}", project_id).replace("{channel_id}", channel_id)
        )

        tool_call_card = Card(
            card_id=uuid6.uuid7().hex,
            project_id=project_id,
            type="tool.call",
            content=ToolCallContent(
                tool_name=tool_name,
                arguments=args,
                status="called",
                target_subject=target_subject,
            ),
            created_at=datetime.now(UTC),
            author_id=caller_agent_id,
            metadata={
                "agent_turn_id": agent_turn_id,
                "step_id": step_id,
                "tool_call_id": tool_call_id,
                **({"trace_id": trace_id} if trace_id else {}),
                **({"parent_step_id": parent_step_id} if parent_step_id else {}),
            },
            tool_call_id=tool_call_id,
        )
        await self.cardbox.save_card(tool_call_card)

        try:
            await self.execution_store.insert_execution_edge(
                edge_id=f"edge_{uuid6.uuid7().hex}",
                project_id=project_id,
                channel_id=channel_id,
                primitive="tool_call",
                edge_phase="request",
                source_agent_id=caller_agent_id,
                source_agent_turn_id=agent_turn_id,
                source_step_id=step_id,
                target_agent_id=caller_agent_id,
                target_agent_turn_id=agent_turn_id,
                correlation_id=tool_call_id,
                enqueue_mode=None,
                recursion_depth=int(depth),
                trace_id=trace_id,
                parent_step_id=parent_step_id,
                metadata={
                    "tool_call_id": tool_call_id,
                    "tool_name": tool_name,
                    "step_id": step_id,
                    "recursion_depth": int(depth),
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("ToolCaller: execution edge insert failed: %s", exc)

        payload = {
            "project_id": project_id,
            "channel": channel_id,
            "agent_id": caller_agent_id,
            "agent_turn_id": agent_turn_id,
            "turn_epoch": int(turn_epoch),
            "step_id": step_id,
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "after_execution": after_execution,
            "context_box_id": context_box_id,
            "tool_call_card_id": tool_call_card.card_id,
        }
        await self.nats.publish_event(target_subject, payload, headers=headers)

        timeout = float(timeout_s or self.DEFAULT_TIMEOUT_S)
        interval = float(poll_interval_s or self.DEFAULT_POLL_INTERVAL_S)
        if await_result:
            payload = await self._await_tool_result(
                project_id=project_id,
                channel_id=channel_id,
                caller_agent_id=caller_agent_id,
                tool_call_id=tool_call_id,
                timeout_s=timeout,
                poll_interval_s=interval,
                ack_result=ack_result,
            )
            return ToolCallResult(
                tool_call_id=tool_call_id,
                tool_call_card_id=tool_call_card.card_id,
                tool_result_card_id=safe_str((payload or {}).get("tool_result_card_id")),
                status=safe_str((payload or {}).get("status")),
                payload=payload,
                step_id=step_id,
                agent_turn_id=agent_turn_id,
                trace_id=trace_id,
            )

        if ack_result:
            task = asyncio.create_task(
                self._await_tool_result(
                    project_id=project_id,
                    channel_id=channel_id,
                    caller_agent_id=caller_agent_id,
                    tool_call_id=tool_call_id,
                    timeout_s=timeout,
                    poll_interval_s=interval,
                    ack_result=True,
                ),
                name=f"tool_caller_ack:{tool_call_id}",
            )
            self._bg_tasks.add(task)
            task.add_done_callback(self._bg_tasks.discard)

        return ToolCallResult(
            tool_call_id=tool_call_id,
            tool_call_card_id=tool_call_card.card_id,
            tool_result_card_id=None,
            status=None,
            payload=None,
            step_id=step_id,
            agent_turn_id=agent_turn_id,
            trace_id=trace_id,
        )

    async def _await_tool_result(
        self,
        *,
        project_id: str,
        channel_id: str,
        caller_agent_id: str,
        tool_call_id: str,
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
                project_id=project_id,
                agent_id=caller_agent_id,
            )
            if not target:
                logger.warning(
                    "ToolCaller: wakeup subscription skipped (missing worker_target) agent=%s",
                    caller_agent_id,
                )
                subscription = None
            else:
                subject = format_subject(project_id, channel_id, "cmd", "agent", target, "wakeup")

                async def _on_wakeup(msg) -> None:
                    try:
                        data = json.loads(msg.data.decode("utf-8"))
                    except Exception:
                        return
                    if safe_str(data.get("agent_id")) != caller_agent_id:
                        return
                    wakeup_event.set()

                subscription = await self.nats.subscribe_core(subject, _on_wakeup)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ToolCaller: wakeup subscription failed: %s", exc)
            subscription = None

        try:
            payload = await self._fetch_tool_result(
                project_id=project_id,
                caller_agent_id=caller_agent_id,
                tool_call_id=tool_call_id,
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
                        await asyncio.wait_for(wakeup_event.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    wakeup_event.clear()
                    payload = await self._fetch_tool_result(
                        project_id=project_id,
                        caller_agent_id=caller_agent_id,
                        tool_call_id=tool_call_id,
                        ack_result=ack_result,
                    )
                    if payload:
                        return payload
                return await self._fetch_tool_result(
                    project_id=project_id,
                    caller_agent_id=caller_agent_id,
                    tool_call_id=tool_call_id,
                    ack_result=ack_result,
                )

            await asyncio.sleep(remaining)
            return await self._fetch_tool_result(
                project_id=project_id,
                caller_agent_id=caller_agent_id,
                tool_call_id=tool_call_id,
                ack_result=ack_result,
            )
        finally:
            if subscription:
                await subscription.unsubscribe()

    async def _fetch_tool_result(
        self,
        *,
        project_id: str,
        caller_agent_id: str,
        tool_call_id: str,
        ack_result: bool,
    ) -> Optional[Dict[str, Any]]:
        rows = await self.execution_store.list_inbox_by_correlation(
            project_id=project_id,
            agent_id=caller_agent_id,
            correlation_id=tool_call_id,
            limit=5,
        )
        row = self._pick_tool_result_row(rows)
        if not row:
            return None
        payload = row.get("payload") or {}
        tool_result_card_id = safe_str(payload.get("tool_result_card_id"))
        if tool_result_card_id:
            cards = await self.cardbox.get_cards([tool_result_card_id], project_id=project_id)
            card = cards[0] if cards else None
            if card and getattr(card, "type", "") == "tool.result":
                try:
                    tool_payload = extract_tool_result_payload(card)
                    if isinstance(tool_payload, dict):
                        payload = {**payload, **tool_payload}
                except Exception:  # noqa: BLE001
                    pass
        if ack_result:
            await self._consume_inbox_row(row)
        return payload

    @staticmethod
    def _pick_tool_result_row(rows: list[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        for row in rows or []:
            if row.get("message_type") != "tool_result":
                continue
            payload = row.get("payload") or {}
            if payload.get("tool_call_id") and payload.get("tool_result_card_id"):
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
