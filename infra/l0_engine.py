from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

import uuid6
from psycopg import Error as PsycopgError

from core.config import MAX_RECURSION_DEPTH
from core.errors import ProtocolViolationError
from core.headers import require_recursion_depth
from core.subject import format_subject
from core.trace import TRACEPARENT_HEADER, ensure_trace_headers
from core.utils import safe_str
from infra.agent_routing import resolve_agent_target
from infra.l0.errors import (
    ACK_STATUS_ACCEPTED,
    ACK_STATUS_BUSY,
    ACK_STATUS_ERROR,
    ACK_STATUS_REJECTED,
    ERROR_AGENT_BUSY,
    ERROR_INTERNAL_ERROR,
    ERROR_PROTOCOL_VIOLATION,
    ERROR_RECURSION_DEPTH_EXCEEDED,
    ERROR_STORAGE_ERROR,
)
from infra.nats_client import NATSClient
from infra.primitives import join_request, join_response, report as report_primitive
from infra.primitives.execution import build_enqueue_records
from infra.stores import ExecutionStore, ResourceStore, StateStore

logger = logging.getLogger("L0Engine")


@dataclass(frozen=True, slots=True)
class WakeupSignal:
    project_id: str
    channel_id: str
    target_agent_id: str
    headers: Optional[Dict[str, str]] = None
    inbox_id: Optional[str] = None
    reason: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True, slots=True)
class L0Result:
    status: Literal["accepted", "busy", "rejected", "error"]
    error_code: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    wakeup_signals: tuple[WakeupSignal, ...] = field(default_factory=tuple)

    @property
    def ack_status(self) -> str:
        return str(self.status)

    @property
    def ack_error_code(self) -> Optional[str]:
        return self.error_code

    @property
    def ack_payload(self) -> Dict[str, Any]:
        return dict(self.payload or {})


class L0Engine:
    """L0 transactional engine: DB is source of truth, NATS is wakeup doorbell."""

    def __init__(
        self,
        *,
        nats: NATSClient,
        execution_store: ExecutionStore,
        resource_store: ResourceStore,
        state_store: Optional[StateStore] = None,
    ) -> None:
        self.nats = nats
        self.execution_store = execution_store
        self.resource_store = resource_store
        self.state_store = state_store

    async def enqueue(
        self,
        *,
        project_id: str,
        channel_id: str,
        target_agent_id: str,
        message_type: str,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
        recursion_depth: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        source_agent_id: Optional[str] = None,
        source_agent_turn_id: Optional[str] = None,
        source_step_id: Optional[str] = None,
        target_agent_turn_id: Optional[str] = None,
        enqueue_mode: str = "call",
        wakeup: bool = True,
        conn: Any,
    ) -> L0Result:
        return await self._enqueue_with_existing_conn(
            conn,
            project_id=project_id,
            channel_id=channel_id,
            target_agent_id=target_agent_id,
            message_type=message_type,
            payload=payload,
            correlation_id=correlation_id,
            recursion_depth=recursion_depth,
            headers=headers,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            source_agent_id=source_agent_id,
            source_agent_turn_id=source_agent_turn_id,
            source_step_id=source_step_id,
            target_agent_turn_id=target_agent_turn_id,
            enqueue_mode=enqueue_mode,
            wakeup=wakeup,
        )

    async def _enqueue_with_existing_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        channel_id: str,
        target_agent_id: str,
        message_type: str,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
        recursion_depth: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        source_agent_id: Optional[str] = None,
        source_agent_turn_id: Optional[str] = None,
        source_step_id: Optional[str] = None,
        target_agent_turn_id: Optional[str] = None,
        enqueue_mode: str = "call",
        wakeup: bool = True,
    ) -> L0Result:
        try:
            if str(enqueue_mode or "call") != "call":
                raise ProtocolViolationError("unsupported enqueue_mode")
            if not project_id:
                raise ProtocolViolationError("missing project_id")
            if not channel_id:
                raise ProtocolViolationError("missing channel_id")
            if not target_agent_id:
                raise ProtocolViolationError("missing target_agent_id")
            if not message_type:
                raise ProtocolViolationError("missing message_type")
            if not isinstance(payload, dict):
                raise ProtocolViolationError("invalid payload")

            merged_headers = self.nats.merge_headers(headers or {})
            merged_headers, _, resolved_trace_id = ensure_trace_headers(
                merged_headers,
                trace_id=trace_id,
            )
            resolved_depth = recursion_depth
            if resolved_depth is None:
                resolved_depth = require_recursion_depth(merged_headers)
            resolved_depth = int(resolved_depth)
            if resolved_depth < 0:
                raise ProtocolViolationError("invalid recursion_depth")
            if resolved_depth >= MAX_RECURSION_DEPTH:
                return self._rejected(ERROR_RECURSION_DEPTH_EXCEEDED)

            if message_type == "turn":
                if self.state_store is None:
                    raise RuntimeError("state_store is required for enqueue(turn)")

                submit_payload = dict(payload or {})
                submit_agent_turn_id = safe_str(submit_payload.get("agent_turn_id")) or safe_str(
                    target_agent_turn_id
                )
                if not submit_agent_turn_id:
                    submit_agent_turn_id = f"turn_{uuid6.uuid7().hex}"
                submit_payload["agent_turn_id"] = submit_agent_turn_id

                tracestate = merged_headers.get("tracestate")
                inbox_record, edge_record, result = await build_enqueue_records(
                    store=self.execution_store,
                    project_id=project_id,
                    channel_id=channel_id,
                    target_agent_id=target_agent_id,
                    message_type=message_type,
                    payload=submit_payload,
                    enqueue_mode="call",
                    correlation_id=correlation_id or submit_agent_turn_id,
                    recursion_depth=resolved_depth,
                    headers=merged_headers,
                    traceparent=merged_headers.get("traceparent"),
                    tracestate=tracestate,
                    trace_id=resolved_trace_id,
                    parent_step_id=parent_step_id,
                    source_agent_id=source_agent_id,
                    source_agent_turn_id=source_agent_turn_id,
                    source_step_id=source_step_id,
                    target_agent_turn_id=submit_agent_turn_id,
                    inbox_status="queued",
                    conn=conn,
                )
                await self.execution_store.enqueue_with_conn(
                    conn,
                    inbox=inbox_record,
                    edge=edge_record,
                )

                dispatched: Optional[Dict[str, Any]] = None
                signals: list[WakeupSignal] = []
                if wakeup:
                    dispatched = await self._dispatch_next_if_idle_with_conn(
                        conn,
                        project_id=project_id,
                        agent_id=target_agent_id,
                    )
                    if dispatched:
                        wakeup_headers: Dict[str, str] = {}
                        tp = safe_str(dispatched.get("traceparent"))
                        if tp:
                            wakeup_headers[TRACEPARENT_HEADER] = tp
                        ts = safe_str(dispatched.get("tracestate"))
                        if ts:
                            wakeup_headers["tracestate"] = ts
                        signals.append(
                            WakeupSignal(
                                project_id=project_id,
                                channel_id=safe_str(dispatched.get("channel_id")) or channel_id,
                                target_agent_id=target_agent_id,
                                headers=wakeup_headers or None,
                                inbox_id=safe_str(dispatched.get("inbox_id")) or None,
                                reason="dispatch_next",
                            )
                        )

                submit_epoch: Optional[int] = None
                if dispatched and safe_str(dispatched.get("agent_turn_id")) == submit_agent_turn_id:
                    try:
                        submit_epoch = int(dispatched.get("turn_epoch"))
                    except Exception:  # noqa: BLE001
                        submit_epoch = None
                accepted_payload: Dict[str, Any] = {
                    "inbox_id": result.inbox_id if result else None,
                    "correlation_id": result.correlation_id if result else correlation_id,
                    "trace_id": result.trace_id if result else resolved_trace_id,
                    "agent_turn_id": submit_agent_turn_id,
                }
                if submit_epoch is not None:
                    accepted_payload["turn_epoch"] = int(submit_epoch)
                return self._accepted(accepted_payload, wakeup_signals=signals)

            if message_type == "ui_action":
                if self.state_store is None:
                    raise RuntimeError("state_store is required for enqueue(ui_action)")

                submit_payload = dict(payload or {})
                submit_agent_turn_id = safe_str(submit_payload.get("agent_turn_id")) or safe_str(
                    target_agent_turn_id
                )
                if not submit_agent_turn_id:
                    submit_agent_turn_id = f"turn_{uuid6.uuid7().hex}"
                submit_payload["agent_turn_id"] = submit_agent_turn_id

                turn_epoch = await self.state_store.lease_agent_turn_with_conn(
                    conn,
                    project_id=project_id,
                    agent_id=target_agent_id,
                    agent_turn_id=submit_agent_turn_id,
                    active_channel_id=channel_id,
                    profile_box_id=safe_str(submit_payload.get("profile_box_id")),
                    context_box_id=safe_str(submit_payload.get("context_box_id")),
                    output_box_id=safe_str(submit_payload.get("output_box_id")),
                    parent_step_id=parent_step_id,
                    trace_id=resolved_trace_id,
                    active_recursion_depth=resolved_depth,
                )
                if turn_epoch is None:
                    return self._busy(ERROR_AGENT_BUSY)
                submit_payload["turn_epoch"] = int(turn_epoch)

                tracestate = merged_headers.get("tracestate")
                inbox_record, edge_record, result = await build_enqueue_records(
                    store=self.execution_store,
                    project_id=project_id,
                    channel_id=channel_id,
                    target_agent_id=target_agent_id,
                    message_type=message_type,
                    payload=submit_payload,
                    enqueue_mode="call",
                    correlation_id=correlation_id,
                    recursion_depth=resolved_depth,
                    headers=merged_headers,
                    traceparent=merged_headers.get("traceparent"),
                    tracestate=tracestate,
                    trace_id=resolved_trace_id,
                    parent_step_id=parent_step_id,
                    source_agent_id=source_agent_id,
                    source_agent_turn_id=source_agent_turn_id,
                    source_step_id=source_step_id,
                    target_agent_turn_id=submit_agent_turn_id,
                    conn=conn,
                )
                await self.execution_store.enqueue_with_conn(conn, inbox=inbox_record, edge=edge_record)

                signals = []
                if wakeup:
                    wakeup_headers = self._merge_traceparent(merged_headers, result.traceparent)
                    signals.append(
                        WakeupSignal(
                            project_id=project_id,
                            channel_id=channel_id,
                            target_agent_id=target_agent_id,
                            headers=wakeup_headers,
                        )
                    )
                return self._accepted(
                    {
                        "inbox_id": result.inbox_id,
                        "correlation_id": result.correlation_id,
                        "trace_id": result.trace_id,
                        "agent_turn_id": submit_agent_turn_id,
                        "turn_epoch": int(turn_epoch),
                    },
                    wakeup_signals=signals,
                )

            tracestate = merged_headers.get("tracestate")
            inbox_record, edge_record, result = await build_enqueue_records(
                store=self.execution_store,
                project_id=project_id,
                channel_id=channel_id,
                target_agent_id=target_agent_id,
                message_type=message_type,
                payload=payload,
                enqueue_mode="call",
                correlation_id=correlation_id,
                recursion_depth=resolved_depth,
                headers=merged_headers,
                traceparent=merged_headers.get("traceparent"),
                tracestate=tracestate,
                trace_id=resolved_trace_id,
                parent_step_id=parent_step_id,
                source_agent_id=source_agent_id,
                source_agent_turn_id=source_agent_turn_id,
                source_step_id=source_step_id,
                target_agent_turn_id=target_agent_turn_id,
                conn=conn,
            )
            await self.execution_store.enqueue_with_conn(conn, inbox=inbox_record, edge=edge_record)

            signals = []
            if wakeup:
                wakeup_headers = self._merge_traceparent(merged_headers, result.traceparent)
                signals.append(
                    WakeupSignal(
                        project_id=project_id,
                        channel_id=channel_id,
                        target_agent_id=target_agent_id,
                        headers=wakeup_headers,
                    )
                )
            return self._accepted(
                {
                    "inbox_id": result.inbox_id,
                    "correlation_id": result.correlation_id,
                    "trace_id": result.trace_id,
                },
                wakeup_signals=signals,
            )
        except ProtocolViolationError as exc:
            if str(exc) == ERROR_RECURSION_DEPTH_EXCEEDED:
                return self._rejected(ERROR_RECURSION_DEPTH_EXCEEDED)
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except PsycopgError:
            return self._error(ERROR_STORAGE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 enqueue(tx) failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def report(
        self,
        *,
        project_id: str,
        channel_id: str,
        target_agent_id: str,
        message_type: str,
        payload: Dict[str, Any],
        correlation_id: str,
        recursion_depth: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        source_agent_id: Optional[str] = None,
        source_agent_turn_id: Optional[str] = None,
        source_step_id: Optional[str] = None,
        target_agent_turn_id: Optional[str] = None,
        wakeup: bool = True,
        conn: Any,
    ) -> L0Result:
        try:
            merged_headers = self.nats.merge_headers(headers or {})
            merged_headers, _, resolved_trace_id = ensure_trace_headers(merged_headers, trace_id=trace_id)
            resolved_depth = recursion_depth
            if resolved_depth is None:
                resolved_depth = require_recursion_depth(merged_headers)
            resolved_depth = int(resolved_depth)
            if resolved_depth < 0:
                raise ProtocolViolationError("invalid recursion_depth")
            if resolved_depth >= MAX_RECURSION_DEPTH:
                return self._rejected(ERROR_RECURSION_DEPTH_EXCEEDED)
            tracestate = merged_headers.get("tracestate")
            result = await report_primitive(
                store=self.execution_store,
                project_id=project_id,
                channel_id=channel_id,
                target_agent_id=target_agent_id,
                message_type=message_type,
                payload=payload,
                correlation_id=correlation_id,
                recursion_depth=resolved_depth,
                headers=merged_headers,
                traceparent=merged_headers.get("traceparent"),
                tracestate=tracestate,
                trace_id=resolved_trace_id,
                parent_step_id=parent_step_id,
                source_agent_id=source_agent_id,
                source_agent_turn_id=source_agent_turn_id,
                source_step_id=source_step_id,
                target_agent_turn_id=target_agent_turn_id,
                conn=conn,
            )
            signals: list[WakeupSignal] = []
            if wakeup:
                wakeup_headers = self._merge_traceparent(merged_headers, result.traceparent)
                signals.append(
                    WakeupSignal(
                        project_id=project_id,
                        channel_id=channel_id,
                        target_agent_id=target_agent_id,
                        headers=wakeup_headers,
                    )
                )
            return self._accepted(
                {
                    "inbox_id": result.inbox_id,
                    "correlation_id": result.correlation_id,
                    "trace_id": result.trace_id,
                },
                wakeup_signals=signals,
            )
        except ProtocolViolationError:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except PsycopgError:
            return self._error(ERROR_STORAGE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 report(tx) failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def join(
        self,
        *,
        phase: Literal["request", "response"],
        project_id: str,
        channel_id: str,
        source_agent_id: Optional[str],
        source_agent_turn_id: Optional[str],
        source_step_id: Optional[str],
        target_agent_id: Optional[str],
        target_agent_turn_id: Optional[str],
        correlation_id: str,
        recursion_depth: Optional[int],
        trace_id: Optional[str],
        parent_step_id: Optional[str],
        metadata: Optional[Dict[str, Any]] = None,
        conn: Any,
    ) -> L0Result:
        try:
            join_fn = join_request if phase == "request" else join_response
            result = await join_fn(
                store=self.execution_store,
                project_id=project_id,
                channel_id=channel_id,
                source_agent_id=source_agent_id,
                source_agent_turn_id=source_agent_turn_id,
                source_step_id=source_step_id,
                target_agent_id=target_agent_id,
                target_agent_turn_id=target_agent_turn_id,
                correlation_id=correlation_id,
                recursion_depth=recursion_depth,
                trace_id=trace_id,
                parent_step_id=parent_step_id,
                metadata=metadata,
                conn=conn,
            )
            return self._accepted(
                {
                    "edge_id": result.edge_id,
                    "correlation_id": result.correlation_id,
                    "trace_id": result.trace_id,
                }
            )
        except ProtocolViolationError:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except PsycopgError:
            return self._error(ERROR_STORAGE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 join failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def cancel(
        self,
        *,
        project_id: str,
        channel_id: str,
        target_agent_id: str,
        agent_turn_id: str,
        reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> L0Result:
        try:
            patch: Dict[str, Any] = {
                "cancel_reason": safe_str(reason) or "canceled",
            }
            if isinstance(metadata, dict) and metadata:
                patch["cancel_metadata"] = metadata
            canceled = False
            async with self.execution_store.pool.connection() as conn:
                async with conn.transaction():
                    res = await conn.execute(
                        """
                        UPDATE state.agent_inbox
                        SET status='skipped',
                            archived_at=COALESCE(archived_at, NOW()),
                            payload = payload || %s::jsonb
                        WHERE project_id=%s
                          AND agent_id=%s
                          AND inbox_id=%s
                          AND message_type='turn'
                          AND status='queued'
                        RETURNING inbox_id
                        """,
                        (
                            json.dumps(patch),
                            project_id,
                            target_agent_id,
                            agent_turn_id,
                        ),
                    )
                    row = await res.fetchone()
                    canceled = bool(row)
            return self._accepted({"canceled": canceled, "agent_turn_id": agent_turn_id})
        except PsycopgError:
            return self._error(ERROR_STORAGE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 cancel failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def wakeup(
        self,
        *,
        project_id: str,
        channel_id: str,
        target_agent_id: str,
        mode: Literal["default", "dispatch_next"] = "default",
        reason: Optional[str] = None,
        inbox_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> L0Result:
        try:
            if mode == "dispatch_next":
                if self.state_store is None:
                    raise RuntimeError("state_store is required for wakeup(dispatch_next)")
                dispatched: Optional[Dict[str, Any]] = None
                async with self.execution_store.pool.connection() as conn:
                    async with conn.transaction():
                        dispatched = await self._dispatch_next_if_idle_with_conn(
                            conn,
                            project_id=project_id,
                            agent_id=target_agent_id,
                        )
                if dispatched:
                    wakeup_headers: Dict[str, str] = {}
                    tp = safe_str(dispatched.get("traceparent"))
                    if tp:
                        wakeup_headers[TRACEPARENT_HEADER] = tp
                    ts = safe_str(dispatched.get("tracestate"))
                    if ts:
                        wakeup_headers["tracestate"] = ts
                    await self._publish_wakeup(
                        project_id=project_id,
                        channel_id=safe_str(dispatched.get("channel_id")) or channel_id,
                        target_agent_id=target_agent_id,
                        headers=wakeup_headers or None,
                        inbox_id=safe_str(dispatched.get("inbox_id")) or None,
                        reason=reason or "dispatch_next",
                        metadata=metadata,
                    )
                return self._accepted(dispatched or {})

            await self._publish_wakeup(
                project_id=project_id,
                channel_id=channel_id,
                target_agent_id=target_agent_id,
                headers=headers,
                inbox_id=inbox_id,
                reason=reason,
                metadata=metadata,
            )
            return self._accepted({})
        except PsycopgError:
            return self._error(ERROR_STORAGE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 wakeup failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def publish_wakeup_signal(self, signal: WakeupSignal) -> None:
        await self._publish_wakeup(
            project_id=signal.project_id,
            channel_id=signal.channel_id,
            target_agent_id=signal.target_agent_id,
            headers=signal.headers,
            inbox_id=signal.inbox_id,
            reason=signal.reason,
            metadata=signal.metadata,
        )

    async def publish_wakeup_signals(self, signals: list[WakeupSignal]) -> None:
        for signal in signals:
            await self.publish_wakeup_signal(signal)

    async def _dispatch_next_if_idle_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        agent_id: str,
    ) -> Optional[Dict[str, Any]]:
        if self.state_store is None:
            raise RuntimeError("state_store is required for dispatch_next")
        for _ in range(5):
            res = await conn.execute(
                """
                SELECT inbox_id, correlation_id, recursion_depth, traceparent, tracestate, trace_id,
                       parent_step_id, payload, created_at
                FROM state.agent_inbox
                WHERE project_id=%s
                  AND agent_id=%s
                  AND status='queued'
                  AND message_type='turn'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (project_id, agent_id),
            )
            row = await res.fetchone()
            if not row:
                return None

            inbox_row = dict(row)
            payload_any = inbox_row.get("payload") or {}
            if isinstance(payload_any, dict):
                payload = dict(payload_any)
            elif isinstance(payload_any, str):
                try:
                    parsed = json.loads(payload_any)
                    payload = dict(parsed) if isinstance(parsed, dict) else {}
                except Exception:  # noqa: BLE001
                    payload = {}
            else:
                payload = {}

            channel_id = safe_str(payload.get("channel_id"))
            profile_box_id = safe_str(payload.get("profile_box_id"))
            context_box_id = safe_str(payload.get("context_box_id"))
            output_box_id = safe_str(payload.get("output_box_id"))
            agent_turn_id = safe_str(payload.get("agent_turn_id")) or safe_str(inbox_row.get("correlation_id"))
            if not agent_turn_id:
                agent_turn_id = safe_str(inbox_row.get("inbox_id"))
            missing = [
                name
                for name, value in (
                    ("channel_id", channel_id),
                    ("profile_box_id", profile_box_id),
                    ("context_box_id", context_box_id),
                    ("output_box_id", output_box_id),
                    ("agent_turn_id", agent_turn_id),
                )
                if not value
            ]
            if missing:
                await conn.execute(
                    """
                    UPDATE state.agent_inbox
                    SET status='error',
                        archived_at=COALESCE(archived_at, NOW()),
                        payload = payload || %s::jsonb
                    WHERE project_id=%s AND inbox_id=%s AND status='queued'
                    """,
                    (
                        json.dumps({"dispatch_error": "missing_required_fields", "missing": missing}),
                        project_id,
                        safe_str(inbox_row.get("inbox_id")),
                    ),
                )
                logger.warning(
                    "L0 dispatch_next skipped invalid queued turn inbox=%s agent=%s missing=%s",
                    inbox_row.get("inbox_id"),
                    agent_id,
                    missing,
                )
                continue

            depth_raw = inbox_row.get("recursion_depth")
            try:
                recursion_depth = int(depth_raw) if depth_raw is not None else 0
            except Exception:  # noqa: BLE001
                recursion_depth = 0

            turn_epoch = await self.state_store.lease_agent_turn_with_conn(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                agent_turn_id=agent_turn_id,
                active_channel_id=channel_id,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
                parent_step_id=safe_str(inbox_row.get("parent_step_id")) or None,
                trace_id=safe_str(inbox_row.get("trace_id")) or None,
                active_recursion_depth=recursion_depth,
            )
            if turn_epoch is None:
                return None

            patch = {"turn_epoch": int(turn_epoch), "agent_turn_id": agent_turn_id}
            updated = await conn.execute(
                """
                UPDATE state.agent_inbox
                SET status='pending',
                    processed_at=NULL,
                    archived_at=NULL,
                    payload = payload || %s::jsonb
                WHERE project_id=%s AND inbox_id=%s AND status='queued'
                RETURNING inbox_id
                """,
                (
                    json.dumps(patch),
                    project_id,
                    safe_str(inbox_row.get("inbox_id")),
                ),
            )
            promoted = await updated.fetchone()
            if not promoted:
                logger.warning(
                    "L0 dispatch_next promote failed inbox=%s agent=%s (stale?)",
                    inbox_row.get("inbox_id"),
                    agent_id,
                )
                return None

            return {
                "inbox_id": safe_str(inbox_row.get("inbox_id")),
                "agent_turn_id": agent_turn_id,
                "turn_epoch": int(turn_epoch),
                "channel_id": channel_id,
                "traceparent": safe_str(inbox_row.get("traceparent")),
                "tracestate": safe_str(inbox_row.get("tracestate")),
                "trace_id": safe_str(inbox_row.get("trace_id")),
                "recursion_depth": recursion_depth,
            }
        return None

    async def _publish_wakeup(
        self,
        *,
        project_id: str,
        channel_id: str,
        target_agent_id: str,
        headers: Optional[Dict[str, str]],
        inbox_id: Optional[str] = None,
        reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            target = await resolve_agent_target(
                resource_store=self.resource_store,
                project_id=project_id,
                agent_id=target_agent_id,
            )
            if not target:
                logger.warning("L0 wakeup skipped: missing worker_target agent=%s", target_agent_id)
                return
            subject = format_subject(project_id, channel_id, "cmd", "agent", target, "wakeup")
            payload: Dict[str, Any] = {"agent_id": target_agent_id}
            if inbox_id:
                payload["inbox_id"] = inbox_id
            if reason:
                payload["reason"] = reason
            if metadata:
                payload["metadata"] = metadata
            await self.nats.publish_event(subject, payload, headers=headers)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "L0 wakeup publish failed agent=%s channel=%s: %s",
                target_agent_id,
                channel_id,
                exc,
            )

    def _merge_traceparent(
        self,
        headers: Optional[Dict[str, str]],
        traceparent: Optional[str],
    ) -> Optional[Dict[str, str]]:
        if headers is None and not traceparent:
            return None
        merged = dict(headers or {})
        if traceparent:
            merged[TRACEPARENT_HEADER] = traceparent
        return merged

    def _accepted(
        self,
        payload: Optional[Dict[str, Any]] = None,
        *,
        wakeup_signals: Optional[list[WakeupSignal]] = None,
    ) -> L0Result:
        return L0Result(
            status=ACK_STATUS_ACCEPTED,
            payload=dict(payload or {}),
            wakeup_signals=tuple(wakeup_signals or ()),
        )

    def _busy(self, error_code: Optional[str] = None) -> L0Result:
        return L0Result(status=ACK_STATUS_BUSY, error_code=error_code, payload={}, wakeup_signals=())

    def _rejected(self, error_code: Optional[str] = None) -> L0Result:
        return L0Result(status=ACK_STATUS_REJECTED, error_code=error_code, payload={}, wakeup_signals=())

    def _error(self, error_code: Optional[str] = None) -> L0Result:
        return L0Result(status=ACK_STATUS_ERROR, error_code=error_code, payload={}, wakeup_signals=())
