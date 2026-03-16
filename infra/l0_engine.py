from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Literal, Optional

import uuid6
from psycopg import Error as PsycopgError

from core.config import MAX_RECURSION_DEPTH
from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.message_source import MessageSource
from core.subject import parse_subject
from core.trace import TRACEPARENT_HEADER, TRACESTATE_HEADER
from core.utils import safe_str
from infra.agent_routing import resolve_agent_target
from infra.l0.errors import (
    ACK_STATUS_ACCEPTED,
    ACK_STATUS_BUSY,
    ACK_STATUS_ERROR,
    ACK_STATUS_REJECTED,
    ERROR_AGENT_BUSY,
    ERROR_INVALID_SOURCE_ACTOR,
    ERROR_INTERNAL_ERROR,
    ERROR_PROTOCOL_VIOLATION,
    ERROR_RECURSION_DEPTH_EXCEEDED,
    ERROR_STORAGE_ERROR,
)
from infra.nats_client import NATSClient
from infra.primitives import join_request, join_response, report as report_primitive
from infra.primitives.execution import enqueue as enqueue_primitive
from infra.stores import ExecutionStore, ResourceStore, StateStore
from infra.stores.context_hydration import build_replay_context

logger = logging.getLogger("L0Engine")

_SYSTEM_ACTOR_PREFIXES = ("sys.", "tool.")
_REPORT_AUTHOR_ALIASES = {
    "sys.agent_worker.watchdog": {"worker.watchdog"},
}


@dataclass(frozen=True, slots=True)
class WakeupSignal:
    target_ctx: CGContext
    inbox_id: Optional[str] = None
    reason: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True, slots=True)
class CommandSignal:
    subject: str
    payload: Dict[str, Any]
    headers: Dict[str, str]


@dataclass(frozen=True, slots=True)
class L0Result:
    status: Literal["accepted", "busy", "rejected", "error"]
    error_code: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    wakeup_signals: tuple[WakeupSignal, ...] = field(default_factory=tuple)
    command_signals: tuple[CommandSignal, ...] = field(default_factory=tuple)

    @property
    def ack_status(self) -> str:
        return str(self.status)

    @property
    def ack_error_code(self) -> Optional[str]:
        return self.error_code

    @property
    def ack_payload(self) -> Dict[str, Any]:
        return dict(self.payload or {})


class DepthPolicy(str, Enum):
    DESCEND = "descend"
    PEER = "peer"
    ROOT = "root"


@dataclass(frozen=True, slots=True)
class TurnRef:
    project_id: str
    agent_id: str
    agent_turn_id: str
    expected_turn_epoch: Optional[int] = None


@dataclass(frozen=True, slots=True)
class SpawnIntent:
    target_agent_id: str
    message_type: Literal["turn", "ui_action"]
    payload: Dict[str, Any]
    correlation_id: Optional[str] = None
    depth_policy: DepthPolicy = DepthPolicy.DESCEND
    lineage_ctx: Optional[CGContext] = None


@dataclass(frozen=True, slots=True)
class AddressIntent:
    target: TurnRef
    message_type: str
    payload: Dict[str, Any]
    correlation_id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class ReportIntent:
    target: TurnRef
    message_type: str
    payload: Dict[str, Any]
    correlation_id: str


@dataclass(frozen=True, slots=True)
class JoinIntent:
    target: TurnRef
    phase: Literal["request", "response"]
    correlation_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommandIntent:
    subject: str
    payload: Dict[str, Any]
    correlation_id: str
    primitive: Literal["tool_call"] = "tool_call"
    metadata: Dict[str, Any] = field(default_factory=dict)


class L0Engine:
    """L0 transactional engine: DB is source of truth, NATS is wakeup doorbell."""

    def __init__(
        self,
        *,
        nats: NATSClient,
        execution_store: ExecutionStore,
        resource_store: ResourceStore,
        state_store: Optional[StateStore] = None,
        cardbox: Any = None,
    ) -> None:
        self.nats = nats
        self.execution_store = execution_store
        self.resource_store = resource_store
        self.state_store = state_store
        self.cardbox = cardbox

    def _build_wakeup_ctx_from_result(
        self,
        *,
        base_ctx: CGContext,
        channel_id: Optional[str] = None,
        agent_turn_id: Optional[str] = None,
        turn_epoch: Optional[int] = None,
        trace_id: Optional[str] = None,
        traceparent: Optional[str] = None,
        tracestate: Optional[str] = None,
        default_depth: Optional[int] = None,
        base_headers: Optional[Dict[str, str]] = None,
    ) -> CGContext:
        next_ctx = base_ctx.evolve(
            channel_id=channel_id or base_ctx.channel_id,
            agent_turn_id=agent_turn_id or base_ctx.agent_turn_id,
            turn_epoch=base_ctx.turn_epoch if turn_epoch is None else int(turn_epoch),
            trace_id=trace_id or base_ctx.trace_id,
        )
        resolved_depth = next_ctx.recursion_depth if default_depth is None else int(default_depth)
        return next_ctx.with_trace_transport(
            base_headers=base_headers if base_headers is not None else next_ctx.headers,
            traceparent=traceparent,
            tracestate=tracestate,
            trace_id=next_ctx.trace_id,
            default_depth=resolved_depth,
        )

    @staticmethod
    def _is_report_message_type(message_type: str) -> bool:
        return str(message_type or "").strip() in ("tool_result", "timeout")

    @staticmethod
    def _is_system_actor(agent_id: str) -> bool:
        value = str(agent_id or "").strip()
        return bool(value) and value.startswith(_SYSTEM_ACTOR_PREFIXES)

    async def _validate_source_ctx(
        self,
        *,
        source_ctx: CGContext,
        target_project_id: str,
        conn: Any,
    ) -> Optional[L0Result]:
        source_agent_id = safe_str(source_ctx.agent_id)
        if not source_agent_id:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        if source_ctx.project_id != target_project_id:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        if self._is_system_actor(source_agent_id):
            return None
        row = await self.resource_store.fetch_roster(source_ctx, conn=conn)
        if row:
            return None
        return self._rejected(ERROR_INVALID_SOURCE_ACTOR)

    async def _validate_report_payload_source(
        self,
        *,
        source_ctx: CGContext,
        payload: Dict[str, Any],
        correlation_id: str,
        target_project_id: str,
        conn: Any,
    ) -> Optional[L0Result]:
        if self.cardbox is None:
            return None
        tool_result_card_id = safe_str(payload.get("tool_result_card_id"))
        if not tool_result_card_id:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        cards = await self.cardbox.get_cards(
            [tool_result_card_id],
            project_id=target_project_id,
            conn=conn,
        )
        card = cards[0] if cards else None
        if not card or getattr(card, "type", None) != "tool.result":
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        actual_tool_call_id = safe_str(getattr(card, "tool_call_id", None))
        if not actual_tool_call_id:
            meta = getattr(card, "metadata", None)
            if isinstance(meta, dict):
                actual_tool_call_id = safe_str(meta.get("tool_call_id"))
        if actual_tool_call_id != correlation_id:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        source_agent_id = safe_str(source_ctx.agent_id)
        expected_authors = {source_agent_id, *(_REPORT_AUTHOR_ALIASES.get(source_agent_id) or set())}
        author_id = safe_str(getattr(card, "author_id", None))
        if author_id not in expected_authors:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        meta = getattr(card, "metadata", None)
        function_name = safe_str(meta.get("function_name")) if isinstance(meta, dict) else ""
        if not function_name:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        return None

    def _resolve_depth_from_policy(
        self,
        *,
        source_ctx: CGContext,
        depth_policy: DepthPolicy,
    ) -> int:
        source_depth = int(source_ctx.recursion_depth)
        if depth_policy == DepthPolicy.DESCEND:
            return source_depth + 1
        if depth_policy == DepthPolicy.PEER:
            return source_depth
        if depth_policy == DepthPolicy.ROOT:
            # Caller-declared root reset: no extra authorization semantics at L0.
            return 0
        raise ProtocolViolationError("invalid depth_policy")

    def _build_spawn_target_ctx(
        self,
        *,
        source_ctx: CGContext,
        intent: SpawnIntent,
    ) -> CGContext:
        lineage_ctx = intent.lineage_ctx or source_ctx
        target_agent_id = safe_str(intent.target_agent_id)
        if not target_agent_id:
            raise ProtocolViolationError("missing target_agent_id")
        try:
            if isinstance(intent.depth_policy, DepthPolicy):
                depth_policy = intent.depth_policy
            else:
                depth_policy = DepthPolicy(str(intent.depth_policy or DepthPolicy.DESCEND.value))
        except ValueError as exc:
            raise ProtocolViolationError("invalid depth_policy") from exc
        resolved_depth = self._resolve_depth_from_policy(
            source_ctx=lineage_ctx,
            depth_policy=depth_policy,
        )
        if resolved_depth >= MAX_RECURSION_DEPTH:
            raise ProtocolViolationError(ERROR_RECURSION_DEPTH_EXCEEDED)
        parent_agent_id: Optional[str] = lineage_ctx.agent_id
        parent_agent_turn_id: Optional[str] = lineage_ctx.agent_turn_id or None
        parent_step_id: Optional[str] = lineage_ctx.step_id
        if depth_policy == DepthPolicy.ROOT:
            parent_agent_id = None
            parent_agent_turn_id = None
            parent_step_id = None
        target_ctx = lineage_ctx.evolve(
            agent_id=target_agent_id,
            agent_turn_id="",
            turn_epoch=0,
            step_id=None,
            tool_call_id=None,
            parent_agent_id=parent_agent_id,
            parent_agent_turn_id=parent_agent_turn_id,
            parent_step_id=parent_step_id,
            recursion_depth=int(resolved_depth),
        ).with_trace_transport(
            base_headers=lineage_ctx.headers,
            trace_id=lineage_ctx.trace_id,
            default_depth=int(resolved_depth),
        )
        return target_ctx

    async def _resolve_turn_ref_target_ctx(
        self,
        *,
        conn: Any,
        target: TurnRef,
        correlation_id: Optional[str] = None,
    ) -> Optional[CGContext]:
        project_id = safe_str(target.project_id)
        agent_id = safe_str(target.agent_id)
        agent_turn_id = safe_str(target.agent_turn_id)
        if not project_id or not agent_id or not agent_turn_id:
            return None
        expected_epoch_raw = target.expected_turn_epoch
        expected_epoch: Optional[int] = None
        if expected_epoch_raw is not None:
            try:
                expected_epoch = int(expected_epoch_raw)
            except Exception:
                return None

        resolved_correlation_id = safe_str(correlation_id) or None
        row = await self.execution_store.fetch_one(
            """
            SELECT project_id, agent_id, channel_id, agent_turn_id, turn_epoch, context_headers
            FROM state.agent_inbox
            WHERE project_id=%s
              AND agent_id=%s
              AND agent_turn_id=%s
              AND (%s::bigint IS NULL OR turn_epoch=%s::bigint)
              AND (
                  (%s::text IS NOT NULL AND correlation_id=%s::text AND message_type='command_anchor')
                  OR message_type IN ('turn', 'ui_action')
              )
            ORDER BY
              (CASE WHEN %s::text IS NOT NULL AND correlation_id=%s::text AND message_type='command_anchor' THEN 1 ELSE 0 END) DESC,
              created_at DESC
            LIMIT 1
            """,
            (
                project_id,
                agent_id,
                agent_turn_id,
                expected_epoch,
                expected_epoch,
                resolved_correlation_id,
                resolved_correlation_id,
                resolved_correlation_id,
                resolved_correlation_id,
            ),
            conn=conn,
        )
        if row:
            row_data = dict(row)
            try:
                target_ctx, _ = build_replay_context(raw_payload={}, db_row=row_data)
            except ProtocolViolationError:
                return None
            if expected_epoch is not None and int(expected_epoch) != int(target_ctx.turn_epoch):
                return None
            return target_ctx

        return None

    async def enqueue_intent(
        self,
        *,
        source_ctx: CGContext,
        intent: SpawnIntent | AddressIntent,
        conn: Any,
        wakeup: bool = True,
    ) -> L0Result:
        try:
            if isinstance(intent, SpawnIntent):
                if not isinstance(intent.payload, dict):
                    raise ProtocolViolationError("invalid payload")
                correlation_id = safe_str(intent.correlation_id)
                target_ctx = self._build_spawn_target_ctx(
                    source_ctx=source_ctx,
                    intent=intent,
                )
                return await self._enqueue_with_existing_conn(
                    conn,
                    source_ctx=source_ctx,
                    target_ctx=target_ctx,
                    message_type=str(intent.message_type),
                    payload=dict(intent.payload or {}),
                    correlation_id=correlation_id or None,
                    enqueue_mode="call",
                    wakeup=wakeup,
                )

            if isinstance(intent, AddressIntent):
                if not isinstance(intent.payload, dict):
                    raise ProtocolViolationError("invalid payload")
                correlation_id = safe_str(intent.correlation_id)
                target_ctx = await self._resolve_turn_ref_target_ctx(
                    conn=conn,
                    target=intent.target,
                    correlation_id=correlation_id or None,
                )
                if target_ctx is None:
                    return self._rejected(ERROR_PROTOCOL_VIOLATION)
                return await self._enqueue_with_existing_conn(
                    conn,
                    source_ctx=source_ctx,
                    target_ctx=target_ctx,
                    message_type=str(intent.message_type),
                    payload=dict(intent.payload or {}),
                    correlation_id=correlation_id or None,
                    enqueue_mode="call",
                    wakeup=wakeup,
                )

            raise ProtocolViolationError("invalid enqueue_intent type")
        except ProtocolViolationError as exc:
            if str(exc) == ERROR_RECURSION_DEPTH_EXCEEDED:
                return self._rejected(ERROR_RECURSION_DEPTH_EXCEEDED)
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 enqueue_intent failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def report_intent(
        self,
        *,
        source_ctx: CGContext,
        intent: ReportIntent,
        conn: Any,
        wakeup: bool = True,
    ) -> L0Result:
        try:
            if not isinstance(intent.payload, dict):
                raise ProtocolViolationError("invalid payload")
            if not self._is_report_message_type(str(intent.message_type)):
                raise ProtocolViolationError("invalid report_intent message_type")
            correlation_id = str(intent.correlation_id or "")
            target_ctx = await self._resolve_turn_ref_target_ctx(
                conn=conn,
                target=intent.target,
                correlation_id=correlation_id or None,
            )
            if target_ctx is None:
                return self._rejected(ERROR_PROTOCOL_VIOLATION)
            return await self._report_with_existing_conn(
                conn,
                source_ctx=source_ctx,
                target_ctx=target_ctx,
                message_type=str(intent.message_type),
                payload=dict(intent.payload or {}),
                correlation_id=correlation_id,
                wakeup=wakeup,
            )
        except ProtocolViolationError:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 report_intent failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def join_intent(
        self,
        *,
        source_ctx: CGContext,
        intent: JoinIntent,
        conn: Any,
    ) -> L0Result:
        try:
            correlation_id = str(intent.correlation_id or "")
            target_ctx = await self._resolve_turn_ref_target_ctx(
                conn=conn,
                target=intent.target,
                correlation_id=correlation_id or None,
            )
            if target_ctx is None:
                return self._rejected(ERROR_PROTOCOL_VIOLATION)
            return await self._join_with_existing_conn(
                conn,
                source_ctx=source_ctx,
                ctx=target_ctx,
                phase=intent.phase,
                correlation_id=correlation_id,
                metadata=dict(intent.metadata or {}),
            )
        except ProtocolViolationError:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 join_intent failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def command_intent(
        self,
        *,
        source_ctx: CGContext,
        intent: CommandIntent,
        conn: Any,
    ) -> L0Result:
        try:
            if not isinstance(intent.payload, dict):
                raise ProtocolViolationError("invalid payload")
            if not intent.correlation_id:
                raise ProtocolViolationError("missing correlation_id")
            if not intent.subject:
                raise ProtocolViolationError("missing subject")
            parts = parse_subject(intent.subject)
            if not parts:
                raise ProtocolViolationError("invalid subject")
            if parts.category != "cmd":
                raise ProtocolViolationError("command_intent requires cmd.* subject")
            if parts.component == "agent" and parts.suffix == "wakeup":
                raise ProtocolViolationError("cmd.agent.*.wakeup is forbidden in command_intent")
            if safe_str(parts.project_id) != safe_str(source_ctx.project_id):
                raise ProtocolViolationError("subject project mismatch")
            if safe_str(parts.channel_id) != safe_str(source_ctx.channel_id):
                raise ProtocolViolationError("subject channel mismatch")

            source_check = await self._validate_source_ctx(
                source_ctx=source_ctx,
                target_project_id=parts.project_id,
                conn=conn,
            )
            if source_check is not None:
                return source_check

            publish_ctx = source_ctx.evolve(channel_id=parts.channel_id).with_trace_transport(
                base_headers=source_ctx.headers,
                trace_id=source_ctx.trace_id,
                default_depth=int(source_ctx.recursion_depth),
            )
            source = MessageSource.from_ctx(source_ctx)
            inserted = await self.execution_store.insert_execution_edge_with_conn(
                conn,
                ctx=publish_ctx,
                source=source,
                edge_id=f"edge_{uuid6.uuid7().hex}",
                primitive=str(intent.primitive or "tool_call"),
                edge_phase="request",
                correlation_id=str(intent.correlation_id),
                enqueue_mode="call",
                metadata={
                    "subject": str(intent.subject),
                    "payload_keys": sorted(str(k) for k in intent.payload.keys()),
                    **dict(intent.metadata or {}),
                },
            )
            if not inserted:
                return self._accepted(
                    {
                        "subject": str(intent.subject),
                        "correlation_id": str(intent.correlation_id),
                        "idempotent_hit": True,
                    },
                    command_signals=[],
                )
            if safe_str(publish_ctx.agent_turn_id):
                await self._ensure_command_turn_anchor_with_conn(
                    conn,
                    ctx=publish_ctx,
                    correlation_id=str(intent.correlation_id),
                    subject=str(intent.subject),
                )
            signal = CommandSignal(
                subject=str(intent.subject),
                payload=dict(intent.payload or {}),
                headers=publish_ctx.to_nats_headers(include_topology=True),
            )
            return self._accepted(
                {
                    "subject": signal.subject,
                    "correlation_id": str(intent.correlation_id),
                    "idempotent_hit": False,
                },
                command_signals=[signal],
            )
        except ProtocolViolationError:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except PsycopgError:
            return self._error(ERROR_STORAGE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 command_intent failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def _ensure_command_turn_anchor_with_conn(
        self,
        conn: Any,
        *,
        ctx: CGContext,
        correlation_id: str,
        subject: str,
    ) -> None:
        """Persist a consumed inbox anchor so external callers can be resolved via TurnRef.

        External callers (API/UI/tools outside worker turn lifecycle) may dispatch commands
        without an existing inbox row. Later tool.result report_intent resolves target turn
        by (project_id, agent_id, agent_turn_id, turn_epoch) from inbox snapshots.
        This consumed anchor keeps that resolution path deterministic without creating runnable
        work.
        """
        if not safe_str(ctx.agent_turn_id):
            return
        inbox_id = f"inbox_cmd_anchor_{uuid6.uuid7().hex}"
        payload = {
            "kind": "command_anchor",
            "subject": str(subject),
            "correlation_id": str(correlation_id),
        }
        await self.execution_store.insert_inbox_with_conn(
            conn,
            ctx=ctx,
            inbox_id=inbox_id,
            message_type="command_anchor",
            enqueue_mode="call",
            correlation_id=str(correlation_id),
            payload=payload,
            status="consumed",
        )

    async def _enqueue_with_existing_conn(
        self,
        conn: Any,
        *,
        source_ctx: CGContext,
        target_ctx: CGContext,
        message_type: str,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
        enqueue_mode: str = "call",
        wakeup: bool = True,
    ) -> L0Result:
        try:
            project_id = target_ctx.project_id
            source_check = await self._validate_source_ctx(
                source_ctx=source_ctx,
                target_project_id=project_id,
                conn=conn,
            )
            if source_check is not None:
                return source_check
            source = MessageSource.from_ctx(source_ctx)
            if str(enqueue_mode or "call") != "call":
                raise ProtocolViolationError("unsupported enqueue_mode")
            if not target_ctx.agent_id:
                raise ProtocolViolationError("missing target_agent_id")
            if not message_type:
                raise ProtocolViolationError("missing message_type")
            if not isinstance(payload, dict):
                raise ProtocolViolationError("invalid payload")

            dispatch_ctx = target_ctx
            resolved_depth = int(dispatch_ctx.recursion_depth)
            if resolved_depth >= MAX_RECURSION_DEPTH:
                return self._rejected(ERROR_RECURSION_DEPTH_EXCEEDED)
            child_turn_id = safe_str(dispatch_ctx.agent_turn_id) or f"turn_{uuid6.uuid7().hex}"
            dispatch_ctx = dispatch_ctx.evolve(agent_turn_id=child_turn_id)
            merged_headers = dispatch_ctx.to_nats_headers()
            resolved_channel_id = dispatch_ctx.channel_id
            resolved_target_agent_id = dispatch_ctx.agent_id
            resolved_parent_step_id = dispatch_ctx.parent_step_id
            resolved_trace_id = dispatch_ctx.trace_id

            if message_type == "turn":
                if self.state_store is None:
                    raise RuntimeError("state_store is required for enqueue(turn)")

                submit_payload = dict(payload or {})
                submit_agent_turn_id = safe_str(dispatch_ctx.agent_turn_id)
                if not submit_agent_turn_id:
                    submit_agent_turn_id = f"turn_{uuid6.uuid7().hex}"
                write_ctx = dispatch_ctx.evolve(agent_turn_id=submit_agent_turn_id)
                result = await enqueue_primitive(
                    store=self.execution_store,
                    ctx=write_ctx,
                    source=source,
                    message_type=message_type,
                    payload=submit_payload,
                    correlation_id=correlation_id or submit_agent_turn_id,
                    enqueue_mode="call",
                    inbox_status="queued",
                    conn=conn,
                )

                dispatched: Optional[Dict[str, Any]] = None
                signals: list[WakeupSignal] = []
                if wakeup:
                    dispatched = await self._dispatch_next_if_idle_with_conn(
                        conn,
                        ctx=write_ctx,
                    )
                    if dispatched:
                        tp = safe_str(dispatched.get("traceparent"))
                        ts = safe_str(dispatched.get("tracestate"))
                        signals.append(
                            WakeupSignal(
                                target_ctx=self._build_wakeup_ctx_from_result(
                                    base_ctx=write_ctx,
                                    channel_id=safe_str(dispatched.get("channel_id")) or resolved_channel_id,
                                    agent_turn_id=safe_str(dispatched.get("agent_turn_id")) or submit_agent_turn_id,
                                    turn_epoch=int(dispatched.get("turn_epoch") or 0) or write_ctx.turn_epoch,
                                    trace_id=safe_str(dispatched.get("trace_id")) or resolved_trace_id,
                                    traceparent=tp or None,
                                    tracestate=ts or None,
                                    default_depth=int(dispatched.get("recursion_depth") or 0),
                                ),
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
                    "channel_id": write_ctx.channel_id,
                    "recursion_depth": int(write_ctx.recursion_depth),
                }
                if submit_epoch is not None:
                    accepted_payload["turn_epoch"] = int(submit_epoch)
                return self._accepted(accepted_payload, wakeup_signals=signals)

            if message_type == "ui_action":
                if self.state_store is None:
                    raise RuntimeError("state_store is required for enqueue(ui_action)")

                submit_payload = dict(payload or {})
                submit_agent_turn_id = safe_str(dispatch_ctx.agent_turn_id)
                if not submit_agent_turn_id:
                    submit_agent_turn_id = f"turn_{uuid6.uuid7().hex}"

                lease_ctx = dispatch_ctx.evolve(
                    agent_id=resolved_target_agent_id,
                    channel_id=resolved_channel_id,
                    agent_turn_id=submit_agent_turn_id,
                    parent_step_id=resolved_parent_step_id,
                    trace_id=resolved_trace_id,
                    recursion_depth=resolved_depth,
                )
                turn_epoch = await self.state_store.lease_agent_turn_with_conn(
                    conn,
                    ctx=lease_ctx,
                    profile_box_id=safe_str(submit_payload.get("profile_box_id")),
                    context_box_id=safe_str(submit_payload.get("context_box_id")),
                    output_box_id=safe_str(submit_payload.get("output_box_id")),
                )
                if turn_epoch is None:
                    return self._busy(ERROR_AGENT_BUSY)
                write_ctx = dispatch_ctx.evolve(
                    agent_turn_id=submit_agent_turn_id,
                    turn_epoch=int(turn_epoch),
                )
                result = await enqueue_primitive(
                    store=self.execution_store,
                    ctx=write_ctx,
                    source=source,
                    message_type=message_type,
                    payload=submit_payload,
                    correlation_id=correlation_id,
                    enqueue_mode="call",
                    conn=conn,
                )

                signals = []
                if wakeup:
                    signals.append(
                        WakeupSignal(
                            target_ctx=self._build_wakeup_ctx_from_result(
                                base_ctx=write_ctx,
                                trace_id=result.trace_id or resolved_trace_id,
                                traceparent=result.traceparent,
                                default_depth=resolved_depth,
                                base_headers=write_ctx.headers,
                            ),
                        )
                    )
                return self._accepted(
                    {
                        "inbox_id": result.inbox_id,
                        "correlation_id": result.correlation_id,
                        "trace_id": result.trace_id,
                        "agent_turn_id": submit_agent_turn_id,
                        "turn_epoch": int(turn_epoch),
                        "channel_id": write_ctx.channel_id,
                        "recursion_depth": int(write_ctx.recursion_depth),
                    },
                    wakeup_signals=signals,
                )

            write_ctx = dispatch_ctx
            result = await enqueue_primitive(
                store=self.execution_store,
                ctx=write_ctx,
                source=source,
                message_type=message_type,
                payload=payload,
                correlation_id=correlation_id,
                enqueue_mode="call",
                conn=conn,
            )

            signals = []
            if wakeup:
                signals.append(
                    WakeupSignal(
                        target_ctx=self._build_wakeup_ctx_from_result(
                            base_ctx=write_ctx,
                            trace_id=result.trace_id or resolved_trace_id,
                            traceparent=result.traceparent,
                            default_depth=resolved_depth,
                            base_headers=merged_headers,
                        ),
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

    async def _report_with_existing_conn(
        self,
        conn: Any,
        *,
        source_ctx: CGContext,
        target_ctx: CGContext,
        message_type: str,
        payload: Dict[str, Any],
        correlation_id: str,
        wakeup: bool = True,
    ) -> L0Result:
        try:
            resolved_correlation_id = safe_str(correlation_id)
            if not resolved_correlation_id:
                raise ProtocolViolationError("missing correlation_id")
            source_check = await self._validate_source_ctx(
                source_ctx=source_ctx,
                target_project_id=target_ctx.project_id,
                conn=conn,
            )
            if source_check is not None:
                return source_check
            payload_check = await self._validate_report_payload_source(
                source_ctx=source_ctx,
                payload=payload,
                correlation_id=resolved_correlation_id,
                target_project_id=target_ctx.project_id,
                conn=conn,
            )
            if payload_check is not None:
                return payload_check
            source = MessageSource.from_ctx(source_ctx)
            report_ctx = target_ctx.with_tool_call(resolved_correlation_id)
            resolved_depth = int(report_ctx.recursion_depth)
            if resolved_depth >= MAX_RECURSION_DEPTH:
                return self._rejected(ERROR_RECURSION_DEPTH_EXCEEDED)
            merged_headers = report_ctx.to_nats_headers()
            result = await report_primitive(
                store=self.execution_store,
                ctx=report_ctx,
                source=source,
                message_type=message_type,
                payload=payload,
                correlation_id=resolved_correlation_id,
                conn=conn,
            )
            signals: list[WakeupSignal] = []
            if wakeup:
                signals.append(
                    WakeupSignal(
                        target_ctx=self._build_wakeup_ctx_from_result(
                            base_ctx=report_ctx,
                            trace_id=result.trace_id or report_ctx.trace_id,
                            traceparent=result.traceparent,
                            default_depth=report_ctx.recursion_depth,
                            base_headers=merged_headers,
                        ),
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

    async def _join_with_existing_conn(
        self,
        conn: Any,
        *,
        source_ctx: CGContext,
        ctx: CGContext,
        phase: Literal["request", "response"],
        correlation_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> L0Result:
        try:
            source_check = await self._validate_source_ctx(
                source_ctx=source_ctx,
                target_project_id=ctx.project_id,
                conn=conn,
            )
            if source_check is not None:
                return source_check
            source = MessageSource.from_ctx(source_ctx)
            join_fn = join_request if phase == "request" else join_response
            result = await join_fn(
                store=self.execution_store,
                ctx=ctx,
                source=source,
                correlation_id=correlation_id,
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
        ctx: CGContext,
        reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> L0Result:
        try:
            target_agent_id = ctx.agent_id
            agent_turn_id = ctx.require_agent_turn_id
            project_id = ctx.project_id
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
        except ProtocolViolationError:
            return self._rejected(ERROR_PROTOCOL_VIOLATION)
        except PsycopgError:
            return self._error(ERROR_STORAGE_ERROR)
        except Exception as exc:  # noqa: BLE001
            logger.error("L0 cancel failed: %s", exc, exc_info=True)
            return self._error(ERROR_INTERNAL_ERROR)

    async def wakeup(
        self,
        *,
        target_ctx: CGContext,
        mode: Literal["default", "dispatch_next"] = "default",
        reason: Optional[str] = None,
        inbox_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
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
                            ctx=target_ctx,
                        )
                if dispatched:
                    tp = safe_str(dispatched.get("traceparent"))
                    ts = safe_str(dispatched.get("tracestate"))
                    dispatch_ctx = target_ctx.evolve(
                        channel_id=safe_str(dispatched.get("channel_id")) or target_ctx.channel_id,
                        agent_turn_id=safe_str(dispatched.get("agent_turn_id")) or target_ctx.agent_turn_id,
                        turn_epoch=int(dispatched.get("turn_epoch") or target_ctx.turn_epoch or 0),
                        trace_id=safe_str(dispatched.get("trace_id")) or target_ctx.trace_id,
                    ).with_trace_transport(
                        traceparent=tp or None,
                        tracestate=ts or None,
                        trace_id=safe_str(dispatched.get("trace_id")) or target_ctx.trace_id,
                        default_depth=int(dispatched.get("recursion_depth") or 0),
                    )
                    await self._publish_wakeup(
                        target_ctx=dispatch_ctx,
                        inbox_id=safe_str(dispatched.get("inbox_id")) or None,
                        reason=reason or "dispatch_next",
                        metadata=metadata,
                    )
                return self._accepted(dispatched or {})

            wakeup_ctx = target_ctx
            await self._publish_wakeup(
                target_ctx=wakeup_ctx,
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
            target_ctx=signal.target_ctx,
            inbox_id=signal.inbox_id,
            reason=signal.reason,
            metadata=signal.metadata,
        )

    async def publish_wakeup_signals(self, signals: list[WakeupSignal]) -> None:
        for signal in signals:
            await self.publish_wakeup_signal(signal)

    async def publish_command_signal(self, signal: CommandSignal) -> None:
        await self.nats.publish_event(signal.subject, signal.payload, headers=signal.headers)

    async def publish_command_signals(self, signals: list[CommandSignal]) -> None:
        for signal in signals:
            await self.publish_command_signal(signal)

    async def _archive_dispatch_error(
        self,
        conn: Any,
        *,
        project_id: str,
        inbox_id: str,
        patch: Dict[str, Any],
    ) -> None:
        await conn.execute(
            """
            UPDATE state.agent_inbox
            SET status='error',
                archived_at=COALESCE(archived_at, NOW()),
                payload = payload || %s::jsonb
            WHERE project_id=%s AND inbox_id=%s AND status='queued'
            """,
            (
                json.dumps(patch),
                project_id,
                inbox_id,
            ),
        )

    async def _fetch_next_runnable_inbox(
        self,
        conn: Any,
        *,
        project_id: str,
        agent_id: str,
    ) -> Optional[Dict[str, Any]]:
        res = await conn.execute(
            """
            SELECT inbox_id, project_id, agent_id, message_type, correlation_id, channel_id, agent_turn_id, turn_epoch,
                   context_headers, payload, created_at
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
        return dict(row) if row else None

    def _decode_dispatch_payload(
        self,
        *,
        inbox_row: Dict[str, Any],
    ) -> tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
        payload_any = inbox_row.get("payload")
        if isinstance(payload_any, dict):
            return dict(payload_any), None, None
        if isinstance(payload_any, str):
            try:
                parsed = json.loads(payload_any)
            except Exception as exc:  # noqa: BLE001
                return None, "invalid_payload_json", str(exc)
            if not isinstance(parsed, dict):
                return None, "invalid_payload_type", None
            return dict(parsed), None, None
        return None, "invalid_payload_type", None

    async def _promote_dispatched_inbox_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        inbox_id: str,
        agent_turn_id: str,
        turn_epoch: int,
    ) -> bool:
        updated = await conn.execute(
            """
            UPDATE state.agent_inbox
            SET status='pending',
                processed_at=NULL,
                archived_at=NULL,
                agent_turn_id=%s,
                turn_epoch=%s
            WHERE project_id=%s AND inbox_id=%s AND status='queued'
            RETURNING inbox_id
            """,
            (
                agent_turn_id,
                int(turn_epoch),
                project_id,
                inbox_id,
            ),
        )
        promoted = await updated.fetchone()
        return bool(promoted)

    async def _dispatch_next_if_idle_with_conn(
        self,
        conn: Any,
        *,
        ctx: CGContext,
    ) -> Optional[Dict[str, Any]]:
        project_id = ctx.project_id
        agent_id = ctx.agent_id
        if self.state_store is None:
            raise RuntimeError("state_store is required for dispatch_next")

        for _ in range(5):
            inbox_row = await self._fetch_next_runnable_inbox(
                conn,
                project_id=project_id,
                agent_id=agent_id,
            )
            if inbox_row is None:
                return None
            inbox_id = safe_str(inbox_row.get("inbox_id"))

            payload, payload_error_code, payload_error_detail = self._decode_dispatch_payload(
                inbox_row=inbox_row,
            )
            if payload_error_code:
                patch: Dict[str, Any] = {"dispatch_error": payload_error_code}
                if payload_error_code == "invalid_payload_json" and payload_error_detail:
                    patch["error"] = payload_error_detail
                await self._archive_dispatch_error(
                    conn,
                    project_id=project_id,
                    inbox_id=inbox_id,
                    patch=patch,
                )
                if payload_error_code == "invalid_payload_json":
                    logger.warning(
                        "L0 dispatch_next invalid payload json inbox=%s agent=%s err=%s",
                        inbox_row.get("inbox_id"),
                        agent_id,
                        payload_error_detail,
                    )
                else:
                    logger.warning(
                        "L0 dispatch_next invalid payload type inbox=%s agent=%s",
                        inbox_row.get("inbox_id"),
                        agent_id,
                    )
                continue
            if payload is None:
                continue

            try:
                replay_ctx, safe_payload = build_replay_context(
                    raw_payload=payload,
                    db_row=inbox_row,
                )
            except ProtocolViolationError as exc:
                await self._archive_dispatch_error(
                    conn,
                    project_id=project_id,
                    inbox_id=inbox_id,
                    patch={
                        "dispatch_error": "invalid_context",
                        "error": str(exc),
                    },
                )
                logger.warning(
                    "L0 dispatch_next invalid context inbox=%s agent=%s err=%s",
                    inbox_row.get("inbox_id"),
                    agent_id,
                    exc,
                )
                continue

            profile_box_id = safe_str(safe_payload.get("profile_box_id"))
            context_box_id = safe_str(safe_payload.get("context_box_id"))
            output_box_id = safe_str(safe_payload.get("output_box_id"))
            missing = [
                name
                for name, value in (
                    ("profile_box_id", profile_box_id),
                    ("context_box_id", context_box_id),
                    ("output_box_id", output_box_id),
                )
                if not value
            ]
            if missing:
                await self._archive_dispatch_error(
                    conn,
                    project_id=project_id,
                    inbox_id=inbox_id,
                    patch={"dispatch_error": "missing_required_fields", "missing": missing},
                )
                logger.warning(
                    "L0 dispatch_next skipped invalid queued turn inbox=%s agent=%s missing=%s",
                    inbox_row.get("inbox_id"),
                    agent_id,
                    missing,
                )
                continue

            turn_epoch = await self.state_store.lease_agent_turn_with_conn(
                conn,
                ctx=replay_ctx,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
            )
            if turn_epoch is None:
                return None

            promoted = await self._promote_dispatched_inbox_with_conn(
                conn,
                project_id=project_id,
                inbox_id=inbox_id,
                agent_turn_id=replay_ctx.agent_turn_id,
                turn_epoch=int(turn_epoch),
            )
            if not promoted:
                logger.warning(
                    "L0 dispatch_next promote failed inbox=%s agent=%s (stale?)",
                    inbox_row.get("inbox_id"),
                    agent_id,
                )
                return None

            return {
                "inbox_id": inbox_id,
                "agent_turn_id": replay_ctx.agent_turn_id,
                "turn_epoch": int(turn_epoch),
                "channel_id": replay_ctx.channel_id,
                "traceparent": safe_str(replay_ctx.headers.get(TRACEPARENT_HEADER)),
                "tracestate": safe_str(replay_ctx.headers.get(TRACESTATE_HEADER)),
                "trace_id": safe_str(replay_ctx.trace_id),
                "recursion_depth": int(replay_ctx.recursion_depth),
            }
        return None

    async def _publish_wakeup(
        self,
        *,
        target_ctx: CGContext,
        inbox_id: Optional[str] = None,
        reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            target = await resolve_agent_target(
                resource_store=self.resource_store,
                ctx=target_ctx,
            )
            if not target:
                logger.warning("L0 wakeup skipped: missing worker_target agent=%s", target_ctx.agent_id)
                return
            subject = target_ctx.subject("cmd", "agent", target, "wakeup")
            payload: Dict[str, Any] = {}
            if inbox_id:
                payload["inbox_id"] = inbox_id
            if reason:
                payload["reason"] = reason
            if metadata:
                payload["metadata"] = metadata
            await self.nats.publish_event(
                subject,
                payload,
                headers=target_ctx.to_nats_headers(include_topology=False),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "L0 wakeup publish failed agent=%s channel=%s: %s",
                target_ctx.agent_id,
                target_ctx.channel_id,
                exc,
            )

    def _accepted(
        self,
        payload: Optional[Dict[str, Any]] = None,
        *,
        wakeup_signals: Optional[list[WakeupSignal]] = None,
        command_signals: Optional[list[CommandSignal]] = None,
    ) -> L0Result:
        return L0Result(
            status=ACK_STATUS_ACCEPTED,
            payload=dict(payload or {}),
            wakeup_signals=tuple(wakeup_signals or ()),
            command_signals=tuple(command_signals or ()),
        )

    def _busy(self, error_code: Optional[str] = None) -> L0Result:
        return L0Result(
            status=ACK_STATUS_BUSY,
            error_code=error_code,
            payload={},
            wakeup_signals=(),
            command_signals=(),
        )

    def _rejected(self, error_code: Optional[str] = None) -> L0Result:
        return L0Result(
            status=ACK_STATUS_REJECTED,
            error_code=error_code,
            payload={},
            wakeup_signals=(),
            command_signals=(),
        )

    def _error(self, error_code: Optional[str] = None) -> L0Result:
        return L0Result(
            status=ACK_STATUS_ERROR,
            error_code=error_code,
            payload={},
            wakeup_signals=(),
            command_signals=(),
        )
