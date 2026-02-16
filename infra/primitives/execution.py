from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Dict, Mapping, Optional

import uuid6

from core.errors import ProtocolViolationError
from core.headers import require_recursion_depth
from core.trace import TRACEPARENT_HEADER, next_traceparent, normalize_trace_id
from infra.observability.otel import get_tracer, mark_span_error, traced
from infra.stores import ExecutionEdgeInsert, ExecutionStore, InboxInsert


logger = logging.getLogger("ExecutionPrimitives")
_TRACER = get_tracer("infra.primitives.execution")


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    inbox_id: str
    correlation_id: str
    traceparent: str
    trace_id: str
    recursion_depth: int


@dataclass(frozen=True, slots=True)
class ReportResult:
    inbox_id: str
    correlation_id: str
    traceparent: str
    trace_id: str
    recursion_depth: int


@dataclass(frozen=True, slots=True)
class JoinResult:
    edge_id: str
    correlation_id: str
    trace_id: Optional[str]
    recursion_depth: Optional[int]


def _resolve_traceparent(
    traceparent: Optional[str],
    headers: Mapping[str, str] | None,
) -> Optional[str]:
    if traceparent:
        return str(traceparent)
    if headers:
        return headers.get(TRACEPARENT_HEADER)
    return None


def _resolve_recursion_depth(
    recursion_depth: Optional[int],
    headers: Mapping[str, str] | None,
) -> int:
    if recursion_depth is None:
        return require_recursion_depth(headers)
    if recursion_depth < 0:
        raise ProtocolViolationError("invalid recursion_depth")
    return int(recursion_depth)


async def _inherit_recursion_depth(
    *,
    store: ExecutionStore,
    project_id: str,
    correlation_id: Optional[str],
    resolved_depth: Optional[int],
    conn: Any,
    require_match: bool = False,
) -> Optional[int]:
    if not correlation_id:
        return resolved_depth
    inherited = await store.get_request_recursion_depth_with_conn(
        conn,
        project_id=project_id,
        correlation_id=str(correlation_id),
    )
    if inherited is None:
        if require_match:
            raise ProtocolViolationError(
                "missing request recursion_depth for correlation_id",
                detail={
                    "project_id": project_id,
                    "correlation_id": str(correlation_id),
                    "require_match": require_match,
                },
            )
        return resolved_depth
    if resolved_depth is not None and int(resolved_depth) != int(inherited):
        raise ProtocolViolationError(
            "recursion_depth mismatch for correlation_id",
            detail={
                "project_id": project_id,
                "correlation_id": str(correlation_id),
                "resolved_depth": int(resolved_depth),
                "inherited_depth": int(inherited),
                "require_match": require_match,
            },
        )
    return inherited


@traced(_TRACER, "execution.enqueue", span_arg="_span")
async def enqueue(
    *,
    store: ExecutionStore,
    project_id: str,
    channel_id: Optional[str],
    target_agent_id: str,
    message_type: str,
    payload: Dict[str, Any],
    enqueue_mode: Optional[str] = "call",
    correlation_id: Optional[str] = None,
    recursion_depth: Optional[int] = None,
    headers: Mapping[str, str] | None = None,
    traceparent: Optional[str] = None,
    tracestate: Optional[str] = None,
    trace_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    source_agent_id: Optional[str] = None,
    source_agent_turn_id: Optional[str] = None,
    source_step_id: Optional[str] = None,
    target_agent_turn_id: Optional[str] = None,
    conn: Any,
    _span: Any = None,
) -> EnqueueResult:
    span = _span
    if span is not None:
        span.set_attribute("cg.project_id", str(project_id))
        span.set_attribute("cg.target_agent_id", str(target_agent_id))
        span.set_attribute("cg.message_type", str(message_type))
        if correlation_id:
            span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        inbox_record, edge_record, result = await build_enqueue_records(
            store=store,
            project_id=project_id,
            channel_id=channel_id,
            target_agent_id=target_agent_id,
            message_type=message_type,
            payload=payload,
            enqueue_mode=enqueue_mode,
            correlation_id=correlation_id,
            recursion_depth=recursion_depth,
            headers=headers,
            traceparent=traceparent,
            tracestate=tracestate,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            source_agent_id=source_agent_id,
            source_agent_turn_id=source_agent_turn_id,
            source_step_id=source_step_id,
            target_agent_turn_id=target_agent_turn_id,
            conn=conn,
        )

        await store.enqueue_with_conn(conn, inbox=inbox_record, edge=edge_record)
        if span is not None:
            span.set_attribute("cg.inbox_id", str(result.inbox_id))
        return result
    except Exception as exc:
        mark_span_error(span, exc)
        raise


async def build_enqueue_records(
    *,
    store: ExecutionStore,
    project_id: str,
    channel_id: Optional[str],
    target_agent_id: str,
    message_type: str,
    payload: Dict[str, Any],
    enqueue_mode: Optional[str] = "call",
    correlation_id: Optional[str] = None,
    recursion_depth: Optional[int] = None,
    headers: Mapping[str, str] | None = None,
    traceparent: Optional[str] = None,
    tracestate: Optional[str] = None,
    trace_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    source_agent_id: Optional[str] = None,
    source_agent_turn_id: Optional[str] = None,
    source_step_id: Optional[str] = None,
    target_agent_turn_id: Optional[str] = None,
    inbox_status: str = "pending",
    conn: Any,
) -> tuple[InboxInsert, ExecutionEdgeInsert, EnqueueResult]:
    if not project_id:
        raise ProtocolViolationError("missing project_id")
    if not target_agent_id:
        raise ProtocolViolationError("missing target_agent_id")
    if not message_type:
        raise ProtocolViolationError("missing message_type")

    resolved_mode = str(enqueue_mode or "call")
    if resolved_mode != "call":
        raise ProtocolViolationError("unsupported enqueue_mode")
    resolved_depth = _resolve_recursion_depth(recursion_depth, headers)
    resolved_depth = await _inherit_recursion_depth(
        store=store,
        project_id=project_id,
        correlation_id=correlation_id,
        resolved_depth=resolved_depth,
        conn=conn,
    )
    traceparent = _resolve_traceparent(traceparent, headers)
    traceparent, trace_id = next_traceparent(traceparent, trace_id)

    if correlation_id:
        inbox_id = str(correlation_id)
        correlation_id = inbox_id
    else:
        inbox_id = f"inbox_{uuid6.uuid7().hex}"
        correlation_id = inbox_id
    edge_id = f"edge_{uuid6.uuid7().hex}"
    inbox_record = InboxInsert(
        inbox_id=inbox_id,
        project_id=project_id,
        agent_id=target_agent_id,
        message_type=message_type,
        enqueue_mode="call",
        correlation_id=correlation_id,
        recursion_depth=resolved_depth,
        traceparent=traceparent,
        tracestate=tracestate,
        trace_id=trace_id,
        parent_step_id=parent_step_id,
        source_agent_id=source_agent_id,
        source_step_id=source_step_id,
        payload=payload,
        status=inbox_status,
    )
    edge_record = ExecutionEdgeInsert(
        edge_id=edge_id,
        project_id=project_id,
        channel_id=channel_id,
        primitive="enqueue",
        edge_phase="request",
        source_agent_id=source_agent_id,
        source_agent_turn_id=source_agent_turn_id,
        source_step_id=source_step_id,
        target_agent_id=target_agent_id,
        target_agent_turn_id=target_agent_turn_id,
        correlation_id=correlation_id,
        enqueue_mode="call",
        recursion_depth=resolved_depth,
        trace_id=trace_id,
        parent_step_id=parent_step_id,
        metadata={
            "message_type": message_type,
            "enqueue_mode": "call",
            "inbox_id": inbox_id,
            "recursion_depth": resolved_depth,
        },
    )
    result = EnqueueResult(
        inbox_id=inbox_id,
        correlation_id=correlation_id,
        traceparent=traceparent,
        trace_id=trace_id,
        recursion_depth=resolved_depth,
    )
    return inbox_record, edge_record, result


@traced(_TRACER, "execution.report", span_arg="_span")
async def report(
    *,
    store: ExecutionStore,
    project_id: str,
    channel_id: Optional[str],
    target_agent_id: str,
    message_type: str,
    payload: Dict[str, Any],
    correlation_id: str,
    recursion_depth: Optional[int] = None,
    headers: Mapping[str, str] | None = None,
    traceparent: Optional[str] = None,
    tracestate: Optional[str] = None,
    trace_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    source_agent_id: Optional[str] = None,
    source_agent_turn_id: Optional[str] = None,
    source_step_id: Optional[str] = None,
    target_agent_turn_id: Optional[str] = None,
    conn: Any,
    _span: Any = None,
) -> ReportResult:
    span = _span
    if span is not None:
        span.set_attribute("cg.project_id", str(project_id))
        span.set_attribute("cg.target_agent_id", str(target_agent_id))
        span.set_attribute("cg.message_type", str(message_type))
        span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        if not project_id:
            raise ProtocolViolationError("missing project_id")
        if not target_agent_id:
            raise ProtocolViolationError("missing target_agent_id")
        if not message_type:
            raise ProtocolViolationError("missing message_type")
        if not correlation_id:
            raise ProtocolViolationError("missing correlation_id")

        resolved_depth = _resolve_recursion_depth(recursion_depth, headers)
        require_match = message_type in ("tool_result", "timeout")
        resolved_depth = await _inherit_recursion_depth(
            store=store,
            project_id=project_id,
            correlation_id=correlation_id,
            resolved_depth=resolved_depth,
            conn=conn,
            require_match=require_match,
        )
        traceparent = _resolve_traceparent(traceparent, headers)
        traceparent, trace_id = next_traceparent(traceparent, trace_id)

        inbox_id = f"inbox_{uuid6.uuid7().hex}"
        if trace_id:
            trace_id = normalize_trace_id(trace_id)
        edge_id = f"edge_{uuid6.uuid7().hex}"
        inbox_record = InboxInsert(
            inbox_id=inbox_id,
            project_id=project_id,
            agent_id=target_agent_id,
            message_type=message_type,
            enqueue_mode=None,
            correlation_id=correlation_id,
            recursion_depth=resolved_depth,
            traceparent=traceparent,
            tracestate=tracestate,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            source_agent_id=source_agent_id,
            source_step_id=source_step_id,
            payload=payload,
            status="pending",
        )
        edge_record = ExecutionEdgeInsert(
            edge_id=edge_id,
            project_id=project_id,
            channel_id=channel_id,
            primitive="report",
            edge_phase="response",
            source_agent_id=source_agent_id,
            source_agent_turn_id=source_agent_turn_id,
            source_step_id=source_step_id,
            target_agent_id=target_agent_id,
            target_agent_turn_id=target_agent_turn_id,
            correlation_id=correlation_id,
            enqueue_mode=None,
            recursion_depth=resolved_depth,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            metadata={
                "message_type": message_type,
                "inbox_id": inbox_id,
                "recursion_depth": resolved_depth,
            },
        )
        await store.enqueue_with_conn(conn, inbox=inbox_record, edge=edge_record)

        if span is not None:
            span.set_attribute("cg.inbox_id", str(inbox_id))
        return ReportResult(
            inbox_id=inbox_id,
            correlation_id=correlation_id,
            traceparent=traceparent,
            trace_id=trace_id,
            recursion_depth=resolved_depth,
        )
    except Exception as exc:
        mark_span_error(span, exc)
        raise


@traced(_TRACER, "execution.join_request", span_arg="_span")
async def join_request(
    *,
    store: ExecutionStore,
    project_id: str,
    channel_id: Optional[str],
    source_agent_id: Optional[str],
    source_agent_turn_id: Optional[str],
    source_step_id: Optional[str],
    target_agent_id: Optional[str],
    target_agent_turn_id: Optional[str],
    correlation_id: str,
    recursion_depth: Optional[int] = None,
    trace_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    conn: Any,
    _span: Any = None,
) -> JoinResult:
    span = _span
    if span is not None:
        span.set_attribute("cg.project_id", str(project_id))
        span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        if not project_id:
            raise ProtocolViolationError("missing project_id")
        if not correlation_id:
            raise ProtocolViolationError("missing correlation_id")
        if recursion_depth is None:
            raise ProtocolViolationError("missing recursion_depth")
        if recursion_depth < 0:
            raise ProtocolViolationError("invalid recursion_depth")

        recursion_depth = await _inherit_recursion_depth(
            store=store,
            project_id=project_id,
            correlation_id=correlation_id,
            resolved_depth=recursion_depth,
            conn=conn,
            require_match=False,
        )
        if trace_id:
            trace_id = normalize_trace_id(trace_id)
        edge_id = f"edge_{uuid6.uuid7().hex}"
        await store.insert_execution_edge_with_conn(
            conn,
            edge_id=edge_id,
            project_id=project_id,
            channel_id=channel_id,
            primitive="join",
            edge_phase="request",
            source_agent_id=source_agent_id,
            source_agent_turn_id=source_agent_turn_id,
            source_step_id=source_step_id,
            target_agent_id=target_agent_id,
            target_agent_turn_id=target_agent_turn_id,
            correlation_id=correlation_id,
            enqueue_mode=None,
            recursion_depth=recursion_depth,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            metadata=metadata or {},
        )
        if span is not None:
            span.set_attribute("cg.edge_id", str(edge_id))
        return JoinResult(
            edge_id=edge_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            recursion_depth=recursion_depth,
        )
    except Exception as exc:
        mark_span_error(span, exc)
        raise


@traced(_TRACER, "execution.join_response", span_arg="_span")
async def join_response(
    *,
    store: ExecutionStore,
    project_id: str,
    channel_id: Optional[str],
    source_agent_id: Optional[str],
    source_agent_turn_id: Optional[str],
    source_step_id: Optional[str],
    target_agent_id: Optional[str],
    target_agent_turn_id: Optional[str],
    correlation_id: str,
    recursion_depth: Optional[int] = None,
    trace_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    conn: Any,
    _span: Any = None,
) -> JoinResult:
    span = _span
    if span is not None:
        span.set_attribute("cg.project_id", str(project_id))
        span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        if not project_id:
            raise ProtocolViolationError("missing project_id")
        if not correlation_id:
            raise ProtocolViolationError("missing correlation_id")

        recursion_depth = await _inherit_recursion_depth(
            store=store,
            project_id=project_id,
            correlation_id=correlation_id,
            resolved_depth=recursion_depth,
            conn=conn,
            require_match=True,
        )
        edge_id = f"edge_{uuid6.uuid7().hex}"
        await store.insert_execution_edge_with_conn(
            conn,
            edge_id=edge_id,
            project_id=project_id,
            channel_id=channel_id,
            primitive="join",
            edge_phase="response",
            source_agent_id=source_agent_id,
            source_agent_turn_id=source_agent_turn_id,
            source_step_id=source_step_id,
            target_agent_id=target_agent_id,
            target_agent_turn_id=target_agent_turn_id,
            correlation_id=correlation_id,
            enqueue_mode=None,
            recursion_depth=recursion_depth,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            metadata=metadata or {},
        )
        if span is not None:
            span.set_attribute("cg.edge_id", str(edge_id))
        return JoinResult(
            edge_id=edge_id,
            correlation_id=correlation_id,
            trace_id=trace_id,
            recursion_depth=recursion_depth,
        )
    except Exception as exc:
        mark_span_error(span, exc)
        raise
