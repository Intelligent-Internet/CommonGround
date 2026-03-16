from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Dict, Optional

import uuid6

from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.message_source import MessageSource
from core.trace import TRACEPARENT_HEADER, next_traceparent, normalize_trace_id
from infra.observability.otel import get_tracer, mark_span_error, traced
from infra.stores import ExecutionStore


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


def _build_trace_write_ctx(
    *,
    ctx: CGContext,
    headers: Dict[str, str],
    traceparent: str,
    tracestate: Optional[str],
    trace_id: Optional[str],
    recursion_depth: int,
) -> CGContext:
    return ctx.evolve(
        recursion_depth=int(recursion_depth),
        trace_id=trace_id,
    ).with_trace_transport(
        base_headers=headers,
        traceparent=traceparent,
        tracestate=str(tracestate) if tracestate else None,
        trace_id=trace_id,
        default_depth=int(recursion_depth),
    )


@traced(_TRACER, "execution.enqueue", span_arg="_span")
async def enqueue(
    *,
    store: ExecutionStore,
    ctx: CGContext,
    source: MessageSource,
    message_type: str,
    payload: Dict[str, Any],
    correlation_id: Optional[str] = None,
    enqueue_mode: Optional[str] = "call",
    inbox_status: str = "pending",
    conn: Any,
    _span: Any = None,
) -> EnqueueResult:
    span = _span
    if span is not None:
        span.set_attribute("cg.message_type", message_type)
        if correlation_id:
            span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        if not message_type:
            raise ProtocolViolationError("missing message_type")

        resolved_mode = str(enqueue_mode or "call")
        if resolved_mode != "call":
            raise ProtocolViolationError("unsupported enqueue_mode")

        headers = ctx.to_nats_headers()
        tracestate = headers.get("tracestate")
        resolved_depth = ctx.recursion_depth

        traceparent = headers.get(TRACEPARENT_HEADER)
        traceparent, trace_id = next_traceparent(traceparent, ctx.trace_id)

        if correlation_id:
            inbox_id = str(correlation_id)
            correlation_id = inbox_id
        else:
            inbox_id = f"inbox_{uuid6.uuid7().hex}"
            correlation_id = inbox_id
        edge_id = f"edge_{uuid6.uuid7().hex}"

        write_ctx = _build_trace_write_ctx(
            ctx=ctx,
            headers=headers,
            traceparent=traceparent,
            tracestate=tracestate,
            trace_id=trace_id,
            recursion_depth=int(resolved_depth),
        )
        inserted = await store.enqueue_with_conn(
            conn,
            ctx=write_ctx,
            source=source,
            inbox_id=inbox_id,
            message_type=message_type,
            enqueue_mode="call",
            correlation_id=correlation_id,
            payload=payload,
            edge_id=edge_id,
            primitive="enqueue",
            edge_phase="request",
            metadata={
                "message_type": message_type,
                "enqueue_mode": "call",
                "inbox_id": inbox_id,
                "recursion_depth": resolved_depth,
                "cg_ctx": write_ctx.to_sys_dict(),
            },
            inbox_status=inbox_status,
        )
        if not inserted:
            raise ProtocolViolationError("duplicate enqueue correlation")
        result = EnqueueResult(
            inbox_id=inbox_id,
            correlation_id=correlation_id,
            traceparent=traceparent,
            trace_id=trace_id,
            recursion_depth=int(resolved_depth),
        )
        if span is not None:
            span.set_attribute("cg.inbox_id", str(result.inbox_id))
        return result
    except Exception as exc:
        mark_span_error(span, exc)
        raise


@traced(_TRACER, "execution.report", span_arg="_span")
async def report(
    *,
    store: ExecutionStore,
    ctx: CGContext,
    source: MessageSource,
    message_type: str,
    payload: Dict[str, Any],
    correlation_id: str,
    conn: Any,
    _span: Any = None,
) -> ReportResult:
    span = _span

    if span is not None:
        span.set_attribute("cg.message_type", message_type)
        span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        if not message_type:
            raise ProtocolViolationError("missing message_type")
        if not correlation_id:
            raise ProtocolViolationError("missing correlation_id")

        resolved_depth = ctx.recursion_depth
        require_match = message_type in ("tool_result", "timeout")
        resolved_depth = await _inherit_recursion_depth(
            store=store,
            project_id=ctx.project_id,
            correlation_id=correlation_id,
            resolved_depth=resolved_depth,
            conn=conn,
            require_match=require_match,
        )

        headers = ctx.to_nats_headers()
        tracestate = headers.get("tracestate")
        traceparent = headers.get(TRACEPARENT_HEADER)
        traceparent, trace_id = next_traceparent(traceparent, ctx.trace_id)

        inbox_id = f"inbox_{uuid6.uuid7().hex}"
        if trace_id:
            trace_id = normalize_trace_id(trace_id)
        edge_id = f"edge_{uuid6.uuid7().hex}"
        write_ctx = _build_trace_write_ctx(
            ctx=ctx,
            headers=headers,
            traceparent=traceparent,
            tracestate=tracestate,
            trace_id=trace_id,
            recursion_depth=int(resolved_depth),
        )
        inserted = await store.enqueue_with_conn(
            conn,
            ctx=write_ctx,
            source=source,
            inbox_id=inbox_id,
            message_type=message_type,
            enqueue_mode=None,
            correlation_id=correlation_id,
            payload=payload,
            edge_id=edge_id,
            primitive="report",
            edge_phase="response",
            metadata={
                "message_type": message_type,
                "inbox_id": inbox_id,
                "recursion_depth": resolved_depth,
                "cg_ctx": write_ctx.to_sys_dict(),
            },
        )
        if not inserted:
            raise ProtocolViolationError("duplicate report correlation")

        if span is not None:
            span.set_attribute("cg.inbox_id", str(inbox_id))
        return ReportResult(
            inbox_id=inbox_id,
            correlation_id=correlation_id,
            traceparent=traceparent,
            trace_id=trace_id,
            recursion_depth=int(resolved_depth),
        )
    except Exception as exc:
        mark_span_error(span, exc)
        raise


@traced(_TRACER, "execution.join_request", span_arg="_span")
async def join_request(
    *,
    store: ExecutionStore,
    ctx: CGContext,
    source: MessageSource,
    correlation_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    conn: Any,
    _span: Any = None,
) -> JoinResult:
    span = _span

    if span is not None:
        span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        if not correlation_id:
            raise ProtocolViolationError("missing correlation_id")

        recursion_depth = ctx.recursion_depth
        trace_id = normalize_trace_id(ctx.trace_id) if ctx.trace_id else None
        edge_id = f"edge_{uuid6.uuid7().hex}"
        merged_metadata = dict(metadata or {})
        merged_metadata["cg_ctx"] = ctx.to_sys_dict()
        await store.insert_execution_edge_with_conn(
            conn,
            ctx=ctx,
            source=source,
            edge_id=edge_id,
            primitive="join",
            edge_phase="request",
            correlation_id=correlation_id,
            metadata=merged_metadata,
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
    ctx: CGContext,
    source: MessageSource,
    correlation_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    conn: Any,
    _span: Any = None,
) -> JoinResult:
    span = _span

    if span is not None:
        span.set_attribute("cg.correlation_id", str(correlation_id))
    try:
        if not correlation_id:
            raise ProtocolViolationError("missing correlation_id")

        recursion_depth = await _inherit_recursion_depth(
            store=store,
            project_id=ctx.project_id,
            correlation_id=correlation_id,
            resolved_depth=ctx.recursion_depth,
            conn=conn,
            require_match=True,
        )
        trace_id = normalize_trace_id(ctx.trace_id) if ctx.trace_id else None
        edge_id = f"edge_{uuid6.uuid7().hex}"
        merged_metadata = dict(metadata or {})
        merged_metadata["cg_ctx"] = ctx.to_sys_dict()
        await store.insert_execution_edge_with_conn(
            conn,
            ctx=ctx,
            source=source,
            edge_id=edge_id,
            primitive="join",
            edge_phase="response",
            correlation_id=correlation_id,
            metadata=merged_metadata,
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
