from __future__ import annotations

import re
from typing import Dict, Mapping, Optional, Tuple

import uuid6

from core.errors import ProtocolViolationError


TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"

_TRACEPARENT_RE = re.compile(r"^[0-9a-fA-F]{2}-[0-9a-fA-F]{32}-[0-9a-fA-F]{16}-[0-9a-fA-F]{2}$")


def normalize_trace_id(trace_id: str) -> str:
    trace_id = str(trace_id).lower()
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", trace_id):
        trace_id = trace_id.replace("-", "")
    if not re.fullmatch(r"[0-9a-f]{32}", trace_id):
        raise ProtocolViolationError("invalid trace_id")
    if trace_id == "0" * 32:
        raise ProtocolViolationError("invalid trace_id")
    return trace_id


def _generate_span_id() -> str:
    while True:
        span_id = uuid6.uuid7().hex[:16].lower()
        if span_id != "0" * 16:
            return span_id


def parse_traceparent(traceparent: str) -> Tuple[str, str]:
    if not traceparent or not _TRACEPARENT_RE.fullmatch(str(traceparent)):
        raise ProtocolViolationError("invalid traceparent")
    traceparent = str(traceparent).lower()
    trace_id = traceparent.split("-")[1]
    trace_id = normalize_trace_id(trace_id)
    return traceparent, trace_id


def build_traceparent(trace_id: Optional[str] = None) -> Tuple[str, str]:
    if trace_id:
        trace_id = normalize_trace_id(trace_id)
    else:
        trace_id = uuid6.uuid7().hex
    span_id = _generate_span_id()
    traceparent = f"00-{trace_id}-{span_id}-01"
    return traceparent, trace_id


def ensure_traceparent(
    traceparent: Optional[str],
    trace_id: Optional[str] = None,
) -> Tuple[str, str]:
    if traceparent:
        traceparent, parsed_trace_id = parse_traceparent(traceparent)
        if trace_id:
            normalized = normalize_trace_id(trace_id)
            if normalized != parsed_trace_id:
                raise ProtocolViolationError("trace_id mismatch")
        return traceparent, parsed_trace_id
    return build_traceparent(trace_id)


def next_traceparent(
    traceparent: Optional[str],
    trace_id: Optional[str] = None,
) -> Tuple[str, str]:
    if traceparent:
        traceparent, parsed_trace_id = parse_traceparent(traceparent)
        if trace_id:
            normalized = normalize_trace_id(trace_id)
            if normalized != parsed_trace_id:
                raise ProtocolViolationError("trace_id mismatch")
        trace_id = parsed_trace_id
    traceparent, trace_id = build_traceparent(trace_id)
    return traceparent, trace_id


def trace_id_from_headers(headers: Mapping[str, str] | None) -> Optional[str]:
    if not headers:
        return None
    traceparent = headers.get(TRACEPARENT_HEADER)
    if not traceparent:
        return None
    _, trace_id = parse_traceparent(traceparent)
    return trace_id


def ensure_trace_headers(
    headers: Mapping[str, str] | None,
    *,
    traceparent: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> Tuple[Dict[str, str], str, str]:
    merged: Dict[str, str] = dict(headers or {})
    candidate = traceparent or merged.get(TRACEPARENT_HEADER)
    resolved_traceparent, resolved_trace_id = ensure_traceparent(candidate, trace_id)
    merged[TRACEPARENT_HEADER] = resolved_traceparent
    return merged, resolved_traceparent, resolved_trace_id
