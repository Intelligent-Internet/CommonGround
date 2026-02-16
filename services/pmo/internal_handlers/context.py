from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.headers import merge_recursion_depth, next_recursion_depth
from infra.worker_helpers import normalize_headers

from .base import InternalHandlerDeps


@dataclass
class InternalHandlerContext:
    """Per-internal-handler execution context (kept local; safe for concurrency)."""

    deps: InternalHandlerDeps
    meta: Any
    cmd: Any
    headers: Dict[str, str]
    trace_id: Optional[str] = None
    child_depth: Optional[int] = None

    def normalize_headers(self, *, trace_id: Optional[str] = None, require_depth: bool = False) -> None:
        headers, resolved_trace_id, depth = normalize_headers(
            nats=self.deps.nats,
            headers=self.headers,
            trace_id=trace_id,
            require_depth=require_depth,
        )
        self.headers = headers
        self.trace_id = resolved_trace_id
        _ = depth

    def bump_recursion_depth(self) -> int:
        """Advance recursion depth for child operations."""
        child_depth = next_recursion_depth(self.headers)
        self.headers = merge_recursion_depth(self.headers, depth=child_depth)
        self.child_depth = child_depth
        return child_depth

    def fork(self, *, cmd: Any, headers: Optional[Dict[str, str]] = None) -> "InternalHandlerContext":
        """Create a new context for an internal sub-call, without mutating the parent ctx.

        Used by handlers that orchestrate other internal handlers (e.g. launch_principal).
        """
        return InternalHandlerContext(
            deps=self.deps,
            meta=self.meta,
            cmd=cmd,
            headers=dict(headers or self.headers or {}),
            trace_id=self.trace_id,
        )
