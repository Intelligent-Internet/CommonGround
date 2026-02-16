from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

from core.headers import ensure_recursion_depth, require_recursion_depth
from core.trace import ensure_trace_headers


def normalize_headers(
    *,
    nats: Any,
    headers: Mapping[str, str] | None,
    trace_id: Optional[str] = None,
    default_depth: Optional[int] = None,
    require_depth: bool = False,
) -> Tuple[Dict[str, str], Optional[str], Optional[int]]:
    """Normalize inbound headers for worker-style handlers.

    Centralizes the common pattern:
    - merge headers from NATS wrapper
    - ensure trace headers (optionally pin trace_id)
    - optionally ensure/require CG-Recursion-Depth

    Returns: (headers, trace_id, depth)
    - depth is returned when require_depth=True or default_depth is provided; otherwise None.
    """
    merged = nats.merge_headers(dict(headers or {}))
    merged, _, resolved_trace_id = ensure_trace_headers(merged, trace_id=trace_id)

    depth: Optional[int] = None
    if default_depth is not None:
        merged = ensure_recursion_depth(merged, default_depth=int(default_depth))
        depth = require_recursion_depth(merged)
    elif require_depth:
        depth = require_recursion_depth(merged)

    return merged, resolved_trace_id, depth

