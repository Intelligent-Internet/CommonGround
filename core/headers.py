from __future__ import annotations

from typing import Dict, Mapping

from core.errors import ProtocolViolationError


RECURSION_DEPTH_HEADER = "CG-Recursion-Depth"
# Optional overall. When `require_ack=true` on an L0 cmd, this header MUST be
# present and point to a Core NATS inbox subject (Request-Reply style).
L0_ACK_REPLY_HEADER = "CG-L0-Ack-Reply"


def require_recursion_depth(headers: Mapping[str, str] | None) -> int:
    """Return recursion depth from headers or raise ProtocolViolationError."""
    header_map = headers or {}
    raw = header_map.get(RECURSION_DEPTH_HEADER)
    if raw is None or str(raw).strip() == "":
        raise ProtocolViolationError("missing CG-Recursion-Depth header")
    try:
        depth = int(raw)
    except Exception as exc:  # noqa: BLE001
        raise ProtocolViolationError("invalid CG-Recursion-Depth header") from exc
    if depth < 0:
        raise ProtocolViolationError("invalid CG-Recursion-Depth header")
    return depth


def merge_recursion_depth(
    headers: Mapping[str, str] | None,
    *,
    depth: int,
) -> Dict[str, str]:
    """Return a new header dict with CG-Recursion-Depth set."""
    if depth < 0:
        raise ProtocolViolationError("invalid CG-Recursion-Depth header")
    merged: Dict[str, str] = dict(headers or {})
    merged[RECURSION_DEPTH_HEADER] = str(depth)
    return merged


def ensure_recursion_depth(
    headers: Mapping[str, str] | None,
    *,
    default_depth: int,
) -> Dict[str, str]:
    """Ensure CG-Recursion-Depth exists; validate existing value if present."""
    header_map = dict(headers or {})
    if RECURSION_DEPTH_HEADER in header_map:
        require_recursion_depth(header_map)
        return header_map
    return merge_recursion_depth(header_map, depth=default_depth)


def next_recursion_depth(headers: Mapping[str, str] | None) -> int:
    """Compute child recursion depth (parent + 1)."""
    return require_recursion_depth(headers) + 1
