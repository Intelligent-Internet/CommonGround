from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Tuple

from core.headers import RECURSION_DEPTH_HEADER, ensure_recursion_depth, require_recursion_depth
from core.trace import ensure_trace_headers

from infra.l0_engine import L0Engine

logger = logging.getLogger("ToolReports")


async def normalize_tool_headers(
    *,
    nats: Any,
    execution_store: Any,
    project_id: str,
    correlation_id: Optional[str],
    headers: Mapping[str, str] | None,
    trace_id: Optional[str] = None,
    default_depth: int = 0,
) -> Tuple[Dict[str, str], Optional[str], int]:
    """Normalize headers for tool-style handlers.

    Centralizes the common pattern:
    - best-effort merge via nats.merge_headers (when available)
    - ensure trace headers (optionally pin trace_id)
    - ensure CG-Recursion-Depth, backfilling from execution_store by correlation_id

    Returns: (safe_headers, resolved_trace_id, depth)
    """
    safe_headers: Dict[str, str] = dict(headers or {})
    if nats is not None and hasattr(nats, "merge_headers"):
        try:
            safe_headers = nats.merge_headers(safe_headers)
        except Exception:  # noqa: BLE001
            # Merge is best-effort; proceed with the raw header map.
            safe_headers = dict(safe_headers or {})

    safe_headers, _, resolved_trace_id = ensure_trace_headers(safe_headers, trace_id=trace_id)

    if RECURSION_DEPTH_HEADER not in safe_headers:
        request_depth = None
        if correlation_id:
            try:
                request_depth = await execution_store.get_request_recursion_depth(
                    project_id=project_id,
                    correlation_id=str(correlation_id),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Tool recursion_depth lookup failed correlation_id=%s: %s",
                    str(correlation_id),
                    exc,
                )
        if request_depth is not None:
            safe_headers[RECURSION_DEPTH_HEADER] = str(request_depth)
        else:
            logger.warning(
                "Tool missing recursion_depth; defaulting to %s (correlation_id=%s)",
                int(default_depth),
                str(correlation_id or "unknown"),
            )

    safe_headers = ensure_recursion_depth(safe_headers, default_depth=int(default_depth))
    depth = require_recursion_depth(safe_headers)
    return safe_headers, resolved_trace_id, int(depth)


async def publish_tool_result_report(
    *,
    nats: Any,
    execution_store: Any,
    resource_store: Any | None = None,
    state_store: Any | None = None,
    l0_engine: L0Engine | None = None,
    project_id: str,
    channel_id: str,
    payload: Dict[str, Any],
    headers: Mapping[str, str] | None,
    source_agent_id: str = "sys.pmo",
    default_depth: int = 0,
    target_agent_id: Optional[str] = None,
) -> None:
    """Publish a standard L0 report for a tool.result (Inbox + Wakeup fanout path)."""
    correlation_id = str(payload.get("tool_call_id") or "")
    safe_headers, resolved_trace_id, depth = await normalize_tool_headers(
        nats=nats,
        execution_store=execution_store,
        project_id=project_id,
        correlation_id=correlation_id or None,
        headers=headers,
        default_depth=int(default_depth),
    )
    target_agent_id = str(target_agent_id or payload.get("agent_id") or "")
    engine = l0_engine
    if engine is None and resource_store is not None:
        engine = L0Engine(
            nats=nats,
            execution_store=execution_store,
            resource_store=resource_store,
            state_store=state_store,
        )
    if engine is None:
        raise RuntimeError("publish_tool_result_report requires l0_engine or resource_store")
    wakeup_signals = []
    async with execution_store.pool.connection() as conn:
        async with conn.transaction():
            result = await engine.report(
                project_id=project_id,
                channel_id=channel_id,
                target_agent_id=target_agent_id,
                message_type="tool_result",
                payload=payload,
                correlation_id=correlation_id,
                recursion_depth=int(depth),
                trace_id=resolved_trace_id,
                parent_step_id=str(payload.get("step_id") or ""),
                source_agent_id=source_agent_id,
                headers=safe_headers,
                wakeup=True,
                conn=conn,
            )
            wakeup_signals = list(result.wakeup_signals or ())
    if wakeup_signals:
        await engine.publish_wakeup_signals(wakeup_signals)
