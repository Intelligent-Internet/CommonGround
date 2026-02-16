from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple

from core.errors import ProtocolViolationError
from core.headers import ensure_recursion_depth
from core.trace import ensure_trace_headers
from core.utils import safe_str
from infra.l0_engine import L0Engine
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore


def build_wakeup_headers_from_inbox_row(
    *,
    row: Dict[str, Any],
    logger: logging.Logger,
) -> Tuple[Dict[str, str], Optional[str]]:
    wakeup_headers = {"CG-Recursion-Depth": str(row.get("recursion_depth") or 0)}
    traceparent = row.get("traceparent")
    if traceparent:
        wakeup_headers["traceparent"] = str(traceparent)
    tracestate = row.get("tracestate")
    if tracestate:
        wakeup_headers["tracestate"] = str(tracestate)
    try:
        wakeup_headers, _, trace_id = ensure_trace_headers(
            wakeup_headers,
            trace_id=safe_str(row.get("trace_id")),
        )
    except ProtocolViolationError as exc:
        logger.warning("Wakeup invalid trace headers inbox_id=%s: %s", row.get("inbox_id"), exc)
        wakeup_headers = ensure_recursion_depth({}, default_depth=int(row.get("recursion_depth") or 0))
        wakeup_headers, _, trace_id = ensure_trace_headers(wakeup_headers)
    return wakeup_headers, trace_id


async def publish_idle_wakeup(
    *,
    nats: NATSClient,
    resource_store: ResourceStore,
    state_store: StateStore,
    project_id: str,
    channel_id: str,
    agent_id: str,
    headers: Dict[str, str],
    logger: logging.Logger,
    retry_count: int = 3,
    retry_delay: float = 1.0,
    warn_prefix: str = "Idle wakeup",
    execution_store: Optional[ExecutionStore] = None,
    l0_engine: Optional[L0Engine] = None,
) -> bool:
    """Best-effort "I'm idle" signal to L0.

    Contract:
    - Worker should NOT publish `cmd.agent.*.wakeup` to itself.
    - L0 is the sole publisher of wakeups. When it successfully dispatches a queued
      mailbox turn, it will emit a wakeup as part of that dispatch.
    """
    # Best-effort: ask L0 to dispatch the next queued turn for this agent.
    # This advances the per-agent mailbox without requiring PMO/L1 polling.
    engine = l0_engine or L0Engine(
        nats=nats,
        execution_store=execution_store or ExecutionStore(pool=state_store.pool),
        resource_store=resource_store,
        state_store=state_store,
    )
    try:
        wakeup_headers = ensure_recursion_depth(headers, default_depth=0)
    except ProtocolViolationError:
        logger.warning(
            "%s invalid recursion-depth header; fallback to default depth=0 agent=%s",
            warn_prefix,
            agent_id,
        )
        wakeup_headers = ensure_recursion_depth({}, default_depth=0)

    attempts = max(0, int(retry_count)) + 1
    delay = max(0.0, float(retry_delay))
    for attempt in range(1, attempts + 1):
        try:
            result = await engine.wakeup(
                project_id=project_id,
                channel_id=channel_id,
                target_agent_id=agent_id,
                mode="dispatch_next",
                reason="agent_idle",
                headers=wakeup_headers,
            )
            status = safe_str(getattr(result, "status", None))
            if not status:
                status = safe_str(getattr(result, "ack_status", None))
            status = status.lower()
            if status == "accepted":
                return True
            error_code = safe_str(getattr(result, "error_code", None))
            if not error_code:
                error_code = safe_str(getattr(result, "ack_error_code", None))
            if attempt >= attempts:
                logger.warning(
                    "%s wakeup(dispatch_next) returned non-accepted status "
                    "agent=%s status=%s error_code=%s",
                    warn_prefix,
                    agent_id,
                    status or "<empty>",
                    error_code or "<none>",
                )
                return False
            if delay:
                await asyncio.sleep(delay)
            continue
        except Exception as exc:  # noqa: BLE001
            if attempt >= attempts:
                logger.warning("%s wakeup(dispatch_next) publish failed agent=%s: %s", warn_prefix, agent_id, exc)
                return False
            if delay:
                await asyncio.sleep(delay)
    return False
