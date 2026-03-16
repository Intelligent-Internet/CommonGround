from __future__ import annotations

import asyncio
import logging
from typing import Optional

from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.utils import safe_str
from infra.l0_engine import L0Engine
from infra.nats_client import NATSClient
from infra.stores import ExecutionStore, ResourceStore, StateStore


async def publish_idle_wakeup(
    *,
    nats: NATSClient,
    resource_store: ResourceStore,
    state_store: StateStore,
    ctx: CGContext,
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
    if not isinstance(ctx, CGContext):
        raise TypeError("publish_idle_wakeup requires ctx: CGContext")
    # Best-effort: ask L0 to dispatch the next queued turn for this agent.
    # This advances the per-agent mailbox without requiring PMO/L1 polling.
    engine = l0_engine or L0Engine(
        nats=nats,
        execution_store=execution_store or ExecutionStore(pool=state_store.pool),
        resource_store=resource_store,
        state_store=state_store,
    )
    try:
        wakeup_ctx = ctx.with_trace_transport(
            base_headers=dict(ctx.headers or {}),
            trace_id=ctx.trace_id,
            default_depth=int(ctx.recursion_depth),
        )
    except ProtocolViolationError as exc:
        logger.warning(
            "%s invalid wakeup context; skip publish agent=%s err=%s",
            warn_prefix,
            ctx.agent_id,
            exc,
        )
        return False

    attempts = max(0, int(retry_count)) + 1
    delay = max(0.0, float(retry_delay))
    for attempt in range(1, attempts + 1):
        try:
            result = await engine.wakeup(
                target_ctx=wakeup_ctx,
                mode="dispatch_next",
                reason="agent_idle",
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
                    ctx.agent_id,
                    status or "<empty>",
                    error_code or "<none>",
                )
                return False
            if delay:
                await asyncio.sleep(delay)
            continue
        except Exception as exc:  # noqa: BLE001
            if attempt >= attempts:
                logger.warning("%s wakeup(dispatch_next) publish failed agent=%s: %s", warn_prefix, ctx.agent_id, exc)
                return False
            if delay:
                await asyncio.sleep(delay)
    return False
