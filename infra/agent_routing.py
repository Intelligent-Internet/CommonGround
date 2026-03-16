"""Routing helpers for agent command subjects based on roster fields."""

from __future__ import annotations

from typing import Any, Optional

from core.cg_context import CGContext
from infra.stores import ResourceStore

def _target_from_row(row: Any) -> Optional[str]:
    if not isinstance(row, dict):
        return None
    worker_target = row.get("worker_target")
    if isinstance(worker_target, str) and worker_target.strip():
        return worker_target.strip()
    return None


async def resolve_agent_target(
    *,
    resource_store: ResourceStore,
    ctx: CGContext,
    default_target: Optional[str] = None,
    conn: Any = None,
) -> Optional[str]:
    """Resolve NATS subject target for cmd.agent.* based on roster metadata."""
    try:
        row = await resource_store.fetch_roster(
            ctx,
            conn=conn,
        )
    except Exception:
        row = None

    target = _target_from_row(row)
    return target or default_target
