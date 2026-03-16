from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.utils import safe_str
from core.trace import normalize_trace_id, parse_traceparent
from infra.stores import IdentityStore, ResourceStore


@dataclass(frozen=True, slots=True)
class ProvisionIdentityResult:
    edge_id: str
    target_agent_id: str
    action: str
    trace_id: Optional[str]


def _resolve_trace_id(trace_id: Optional[str], traceparent: Optional[str]) -> Optional[str]:
    if traceparent:
        _, parsed = parse_traceparent(traceparent)
        if trace_id:
            normalized = normalize_trace_id(trace_id)
            if normalized != parsed:
                raise ProtocolViolationError("trace_id mismatch")
            return normalized
        return parsed
    if trace_id:
        return normalize_trace_id(trace_id)
    return None


async def provision_identity(
    *,
    identity_store: IdentityStore,
    resource_store: ResourceStore,
    target_ctx: CGContext,
    action: str,
    profile_box_id: str,
    worker_target: str,
    tags: List[str],
    display_name: Optional[str],
    owner_agent_id: Optional[str],
    metadata: Dict[str, Any],
    traceparent: Optional[str] = None,
) -> ProvisionIdentityResult:
    if not target_ctx.project_id:
        raise ProtocolViolationError("missing project_id")
    target_agent_id = target_ctx.agent_id
    if not target_agent_id:
        raise ProtocolViolationError("missing target_agent_id")
    if not action:
        raise ProtocolViolationError("missing action")
    if not profile_box_id:
        raise ProtocolViolationError("missing profile_box_id")
    if not worker_target:
        raise ProtocolViolationError("missing worker_target")

    trace_id = _resolve_trace_id(target_ctx.trace_id, traceparent)
    write_ctx = target_ctx.with_restored_state(trace_id=trace_id)

    roster_meta = dict(metadata or {})
    profile_row = await resource_store.fetch_profile(target_ctx.project_id, profile_box_id)
    profile_name_value = safe_str((profile_row or {}).get("name"))
    if profile_name_value:
        roster_meta["profile_name"] = profile_name_value

    await resource_store.upsert_project_agent(
        ctx=target_ctx,
        profile_box_id=profile_box_id,
        worker_target=worker_target,
        tags=tags,
        display_name=display_name,
        owner_agent_id=owner_agent_id,
        metadata=roster_meta,
    )

    edge_id = await identity_store.insert_identity_edge(
        ctx=write_ctx,
        action=action,
        metadata={
            "profile_box_id": profile_box_id,
            "worker_target": worker_target,
            "tags": tags,
            "display_name": display_name,
            "owner_agent_id": owner_agent_id,
            **(metadata or {}),
        },
    )

    return ProvisionIdentityResult(
        edge_id=edge_id,
        target_agent_id=target_agent_id,
        action=action,
        trace_id=trace_id,
    )
