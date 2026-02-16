from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

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
    project_id: str,
    channel_id: Optional[str],
    action: str,
    target_agent_id: str,
    profile_box_id: str,
    worker_target: str,
    tags: List[str],
    display_name: Optional[str],
    owner_agent_id: Optional[str],
    metadata: Dict[str, Any],
    source_agent_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    traceparent: Optional[str] = None,
) -> ProvisionIdentityResult:
    if not project_id:
        raise ProtocolViolationError("missing project_id")
    if not target_agent_id:
        raise ProtocolViolationError("missing target_agent_id")
    if not action:
        raise ProtocolViolationError("missing action")
    if not profile_box_id:
        raise ProtocolViolationError("missing profile_box_id")
    if not worker_target:
        raise ProtocolViolationError("missing worker_target")

    trace_id = _resolve_trace_id(trace_id, traceparent)

    roster_meta = dict(metadata or {})
    profile_row = await resource_store.fetch_profile(project_id, profile_box_id)
    profile_name_value = safe_str((profile_row or {}).get("name"))
    if profile_name_value:
        roster_meta["profile_name"] = profile_name_value

    await resource_store.upsert_project_agent(
        project_id=project_id,
        agent_id=target_agent_id,
        profile_box_id=profile_box_id,
        worker_target=worker_target,
        tags=tags,
        display_name=display_name,
        owner_agent_id=owner_agent_id,
        metadata=roster_meta,
    )

    edge_id = await identity_store.insert_identity_edge(
        project_id=project_id,
        channel_id=channel_id,
        action=action,
        source_agent_id=source_agent_id,
        target_agent_id=target_agent_id,
        trace_id=trace_id,
        parent_step_id=parent_step_id,
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
