from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.utils import safe_str
from infra.stores import ResourceStore, StateStore
from services.pmo.agent_lifecycle import CreateAgentSpec, ensure_agent_ready


async def resolve_profile_box_id(
    resource_store: ResourceStore,
    *,
    project_id: str,
    profile_box_id: Optional[str],
    profile_name: Optional[str],
) -> Optional[str]:
    if profile_box_id:
        return profile_box_id
    if not profile_name:
        return None
    profile = await resource_store.find_profile_by_name(project_id, profile_name)
    if not profile:
        return None
    return safe_str(profile.get("profile_box_id"))


async def ensure_agent(
    resource_store: ResourceStore,
    state_store: StateStore,
    *,
    project_id: str,
    channel_id: str,
    agent_id: str,
    ensure_agent: bool,
    profile_box_id: Optional[str],
    profile_name: Optional[str],
    display_name: Optional[str],
    owner_agent_id: Optional[str],
    worker_target: Optional[str],
    tags: Optional[List[str]],
    metadata_source: str,
) -> Tuple[Dict[str, Any], str]:
    roster = await resource_store.fetch_roster(project_id, agent_id)
    roster_profile_box_id = safe_str(roster.get("profile_box_id")) if roster else None
    if roster and roster_profile_box_id:
        return roster, roster_profile_box_id
    if not ensure_agent:
        raise RuntimeError(f"agent not found in roster: {agent_id}")

    resolved_profile_box_id = await resolve_profile_box_id(
        resource_store,
        project_id=project_id,
        profile_box_id=profile_box_id,
        profile_name=profile_name,
    )
    if not resolved_profile_box_id:
        if profile_name:
            profiles = await resource_store.list_profiles(project_id, limit=20, offset=0)
            names = [safe_str(p.get("name")) for p in profiles if safe_str(p.get("name"))]
            hint = ", ".join(names) if names else "no profiles found"
            raise RuntimeError(f"profile_name not found: {profile_name} (available: {hint})")
        raise RuntimeError("missing profile_box_id/profile_name for ensure-agent")

    lifecycle_result = await ensure_agent_ready(
        resource_store=resource_store,
        state_store=state_store,
        project_id=project_id,
        agent_id=agent_id,
        spec=CreateAgentSpec(
            profile_box_id=resolved_profile_box_id,
            metadata={"source": metadata_source},
            display_name=display_name or agent_id,
            owner_agent_id=owner_agent_id or "system",
            active_channel_id=channel_id,
            worker_target=worker_target,
            tags=tags,
        ),
    )
    if lifecycle_result.error_code:
        raise RuntimeError(f"ensure_agent_ready failed: {lifecycle_result.error_code}")

    roster = await resource_store.fetch_roster(project_id, agent_id)
    roster_profile_box_id = safe_str(roster.get("profile_box_id")) if roster else None
    if not roster or not roster_profile_box_id:
        raise RuntimeError("agent roster missing after ensure-agent")
    return roster, roster_profile_box_id
