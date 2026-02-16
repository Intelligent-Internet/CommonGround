from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List

from core.utils import safe_str
from core.trace import normalize_trace_id
from infra.stores import IdentityStore, ResourceStore, StateStore

async def _resolve_profile_from_name(
    *,
    resource_store: ResourceStore,
    project_id: str,
    profile_name: Optional[str],
    conn: Any = None,
) -> tuple[Optional[str], Optional[str], List[str], Optional[str]]:
    if not profile_name:
        return None, None, [], None
    profile = await resource_store.find_profile_by_name(project_id, profile_name, conn=conn)
    if not profile:
        return None, None, [], "profile_not_found"
    profile_box_id = safe_str(profile.get("profile_box_id"))
    if not profile_box_id:
        return None, None, [], "profile_box_missing"
    profile_worker_target = safe_str(profile.get("worker_target")) or None
    profile_tags = profile.get("tags")
    tags = profile_tags if isinstance(profile_tags, list) else []
    return profile_box_id, profile_worker_target, tags, None


def _normalize_tags(raw: Optional[List[Any]]) -> List[str]:
    if not raw:
        return []
    tags: List[str] = []
    for item in raw:
        value = safe_str(item)
        if value:
            tags.append(value)
    return tags


async def _provision_identity(
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
    conn: Any = None,
) -> None:
    if trace_id:
        trace_id = normalize_trace_id(trace_id)

    await resource_store.upsert_project_agent(
        project_id=project_id,
        agent_id=target_agent_id,
        profile_box_id=profile_box_id,
        worker_target=worker_target,
        tags=tags,
        display_name=display_name,
        owner_agent_id=owner_agent_id,
        metadata=metadata or {},
        conn=conn,
    )

    await identity_store.insert_identity_edge(
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
        conn=conn,
    )


def _should_update_roster(
    *,
    roster: Optional[Dict[str, Any]],
    existing_meta: Dict[str, Any],
    merged_meta: Dict[str, Any],
    profile_box_id_out: str,
    worker_target: str,
    tags: List[str],
    display_name_out: Optional[str],
    owner_agent_id_out: Optional[str],
    explicit_display_name: bool,
    explicit_owner: bool,
    explicit_profile: bool,
) -> bool:
    if not roster:
        return True
    existing_profile = safe_str(roster.get("profile_box_id"))
    existing_worker_target = safe_str(roster.get("worker_target"))
    existing_tags = roster.get("tags") if isinstance(roster.get("tags"), list) else []
    existing_tags = _normalize_tags(existing_tags)
    existing_display_name = safe_str(roster.get("display_name"))
    existing_owner = safe_str(roster.get("owner_agent_id"))
    existing_tag_set = set(existing_tags)
    tag_set = set(tags)
    return (
        merged_meta != existing_meta
        or (explicit_profile and profile_box_id_out != existing_profile)
        or (worker_target and worker_target != existing_worker_target)
        or (tag_set != existing_tag_set)
        or (explicit_display_name and display_name_out != existing_display_name)
        or (explicit_owner and owner_agent_id_out != existing_owner)
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class AgentLifecycleResult:
    roster_updated: bool
    error_code: Optional[str] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AgentSpec:
    metadata: Dict[str, Any] = field(default_factory=dict)
    display_name: Optional[str] = None
    owner_agent_id: Optional[str] = None
    active_channel_id: Optional[str] = None
    worker_target: Optional[str] = None
    tags: Optional[List[str]] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class CreateAgentSpec(AgentSpec):
    profile_box_id: Optional[str] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DerivedAgentSpec(AgentSpec):
    derived_from: str
    profile_name: Optional[str] = None
    profile_box_id: Optional[str] = None


async def ensure_agent_ready(
    *,
    resource_store: ResourceStore,
    state_store: StateStore,
    identity_store: Optional[IdentityStore] = None,
    project_id: str,
    agent_id: str,
    spec: AgentSpec,
    existing_roster: Optional[Dict[str, Any]] = None,
    source_agent_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    channel_id: Optional[str] = None,
    conn: Any = None,
) -> AgentLifecycleResult:
    roster = existing_roster or await resource_store.fetch_roster(project_id, agent_id, conn=conn)

    if isinstance(spec, CreateAgentSpec):
        return await _ensure_create_agent(
            resource_store=resource_store,
            state_store=state_store,
            identity_store=identity_store,
            project_id=project_id,
            agent_id=agent_id,
            spec=spec,
            roster=roster,
            source_agent_id=source_agent_id,
            parent_step_id=parent_step_id,
            trace_id=trace_id,
            channel_id=channel_id,
            conn=conn,
        )
    if isinstance(spec, DerivedAgentSpec):
        return await _ensure_derived_agent(
            resource_store=resource_store,
            state_store=state_store,
            identity_store=identity_store,
            project_id=project_id,
            agent_id=agent_id,
            spec=spec,
            roster=roster,
            source_agent_id=source_agent_id,
            parent_step_id=parent_step_id,
            trace_id=trace_id,
            channel_id=channel_id,
            conn=conn,
        )
    raise TypeError("spec must be CreateAgentSpec or DerivedAgentSpec")


async def _ensure_create_agent(
    *,
    resource_store: ResourceStore,
    state_store: StateStore,
    identity_store: Optional[IdentityStore],
    project_id: str,
    agent_id: str,
    spec: CreateAgentSpec,
    roster: Optional[Dict[str, Any]],
    source_agent_id: Optional[str],
    parent_step_id: Optional[str],
    trace_id: Optional[str],
    channel_id: Optional[str],
    conn: Any = None,
) -> AgentLifecycleResult:
    if not roster and not safe_str(spec.profile_box_id):
        return AgentLifecycleResult(roster_updated=False, error_code="missing_profile_name")

    profile_box_id_out = safe_str(spec.profile_box_id) or safe_str(roster.get("profile_box_id") if roster else None)
    if not profile_box_id_out:
        return AgentLifecycleResult(roster_updated=False, error_code="profile_box_missing")

    existing_meta: Dict[str, Any] = dict(roster.get("metadata") or {}) if roster else {}
    merged_meta = dict(existing_meta)
    merged_meta.update(spec.metadata or {})

    cached_profile = await resource_store.fetch_profile(project_id, profile_box_id_out, conn=conn)
    cached_profile_name = safe_str((cached_profile or {}).get("name"))
    if cached_profile_name:
        merged_meta["profile_name"] = cached_profile_name
    profile_worker_target = safe_str((cached_profile or {}).get("worker_target")) if isinstance(cached_profile, dict) else ""
    profile_tags_raw = (cached_profile or {}).get("tags") if isinstance(cached_profile, dict) else None
    profile_tags = _normalize_tags(profile_tags_raw if isinstance(profile_tags_raw, list) else [])

    worker_target_out = (
        safe_str(spec.worker_target)
        or profile_worker_target
        or safe_str(roster.get("worker_target") if roster else None)
    )
    if not worker_target_out:
        return AgentLifecycleResult(roster_updated=False, error_code="worker_target_missing")

    if spec.tags is not None:
        tags_out = _normalize_tags(spec.tags)
    elif cached_profile is not None:
        tags_out = list(profile_tags or [])
    elif roster and isinstance(roster.get("tags"), list):
        tags_out = _normalize_tags(roster.get("tags") or [])
    else:
        tags_out = []

    display_name_out = (
        spec.display_name
        if spec.display_name is not None
        else safe_str(roster.get("display_name") if roster else None)
    )
    owner_agent_id_out = (
        spec.owner_agent_id
        if spec.owner_agent_id is not None
        else safe_str(roster.get("owner_agent_id") if roster else None)
    )

    roster_updated = _should_update_roster(
        roster=roster,
        existing_meta=existing_meta,
        merged_meta=merged_meta,
        profile_box_id_out=profile_box_id_out,
        worker_target=worker_target_out,
        tags=tags_out,
        display_name_out=display_name_out,
        owner_agent_id_out=owner_agent_id_out,
        explicit_display_name=spec.display_name is not None,
        explicit_owner=spec.owner_agent_id is not None,
        explicit_profile=spec.profile_box_id is not None,
    )

    if roster_updated:
        action = "provision" if not roster else "update"
        if identity_store:
            await _provision_identity(
                identity_store=identity_store,
                resource_store=resource_store,
                project_id=project_id,
                channel_id=channel_id or spec.active_channel_id,
                action=action,
                target_agent_id=agent_id,
                profile_box_id=profile_box_id_out,
                worker_target=worker_target_out,
                tags=tags_out,
                display_name=display_name_out,
                owner_agent_id=owner_agent_id_out,
                metadata=merged_meta,
                source_agent_id=source_agent_id,
                parent_step_id=parent_step_id,
                trace_id=trace_id,
                conn=conn,
            )
        else:
            await resource_store.upsert_project_agent(
                project_id=project_id,
                agent_id=agent_id,
                profile_box_id=profile_box_id_out,
                worker_target=worker_target_out,
                tags=tags_out,
                display_name=display_name_out,
                owner_agent_id=owner_agent_id_out,
                metadata=merged_meta,
                conn=conn,
            )

    await state_store.init_if_absent(
        project_id=project_id,
        agent_id=agent_id,
        active_channel_id=spec.active_channel_id,
        profile_box_id=profile_box_id_out,
        conn=conn,
    )

    return AgentLifecycleResult(roster_updated=roster_updated)


async def _ensure_derived_agent(
    *,
    resource_store: ResourceStore,
    state_store: StateStore,
    identity_store: Optional[IdentityStore],
    project_id: str,
    agent_id: str,
    spec: DerivedAgentSpec,
    roster: Optional[Dict[str, Any]],
    source_agent_id: Optional[str],
    parent_step_id: Optional[str],
    trace_id: Optional[str],
    channel_id: Optional[str],
    conn: Any = None,
) -> AgentLifecycleResult:
    derived_profile_box_id, derived_profile_worker_target, derived_profile_tags, profile_err = await _resolve_profile_from_name(
        resource_store=resource_store,
        project_id=project_id,
        profile_name=spec.profile_name,
        conn=conn,
    )
    if profile_err:
        return AgentLifecycleResult(roster_updated=False, error_code=profile_err)

    if not roster and not (safe_str(spec.profile_box_id) or derived_profile_box_id):
        return AgentLifecycleResult(roster_updated=False, error_code="missing_derived_profile_name")

    existing_meta: Dict[str, Any] = dict(roster.get("metadata") or {}) if roster else {}

    if roster and derived_profile_box_id:
        existing_profile = safe_str(roster.get("profile_box_id"))
        if existing_profile and existing_profile != derived_profile_box_id:
            return AgentLifecycleResult(roster_updated=False, error_code="profile_mismatch")

    profile_box_id_out = safe_str(spec.profile_box_id) or derived_profile_box_id or safe_str(
        roster.get("profile_box_id") if roster else None
    )
    if not profile_box_id_out:
        return AgentLifecycleResult(roster_updated=False, error_code="profile_box_missing")

    existing_from = safe_str(existing_meta.get("derived_from"))
    if existing_from and existing_from != safe_str(spec.derived_from):
        return AgentLifecycleResult(roster_updated=False, error_code="derived_agent_conflict")

    merged_meta = dict(existing_meta)
    merged_meta.update(spec.metadata or {})
    merged_meta.setdefault("derived_from", safe_str(spec.derived_from))
    if not roster:
        merged_meta.pop("display_name", None)

    cached_profile = None
    if not derived_profile_worker_target or not derived_profile_tags:
        cached_profile = await resource_store.fetch_profile(project_id, profile_box_id_out, conn=conn)

    cached_profile_name = safe_str((cached_profile or {}).get("name"))
    resolved_profile_name = cached_profile_name or safe_str(spec.profile_name)
    if resolved_profile_name:
        merged_meta["profile_name"] = resolved_profile_name

    profile_worker_target = derived_profile_worker_target or safe_str(
        (cached_profile or {}).get("worker_target") if isinstance(cached_profile, dict) else ""
    )
    profile_tags_raw = derived_profile_tags
    if not profile_tags_raw and isinstance(cached_profile, dict):
        profile_tags_raw = cached_profile.get("tags")
    profile_tags = _normalize_tags(profile_tags_raw if isinstance(profile_tags_raw, list) else [])

    worker_target_out = (
        safe_str(spec.worker_target)
        or profile_worker_target
        or safe_str(roster.get("worker_target") if roster else None)
    )
    if not worker_target_out:
        return AgentLifecycleResult(roster_updated=False, error_code="worker_target_missing")

    if spec.tags is not None:
        tags_out = _normalize_tags(spec.tags)
    elif cached_profile is not None or derived_profile_tags:
        tags_out = list(profile_tags or [])
    elif roster and isinstance(roster.get("tags"), list):
        tags_out = _normalize_tags(roster.get("tags") or [])
    else:
        tags_out = []

    if roster:
        display_name_out = safe_str(roster.get("display_name"))
        owner_agent_id_out = safe_str(roster.get("owner_agent_id")) or safe_str(spec.derived_from)
    else:
        display_name_out = spec.display_name or spec.profile_name
        owner_agent_id_out = safe_str(spec.derived_from)

    roster_updated = _should_update_roster(
        roster=roster,
        existing_meta=existing_meta,
        merged_meta=merged_meta,
        profile_box_id_out=profile_box_id_out,
        worker_target=worker_target_out,
        tags=tags_out,
        display_name_out=display_name_out,
        owner_agent_id_out=owner_agent_id_out,
        explicit_display_name=spec.display_name is not None,
        explicit_owner=False,
        explicit_profile=spec.profile_box_id is not None,
    )

    if roster_updated:
        action = "provision" if not roster else "update"
        if identity_store:
            await _provision_identity(
                identity_store=identity_store,
                resource_store=resource_store,
                project_id=project_id,
                channel_id=channel_id or spec.active_channel_id,
                action=action,
                target_agent_id=agent_id,
                profile_box_id=profile_box_id_out,
                worker_target=worker_target_out,
                tags=tags_out,
                display_name=display_name_out,
                owner_agent_id=owner_agent_id_out,
                metadata=merged_meta,
                source_agent_id=source_agent_id,
                parent_step_id=parent_step_id,
                trace_id=trace_id,
                conn=conn,
            )
        else:
            await resource_store.upsert_project_agent(
                project_id=project_id,
                agent_id=agent_id,
                profile_box_id=profile_box_id_out,
                worker_target=worker_target_out,
                tags=tags_out,
                display_name=display_name_out,
                owner_agent_id=owner_agent_id_out,
                metadata=merged_meta,
                conn=conn,
            )

    if roster and not roster_updated:
        return AgentLifecycleResult(roster_updated=False)

    init_channel_id = spec.active_channel_id if not roster else None
    await state_store.init_if_absent(
        project_id=project_id,
        agent_id=agent_id,
        active_channel_id=init_channel_id,
        profile_box_id=profile_box_id_out,
        conn=conn,
    )

    return AgentLifecycleResult(roster_updated=roster_updated)
