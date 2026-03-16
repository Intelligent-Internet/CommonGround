from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List

from core.cg_context import CGContext
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


def _validate_existing_agent_mutation(
    *,
    roster: Dict[str, Any],
    requested_profile_box_id: Optional[str] = None,
    requested_owner_agent_id: Optional[str] = None,
    requested_worker_target: Optional[str] = None,
    requested_tags: Optional[List[Any]] = None,
) -> Optional[str]:
    existing_profile_box_id = safe_str(roster.get("profile_box_id"))
    if requested_profile_box_id is not None and safe_str(requested_profile_box_id) != existing_profile_box_id:
        return "existing_agent_profile_mutation_forbidden"

    existing_owner_agent_id = safe_str(roster.get("owner_agent_id"))
    if requested_owner_agent_id is not None and safe_str(requested_owner_agent_id) != existing_owner_agent_id:
        return "existing_agent_owner_mutation_forbidden"

    existing_worker_target = safe_str(roster.get("worker_target"))
    if requested_worker_target is not None and safe_str(requested_worker_target) != existing_worker_target:
        return "existing_agent_worker_target_mutation_forbidden"

    if requested_tags is not None:
        existing_tags = _normalize_tags(roster.get("tags") if isinstance(roster.get("tags"), list) else [])
        if _normalize_tags(requested_tags) != existing_tags:
            return "existing_agent_tags_mutation_forbidden"
    return None


async def _provision_identity(
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
    conn: Any = None,
) -> None:
    trace_id = normalize_trace_id(target_ctx.trace_id) if target_ctx.trace_id else None

    await resource_store.upsert_project_agent(
        ctx=target_ctx,
        profile_box_id=profile_box_id,
        worker_target=worker_target,
        tags=tags,
        display_name=display_name,
        owner_agent_id=owner_agent_id,
        metadata=metadata or {},
        conn=conn,
    )

    identity_edge_ctx = target_ctx.evolve(trace_id=trace_id)

    await identity_store.insert_identity_edge(
        ctx=identity_edge_ctx,
        action=action,
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


def _roster_snapshot(roster: Optional[Dict[str, Any]]) -> tuple[Dict[str, Any], str, str, List[str], Optional[str], Optional[str]]:
    existing_meta: Dict[str, Any] = dict(roster.get("metadata") or {}) if roster else {}
    existing_profile_box_id = safe_str(roster.get("profile_box_id") if roster else None)
    existing_worker_target = safe_str(roster.get("worker_target") if roster else None)
    existing_tags = _normalize_tags(roster.get("tags") if roster and isinstance(roster.get("tags"), list) else [])
    existing_display_name = safe_str(roster.get("display_name") if roster else None) or None
    existing_owner_agent_id = safe_str(roster.get("owner_agent_id") if roster else None) or None
    return (
        existing_meta,
        existing_profile_box_id,
        existing_worker_target,
        existing_tags,
        existing_display_name,
        existing_owner_agent_id,
    )


async def _persist_agent_ready(
    *,
    resource_store: ResourceStore,
    state_store: StateStore,
    identity_store: Optional[IdentityStore],
    target_ctx: CGContext,
    spec: AgentSpec,
    roster: Optional[Dict[str, Any]],
    existing_meta: Dict[str, Any],
    merged_meta: Dict[str, Any],
    profile_box_id_out: str,
    worker_target_out: str,
    tags_out: List[str],
    display_name_out: Optional[str],
    owner_agent_id_out: Optional[str],
    explicit_owner: bool,
    init_only_for_new_agent: bool,
    conn: Any = None,
) -> AgentLifecycleResult:
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
        explicit_owner=explicit_owner,
        explicit_profile=getattr(spec, "profile_box_id", None) is not None,
    )

    if roster_updated:
        action = "provision" if not roster else "update"
        if identity_store:
            await _provision_identity(
                identity_store=identity_store,
                resource_store=resource_store,
                target_ctx=target_ctx.evolve(channel_id=target_ctx.channel_id or spec.active_channel_id or "public"),
                action=action,
                profile_box_id=profile_box_id_out,
                worker_target=worker_target_out,
                tags=tags_out,
                display_name=display_name_out,
                owner_agent_id=owner_agent_id_out,
                metadata=merged_meta,
                conn=conn,
            )
        else:
            await resource_store.upsert_project_agent(
                ctx=target_ctx,
                profile_box_id=profile_box_id_out,
                worker_target=worker_target_out,
                tags=tags_out,
                display_name=display_name_out,
                owner_agent_id=owner_agent_id_out,
                metadata=merged_meta,
                conn=conn,
            )

    if roster and not roster_updated and init_only_for_new_agent:
        return AgentLifecycleResult(roster_updated=False)

    init_channel_id = spec.active_channel_id if (init_only_for_new_agent and not roster) else target_ctx.channel_id
    if not init_only_for_new_agent:
        init_channel_id = spec.active_channel_id or target_ctx.channel_id
    await state_store.init_if_absent(
        ctx=target_ctx.evolve(channel_id=init_channel_id),
        profile_box_id=profile_box_id_out,
        conn=conn,
    )
    return AgentLifecycleResult(roster_updated=roster_updated)


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
    target_ctx: CGContext,
    spec: AgentSpec,
    existing_roster: Optional[Dict[str, Any]] = None,
    conn: Any = None,
) -> AgentLifecycleResult:
    roster = existing_roster or await resource_store.fetch_roster(
        target_ctx,
        conn=conn,
    )

    if isinstance(spec, CreateAgentSpec):
        return await _ensure_create_agent(
            resource_store=resource_store,
            state_store=state_store,
            identity_store=identity_store,
            target_ctx=target_ctx,
            spec=spec,
            roster=roster,
            conn=conn,
        )
    if isinstance(spec, DerivedAgentSpec):
        return await _ensure_derived_agent(
            resource_store=resource_store,
            state_store=state_store,
            identity_store=identity_store,
            target_ctx=target_ctx,
            spec=spec,
            roster=roster,
            conn=conn,
        )
    raise TypeError("spec must be CreateAgentSpec or DerivedAgentSpec")


async def _ensure_create_agent(
    *,
    resource_store: ResourceStore,
    state_store: StateStore,
    identity_store: Optional[IdentityStore],
    target_ctx: CGContext,
    spec: CreateAgentSpec,
    roster: Optional[Dict[str, Any]],
    conn: Any = None,
) -> AgentLifecycleResult:
    if not roster and not safe_str(spec.profile_box_id):
        return AgentLifecycleResult(roster_updated=False, error_code="missing_profile_name")

    (
        existing_meta,
        existing_profile_box_id,
        existing_worker_target,
        existing_tags,
        existing_display_name,
        existing_owner_agent_id,
    ) = _roster_snapshot(roster)

    if roster:
        mutation_error = _validate_existing_agent_mutation(
            roster=roster,
            requested_profile_box_id=spec.profile_box_id,
            requested_owner_agent_id=spec.owner_agent_id,
            requested_worker_target=spec.worker_target,
            requested_tags=spec.tags,
        )
        if mutation_error:
            return AgentLifecycleResult(roster_updated=False, error_code=mutation_error)

    profile_box_id_out = existing_profile_box_id if roster else safe_str(spec.profile_box_id)
    if not profile_box_id_out:
        return AgentLifecycleResult(roster_updated=False, error_code="profile_box_missing")

    merged_meta = dict(existing_meta)
    merged_meta.update(spec.metadata or {})

    cached_profile = await resource_store.fetch_profile(target_ctx.project_id, profile_box_id_out, conn=conn)
    cached_profile_name = safe_str((cached_profile or {}).get("name"))
    if cached_profile_name:
        merged_meta["profile_name"] = cached_profile_name
    profile_worker_target = safe_str((cached_profile or {}).get("worker_target")) if isinstance(cached_profile, dict) else ""
    profile_tags_raw = (cached_profile or {}).get("tags") if isinstance(cached_profile, dict) else None
    profile_tags = _normalize_tags(profile_tags_raw if isinstance(profile_tags_raw, list) else [])

    worker_target_out = existing_worker_target if roster else (
        safe_str(spec.worker_target)
        or profile_worker_target
    )
    if not worker_target_out:
        return AgentLifecycleResult(roster_updated=False, error_code="worker_target_missing")

    if roster:
        tags_out = list(existing_tags)
    elif spec.tags is not None:
        tags_out = _normalize_tags(spec.tags)
    elif cached_profile is not None:
        tags_out = list(profile_tags or [])
    else:
        tags_out = []

    display_name_out = spec.display_name if spec.display_name is not None else existing_display_name
    owner_agent_id_out = existing_owner_agent_id if roster else spec.owner_agent_id

    return await _persist_agent_ready(
        resource_store=resource_store,
        state_store=state_store,
        identity_store=identity_store,
        target_ctx=target_ctx,
        spec=spec,
        roster=roster,
        existing_meta=existing_meta,
        merged_meta=merged_meta,
        profile_box_id_out=profile_box_id_out,
        worker_target_out=worker_target_out,
        tags_out=tags_out,
        display_name_out=display_name_out,
        owner_agent_id_out=owner_agent_id_out,
        explicit_owner=spec.owner_agent_id is not None,
        init_only_for_new_agent=False,
        conn=conn,
    )


async def _ensure_derived_agent(
    *,
    resource_store: ResourceStore,
    state_store: StateStore,
    identity_store: Optional[IdentityStore],
    target_ctx: CGContext,
    spec: DerivedAgentSpec,
    roster: Optional[Dict[str, Any]],
    conn: Any = None,
) -> AgentLifecycleResult:
    derived_profile_box_id, derived_profile_worker_target, derived_profile_tags, profile_err = await _resolve_profile_from_name(
        resource_store=resource_store,
        project_id=target_ctx.project_id,
        profile_name=spec.profile_name,
        conn=conn,
    )
    if profile_err:
        return AgentLifecycleResult(roster_updated=False, error_code=profile_err)

    if not roster and not (safe_str(spec.profile_box_id) or derived_profile_box_id):
        return AgentLifecycleResult(roster_updated=False, error_code="missing_derived_profile_name")

    (
        existing_meta,
        existing_profile_box_id,
        existing_worker_target,
        existing_tags,
        existing_display_name,
        existing_owner_agent_id,
    ) = _roster_snapshot(roster)

    if roster and derived_profile_box_id:
        if existing_profile_box_id and existing_profile_box_id != derived_profile_box_id:
            return AgentLifecycleResult(roster_updated=False, error_code="profile_mismatch")

    if roster:
        mutation_error = _validate_existing_agent_mutation(
            roster=roster,
            requested_profile_box_id=spec.profile_box_id,
            requested_worker_target=spec.worker_target,
            requested_tags=spec.tags,
        )
        if mutation_error:
            return AgentLifecycleResult(roster_updated=False, error_code=mutation_error)

    profile_box_id_out = existing_profile_box_id if roster else (
        safe_str(spec.profile_box_id) or derived_profile_box_id
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
        cached_profile = await resource_store.fetch_profile(target_ctx.project_id, profile_box_id_out, conn=conn)

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

    worker_target_out = existing_worker_target if roster else (
        safe_str(spec.worker_target)
        or profile_worker_target
    )
    if not worker_target_out:
        return AgentLifecycleResult(roster_updated=False, error_code="worker_target_missing")

    if roster:
        tags_out = list(existing_tags)
    elif spec.tags is not None:
        tags_out = _normalize_tags(spec.tags)
    elif cached_profile is not None or derived_profile_tags:
        tags_out = list(profile_tags or [])
    else:
        tags_out = []

    if roster:
        display_name_out = existing_display_name
        owner_agent_id_out = existing_owner_agent_id
    else:
        display_name_out = spec.display_name or spec.profile_name
        owner_agent_id_out = safe_str(spec.derived_from)

    return await _persist_agent_ready(
        resource_store=resource_store,
        state_store=state_store,
        identity_store=identity_store,
        target_ctx=target_ctx,
        spec=spec,
        roster=roster,
        existing_meta=existing_meta,
        merged_meta=merged_meta,
        profile_box_id_out=profile_box_id_out,
        worker_target_out=worker_target_out,
        tags_out=tags_out,
        display_name_out=display_name_out,
        owner_agent_id_out=owner_agent_id_out,
        explicit_owner=False,
        init_only_for_new_agent=True,
        conn=conn,
    )
