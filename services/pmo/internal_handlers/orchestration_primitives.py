from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, List

import uuid6

from core.cg_context import CGContext
from core.errors import BadRequestError, InternalError, NotFoundError
from core.utils import safe_str
from infra.agent_dispatcher import AgentDispatcher, DispatchRequest, DispatchResult
from infra.event_emitter import emit_agent_state
from infra.l0_engine import WakeupSignal

from ..agent_lifecycle import CreateAgentSpec, DerivedAgentSpec, ensure_agent_ready
from ..delegation_guard import assert_can_delegate_to_profile_name

def _derive_agent_id(profile_ref: str) -> str:
    return f"{str(profile_ref).lower()}_{uuid6.uuid7().hex}"


@dataclass(frozen=True, slots=True)
class TargetPreview:
    target_strategy: str
    target_ref: str
    profile_box_id: str
    profile_name: str
    display_name: str
    reuse_agent_id: Optional[str] = None
    clone_source_agent_id: Optional[str] = None


@dataclass(frozen=True, slots=True)
class ResolvedTarget:
    agent_id: str
    profile_box_id: str
    profile_name: str
    display_name: str
    provision: bool
    clone_source_agent_id: Optional[str] = None


async def _resolve_profile_name_from_box(
    *,
    deps: Any,
    project_id: str,
    profile_box_id: str,
    conn: Any = None,
) -> str:
    profile = await deps.resource_store.fetch_profile(project_id, profile_box_id, conn=conn)
    name = safe_str((profile or {}).get("name")) if isinstance(profile, dict) else ""
    return name or str(profile_box_id)


async def preview_target(
    *,
    ctx: Any,
    target_strategy: str,
    target_ref: str,
    conn: Any = None,
) -> TargetPreview:
    deps = ctx.deps
    meta = ctx.meta
    cmd = ctx.cmd

    strategy = safe_str(target_strategy).lower()
    ref = safe_str(target_ref)
    if strategy not in ("new", "reuse", "clone"):
        raise BadRequestError("delegate_async: args.target_strategy must be one of new|reuse|clone")
    if not ref:
        raise BadRequestError("delegate_async: args.target_ref is required")

    if strategy == "new":
        profile = await deps.resource_store.find_profile_by_name(meta.project_id, ref, conn=conn)
        if not profile:
            raise NotFoundError(f"delegate_async: profile not found: {ref}")
        profile_box_id = safe_str(profile.get("profile_box_id"))
        if not profile_box_id:
            raise BadRequestError(f"delegate_async: profile_box_id missing for profile {ref}")
        profile_name = safe_str(profile.get("name")) or ref
        await assert_can_delegate_to_profile_name(
            deps=deps,
            project_id=meta.project_id,
            caller_agent_id=cmd.source_agent_id,
            target_profile_name=profile_name,
            conn=conn,
        )
        return TargetPreview(
            target_strategy=strategy,
            target_ref=ref,
            profile_box_id=profile_box_id,
            profile_name=profile_name,
            display_name=profile_name,
        )

    if strategy == "reuse":
        roster = await deps.resource_store.fetch_roster(meta.project_id, ref, conn=conn)
        if not roster:
            raise NotFoundError(f"delegate_async: target agent not found: {ref}")
        profile_box_id = safe_str(roster.get("profile_box_id"))
        if not profile_box_id:
            raise BadRequestError(f"delegate_async: target agent profile_box_id missing: {ref}")
        profile_name = await _resolve_profile_name_from_box(
            deps=deps,
            project_id=meta.project_id,
            profile_box_id=profile_box_id,
            conn=conn,
        )
        await assert_can_delegate_to_profile_name(
            deps=deps,
            project_id=meta.project_id,
            caller_agent_id=cmd.source_agent_id,
            target_profile_name=profile_name,
            conn=conn,
        )
        display_name = safe_str(roster.get("display_name")) or profile_name
        return TargetPreview(
            target_strategy=strategy,
            target_ref=ref,
            profile_box_id=profile_box_id,
            profile_name=profile_name,
            display_name=display_name,
            reuse_agent_id=ref,
        )

    source_agent_id = ref
    roster = await deps.resource_store.fetch_roster(meta.project_id, source_agent_id, conn=conn)
    if not roster:
        raise NotFoundError(f"delegate_async: clone source agent not found: {source_agent_id}")
    source_profile_box_id = safe_str(roster.get("profile_box_id"))
    if not source_profile_box_id:
        raise BadRequestError(
            f"delegate_async: clone source profile_box_id missing: {source_agent_id}"
        )
    source_profile_name = await _resolve_profile_name_from_box(
        deps=deps,
        project_id=meta.project_id,
        profile_box_id=source_profile_box_id,
        conn=conn,
    )
    await assert_can_delegate_to_profile_name(
        deps=deps,
        project_id=meta.project_id,
        caller_agent_id=cmd.source_agent_id,
        target_profile_name=source_profile_name,
        conn=conn,
    )
    display_name = safe_str(roster.get("display_name")) or source_profile_name
    return TargetPreview(
        target_strategy=strategy,
        target_ref=ref,
        profile_box_id=source_profile_box_id,
        profile_name=source_profile_name,
        display_name=display_name,
        clone_source_agent_id=source_agent_id,
    )


async def materialize_target(
    *,
    ctx: Any,
    preview: TargetPreview,
    conn: Any = None,
) -> ResolvedTarget:
    deps = ctx.deps
    meta = ctx.meta
    cmd = ctx.cmd

    if preview.target_strategy == "reuse":
        agent_id = safe_str(preview.reuse_agent_id)
        if not agent_id:
            raise BadRequestError("delegate_async: missing reuse target agent_id")
        return ResolvedTarget(
            agent_id=agent_id,
            profile_box_id=preview.profile_box_id,
            profile_name=preview.profile_name,
            display_name=preview.display_name,
            provision=False,
        )

    if preview.target_strategy == "new":
        agent_id = _derive_agent_id(preview.profile_box_id)
        lifecycle = await ensure_agent_ready(
            resource_store=deps.resource_store,
            state_store=deps.state_store,
            identity_store=deps.identity_store,
            project_id=meta.project_id,
            agent_id=agent_id,
            spec=CreateAgentSpec(
                profile_box_id=preview.profile_box_id,
                metadata={"spawned_by": cmd.source_agent_id},
                display_name=f"{preview.profile_name}",
                owner_agent_id=cmd.source_agent_id,
                active_channel_id=meta.channel_id,
            ),
            source_agent_id=cmd.source_agent_id,
            parent_step_id=cmd.step_id,
            trace_id=ctx.trace_id,
            channel_id=meta.channel_id,
            conn=conn,
        )
        if lifecycle.error_code:
            raise InternalError(f"delegate_async: create lifecycle failed: {lifecycle.error_code}")
        return ResolvedTarget(
            agent_id=agent_id,
            profile_box_id=preview.profile_box_id,
            profile_name=preview.profile_name,
            display_name=preview.profile_name,
            provision=True,
        )

    if preview.target_strategy == "clone":
        source_agent_id = safe_str(preview.clone_source_agent_id)
        if not source_agent_id:
            raise BadRequestError("delegate_async: clone source agent missing")
        agent_id = _derive_agent_id(preview.profile_box_id)
        lifecycle = await ensure_agent_ready(
            resource_store=deps.resource_store,
            state_store=deps.state_store,
            identity_store=deps.identity_store,
            project_id=meta.project_id,
            agent_id=agent_id,
            spec=DerivedAgentSpec(
                derived_from=source_agent_id,
                profile_name=preview.profile_name,
                profile_box_id=preview.profile_box_id,
                metadata={"spawned_by": cmd.source_agent_id},
                display_name=f"{preview.profile_name}",
                active_channel_id=meta.channel_id,
            ),
            source_agent_id=cmd.source_agent_id,
            parent_step_id=cmd.step_id,
            trace_id=ctx.trace_id,
            channel_id=meta.channel_id,
            conn=conn,
        )
        if lifecycle.error_code:
            raise InternalError(f"delegate_async: clone lifecycle failed: {lifecycle.error_code}")
        return ResolvedTarget(
            agent_id=agent_id,
            profile_box_id=preview.profile_box_id,
            profile_name=preview.profile_name,
            display_name=preview.profile_name,
            provision=True,
            clone_source_agent_id=source_agent_id,
        )

    raise BadRequestError(
        "delegate_async: args.target_strategy must be one of new|reuse|clone"
    )


async def resolve_current_output_box_id(
    *,
    deps: Any,
    project_id: str,
    agent_id: str,
    conn: Any = None,
) -> str:
    state = await deps.state_store.fetch(project_id, agent_id, conn=conn)
    active_status = safe_str(getattr(state, "status", None))
    if state and active_status in ("dispatched", "running", "suspended"):
        active_output_box_id = safe_str(getattr(state, "output_box_id", None))
        if active_output_box_id:
            return active_output_box_id

    pointers = await deps.state_store.fetch_pointers(project_id, agent_id, conn=conn)
    if isinstance(pointers, dict):
        last_output_box_id = safe_str((pointers or {}).get("last_output_box_id"))
    else:
        last_output_box_id = safe_str(getattr(pointers, "last_output_box_id", None))
    if last_output_box_id:
        return last_output_box_id
    raise BadRequestError(f"delegate_async: source output_box missing for agent {agent_id}")


async def validate_box_ids_exist(
    *,
    deps: Any,
    project_id: str,
    box_ids: Iterable[str],
    conn: Any = None,
) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    for raw in box_ids:
        box_id = safe_str(raw)
        if not box_id or box_id in seen:
            continue
        seen.add(box_id)
        box = await deps.cardbox.get_box(box_id, project_id=project_id, conn=conn)
        if not box:
            raise NotFoundError(f"inherit_context: box not found: {box_id}")
        normalized.append(box_id)
    return normalized


async def pack_context_with_instruction(
    *,
    deps: Any,
    project_id: str,
    source_agent_id: str,
    tool_suffix: str,
    profile_name: str,
    instruction: str,
    inherit_box_ids: Optional[Iterable[str]] = None,
    conn: Any = None,
) -> str:
    box_ids = [safe_str(x) for x in (inherit_box_ids or []) if safe_str(x)]

    handover = {
        "target_profile_config": {"profile_name": profile_name},
        "context_packing_config": {
            "pack_arguments": [{"arg_key": "instruction", "as_card_type": "task.instruction"}],
            "inherit_context": {
                "include_boxes_from_args": ["input_box_ids"] if box_ids else [],
                "include_parent": True,
            },
        },
        "metadata": {"internal": True, "function": "delegate_async"},
    }
    args: Dict[str, Any] = {
        "profile_name": profile_name,
        "instruction": instruction,
    }
    if box_ids:
        args["input_box_ids"] = box_ids

    context_box_id, _profile_box_id, _ = await deps.handover.pack_context(
        project_id=project_id,
        tool_suffix=tool_suffix,
        source_agent_id=source_agent_id,
        arguments=args,
        handover=handover,
        conn=conn,
    )
    return str(context_box_id)


async def dispatch_to_agent_transactional(
    *,
    deps: Any,
    project_id: str,
    channel_id: str,
    target_agent_id: str,
    profile_box_id: str,
    context_box_id: str,
    parent_agent_turn_id: Optional[str],
    parent_tool_call_id: Optional[str],
    parent_step_id: Optional[str],
    trace_id: Optional[str],
    display_name: Optional[str],
    headers: Dict[str, str],
    conn: Any,
) -> tuple[DispatchResult, list[WakeupSignal]]:
    dispatcher = AgentDispatcher(
        resource_store=deps.resource_store,
        state_store=deps.state_store,
        cardbox=deps.cardbox,
        nats=deps.nats,
    )
    return await dispatcher.dispatch_transactional(
        DispatchRequest(
            project_id=project_id,
            channel_id=channel_id,
            agent_id=target_agent_id,
            profile_box_id=profile_box_id,
            context_box_id=context_box_id,
            parent_agent_turn_id=parent_agent_turn_id,
            parent_tool_call_id=parent_tool_call_id,
            parent_step_id=parent_step_id,
            trace_id=trace_id,
            display_name=display_name,
            runtime_config={},
            headers=headers,
            emit_state_event=False,
        ),
        conn=conn,
    )


async def publish_wakeup_signals(*, deps: Any, signals: List[WakeupSignal]) -> None:
    if not signals:
        return
    dispatcher = AgentDispatcher(
        resource_store=deps.resource_store,
        state_store=deps.state_store,
        cardbox=deps.cardbox,
        nats=deps.nats,
    )
    await dispatcher.l0.publish_wakeup_signals(list(signals))


async def publish_dispatched_state_event(
    *,
    deps: Any,
    project_id: str,
    channel_id: str,
    agent_id: str,
    agent_turn_id: Optional[str],
    turn_epoch: Optional[int],
    output_box_id: Optional[str],
    trace_id: Optional[str],
    parent_step_id: Optional[str],
    headers: Dict[str, str],
) -> None:
    if turn_epoch is None:
        return
    resolved_agent_turn_id = safe_str(agent_turn_id)
    if not resolved_agent_turn_id:
        return
    state_ctx = CGContext(
        project_id=project_id,
        channel_id=channel_id,
        agent_id=agent_id,
        agent_turn_id=resolved_agent_turn_id,
        trace_id=safe_str(trace_id),
        headers=dict(headers or {}),
        parent_step_id=safe_str(parent_step_id),
    )
    await emit_agent_state(
        nats=deps.nats,
        ctx=state_ctx,
        turn_epoch=int(turn_epoch),
        status="dispatched",
        output_box_id=output_box_id,
    )
