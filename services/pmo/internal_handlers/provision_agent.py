from __future__ import annotations

from typing import Any, Dict

from core.cg_context import CGContext
from core.errors import ProtocolViolationError
from core.utils import safe_str

from infra.observability.otel import get_tracer, mark_span_error, traced

from .base import InternalHandlerDeps, InternalHandlerResult
from ..agent_lifecycle import CreateAgentSpec, DerivedAgentSpec, ensure_agent_ready
from ..delegation_guard import assert_can_delegate_to_profile_name


_TRACER = get_tracer("services.pmo.internal_handlers.provision_agent")


async def _resolve_profile_name_from_box(
    *,
    deps: InternalHandlerDeps,
    project_id: str,
    profile_box_id: str,
    conn: Any = None,
) -> str | None:
    if not safe_str(profile_box_id):
        return None
    profile = await deps.resource_store.fetch_profile(project_id, profile_box_id, conn=conn)
    if not isinstance(profile, dict):
        return None
    name = safe_str(profile.get("name"))
    return name or None


async def _resolve_provision_target_profile_name(
    *,
    deps: InternalHandlerDeps,
    ctx: CGContext,
    action: str,
    target_agent_id: str,
    profile_name: str | None,
    profile_box_id: str | None,
    derived_from: str | None,
    conn: Any = None,
) -> str | None:
    explicit_profile_name = safe_str(profile_name) or None
    explicit_profile_box_id = safe_str(profile_box_id) or None
    if explicit_profile_name:
        return explicit_profile_name
    if explicit_profile_box_id:
        return await _resolve_profile_name_from_box(
            deps=deps,
            project_id=ctx.project_id,
            profile_box_id=explicit_profile_box_id,
            conn=conn,
        )
    if action == "derive" and derived_from:
        source_roster = await deps.resource_store.fetch_roster(
            ctx.evolve(agent_id=derived_from),
            conn=conn,
        )
        source_profile_box_id = safe_str((source_roster or {}).get("profile_box_id"))
        if source_profile_box_id:
            return await _resolve_profile_name_from_box(
                deps=deps,
                project_id=ctx.project_id,
                profile_box_id=source_profile_box_id,
                conn=conn,
            )
        return None
    roster = await deps.resource_store.fetch_roster(
        ctx.evolve(agent_id=target_agent_id),
        conn=conn,
    )
    roster_profile_box_id = safe_str((roster or {}).get("profile_box_id"))
    if roster_profile_box_id:
        return await _resolve_profile_name_from_box(
            deps=deps,
            project_id=ctx.project_id,
            profile_box_id=roster_profile_box_id,
            conn=conn,
        )
    return None


class ProvisionAgentHandler:
    name = "provision_agent"

    @traced(_TRACER, "pmo.agent.provision", span_arg="_span")
    async def handle(
        self,
        *,
        deps: InternalHandlerDeps,
        ctx: CGContext,
        cmd: Any,
        parent_after_execution: str,
        _span: Any = None,
    ) -> InternalHandlerResult:
        _ = parent_after_execution
        if cmd.tool_name != self.name:
            raise ProtocolViolationError(
                f"tool_name mismatch: expected {self.name}, got {cmd.tool_name}"
            )

        args = cmd.arguments or {}
        action = safe_str(args.get("action")) or ""
        if action not in ("ensure", "create", "derive"):
            return False, {"status": "rejected", "error_code": "invalid_action"}

        agent_id = safe_str(args.get("agent_id"))
        if not agent_id:
            return False, {"status": "rejected", "error_code": "missing_agent_id"}

        profile_name = safe_str(args.get("profile_name")) or None
        profile_box_id = safe_str(args.get("profile_box_id")) or None
        derived_from = safe_str(args.get("derived_from")) or None
        display_name = safe_str(args.get("display_name")) or None
        owner_agent_id = safe_str(args.get("owner_agent_id")) or None
        worker_target = safe_str(args.get("worker_target")) or None
        active_channel_id = safe_str(args.get("active_channel_id")) or None

        tags_raw = args.get("tags")
        tags = None
        if tags_raw is not None:
            if not isinstance(tags_raw, list):
                return False, {"status": "rejected", "error_code": "invalid_tags"}
            tags = [safe_str(t) for t in tags_raw if safe_str(t)]

        metadata_raw = args.get("metadata") or {}
        if metadata_raw and not isinstance(metadata_raw, dict):
            return False, {"status": "rejected", "error_code": "invalid_metadata"}
        metadata = dict(metadata_raw) if isinstance(metadata_raw, dict) else {}

        target_profile_name = await _resolve_provision_target_profile_name(
            deps=deps,
            ctx=ctx,
            action=action,
            target_agent_id=agent_id,
            profile_name=profile_name,
            profile_box_id=profile_box_id,
            derived_from=derived_from,
        )
        if target_profile_name:
            try:
                await assert_can_delegate_to_profile_name(
                    deps=deps,
                    ctx=ctx,
                    target_profile_name=target_profile_name,
                )
            except Exception as exc:  # noqa: BLE001
                error_code = safe_str(getattr(exc, "code", None)) or "delegation_denied"
                return False, {"status": "rejected", "error_code": error_code}

        trace_id = ctx.trace_id

        span = _span
        if span is not None:
            span.set_attribute("cg.action", action)
            span.set_attribute("cg.target_agent_id", agent_id)
            if ctx.agent_id:
                span.set_attribute("cg.source_agent_id", ctx.agent_id)
            if ctx.step_id:
                span.set_attribute("cg.parent_step_id", ctx.step_id)
            if derived_from:
                span.set_attribute("cg.derived_from", derived_from)

        if action == "derive":
            if not derived_from:
                return False, {"status": "rejected", "error_code": "missing_derived_from"}
            spec = DerivedAgentSpec(
                derived_from=derived_from,
                profile_name=profile_name,
                profile_box_id=profile_box_id,
                metadata=metadata,
                display_name=display_name,
                active_channel_id=active_channel_id or ctx.channel_id,
                worker_target=worker_target,
                tags=tags,
            )
        else:
            resolved_profile_box_id = profile_box_id
            if not resolved_profile_box_id and profile_name:
                profile = await deps.resource_store.find_profile_by_name(
                    ctx.project_id, profile_name
                )
                if not profile:
                    return False, {"status": "rejected", "error_code": "profile_not_found"}
                resolved_profile_box_id = safe_str(profile.get("profile_box_id")) or None
                if not resolved_profile_box_id:
                    return False, {"status": "rejected", "error_code": "profile_box_missing"}
            spec = CreateAgentSpec(
                profile_box_id=resolved_profile_box_id,
                metadata=metadata,
                display_name=display_name,
                owner_agent_id=owner_agent_id,
                active_channel_id=active_channel_id or ctx.channel_id,
                worker_target=worker_target,
                tags=tags,
            )

        try:
            lifecycle_result = await ensure_agent_ready(
                resource_store=deps.resource_store,
                state_store=deps.state_store,
                identity_store=deps.identity_store,
                target_ctx=ctx.evolve(
                    agent_id=agent_id,
                    trace_id=trace_id or ctx.trace_id,
                    parent_agent_id=ctx.agent_id,
                    parent_step_id=ctx.step_id,
                ),
                spec=spec,
            )
        except Exception as exc:  # noqa: BLE001
            mark_span_error(span, exc)
            raise
        if lifecycle_result.error_code:
            return False, {"status": "rejected", "error_code": lifecycle_result.error_code}

        return False, {
            "status": "accepted",
            "agent_id": agent_id,
            "roster_updated": lifecycle_result.roster_updated,
        }
