from __future__ import annotations

from typing import Any, Dict

from core.errors import ProtocolViolationError
from core.utils import safe_str

from infra.observability.otel import get_tracer, mark_span_error, traced

from .base import InternalHandlerResult
from .context import InternalHandlerContext
from ..agent_lifecycle import CreateAgentSpec, DerivedAgentSpec, ensure_agent_ready


_TRACER = get_tracer("services.pmo.internal_handlers.provision_agent")


class ProvisionAgentHandler:
    name = "provision_agent"

    @traced(_TRACER, "pmo.agent.provision", span_arg="_span")
    async def handle(
        self,
        *,
        ctx: InternalHandlerContext,
        parent_after_execution: str,
        _span: Any = None,
    ) -> InternalHandlerResult:
        deps = ctx.deps
        meta = ctx.meta
        cmd = ctx.cmd
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

        trace_id = ctx.trace_id

        span = _span
        if span is not None:
            span.set_attribute("cg.project_id", str(meta.project_id))
            span.set_attribute("cg.channel_id", str(meta.channel_id))
            span.set_attribute("cg.action", str(action))
            span.set_attribute("cg.target_agent_id", str(agent_id))
            if cmd.source_agent_id:
                span.set_attribute("cg.source_agent_id", str(cmd.source_agent_id))
            if cmd.step_id:
                span.set_attribute("cg.parent_step_id", str(cmd.step_id))
            if derived_from:
                span.set_attribute("cg.derived_from", str(derived_from))

        if action == "derive":
            if not derived_from:
                return False, {"status": "rejected", "error_code": "missing_derived_from"}
            spec = DerivedAgentSpec(
                derived_from=derived_from,
                profile_name=profile_name,
                profile_box_id=profile_box_id,
                metadata=metadata,
                display_name=display_name,
                active_channel_id=active_channel_id or meta.channel_id,
                worker_target=worker_target,
                tags=tags,
            )
        else:
            resolved_profile_box_id = profile_box_id
            if not resolved_profile_box_id and profile_name:
                profile = await deps.resource_store.find_profile_by_name(
                    meta.project_id, profile_name
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
                active_channel_id=active_channel_id or meta.channel_id,
                worker_target=worker_target,
                tags=tags,
            )

        try:
            lifecycle_result = await ensure_agent_ready(
                resource_store=deps.resource_store,
                state_store=deps.state_store,
                identity_store=deps.identity_store,
                project_id=meta.project_id,
                agent_id=agent_id,
                spec=spec,
                source_agent_id=cmd.source_agent_id,
                parent_step_id=cmd.step_id,
                trace_id=trace_id,
                channel_id=meta.channel_id,
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
