from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.cg_context import CGContext
from core.events import AgentResultPayload, AgentStatePayload, StepEventPayload
from core.subject import evt_agent_state_subject, evt_agent_step_subject, evt_agent_task_subject
from core.time_utils import utc_now_iso
from core.utils import safe_str
from infra.nats_client import NATSClient


def _iso_now() -> str:
    return utc_now_iso()


def extract_llm_meta(timing: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    llm = timing.get("llm") if isinstance(timing, dict) else None
    if not isinstance(llm, dict) or not llm:
        return {}
    meta: Dict[str, Any] = {"llm_timing": llm}
    request_started_at = llm.get("request_started_at")
    response_received_at = llm.get("response_received_at")
    error_at = llm.get("error_at")
    if request_started_at:
        meta["llm_request_started_at"] = request_started_at
    if response_received_at:
        meta["llm_response_received_at"] = response_received_at
    if error_at:
        meta["llm_error_at"] = error_at
    return meta

def _merge_metadata(
    metadata: Optional[Dict[str, Any]],
    *,
    timing: Optional[Dict[str, Any]],
    include_llm_meta: bool,
) -> Dict[str, Any]:
    meta = dict(metadata or {})
    if timing:
        meta.setdefault("timing", timing)
        if include_llm_meta:
            llm_meta = extract_llm_meta(timing)
            for key, value in llm_meta.items():
                meta.setdefault(key, value)
    return meta


def _merge_stats(
    stats: Optional[Dict[str, Any]],
    *,
    timing: Optional[Dict[str, Any]],
    include_llm_meta: bool,
) -> Dict[str, Any]:
    merged = dict(stats or {})
    if timing:
        merged["timing"] = timing
        if include_llm_meta:
            merged.update(extract_llm_meta(timing))
    return merged


def _resolve_agent_turn_context(ctx: CGContext) -> tuple[str, str, str, str, Dict[str, str]]:
    resolved_project_id = str(ctx.project_id or "")
    resolved_channel_id = str(ctx.channel_id or "")
    resolved_agent_id = str(ctx.agent_id or "")
    resolved_agent_turn_id = str(ctx.agent_turn_id or "")
    resolved_headers = dict(ctx.headers or {})
    if not resolved_project_id or not resolved_channel_id or not resolved_agent_id or not resolved_agent_turn_id:
        raise ValueError("emit_agent_* requires project/channel/agent/agent_turn ids")
    return (
        resolved_project_id,
        resolved_channel_id,
        resolved_agent_id,
        resolved_agent_turn_id,
        resolved_headers,
    )


async def emit_agent_state(
    *,
    nats: NATSClient,
    ctx: CGContext,
    turn_epoch: Optional[int],
    status: str,
    output_box_id: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    if turn_epoch is None:
        return
    project_id, channel_id, agent_id, agent_turn_id, headers = _resolve_agent_turn_context(ctx)
    subject = evt_agent_state_subject(project_id, channel_id, agent_id)
    payload = AgentStatePayload(
        agent_id=agent_id,
        agent_turn_id=agent_turn_id,
        status=status,  # type: ignore[arg-type]
        turn_epoch=int(turn_epoch),
        updated_at=_iso_now(),
        output_box_id=safe_str(output_box_id),
        metadata=dict(metadata or {}),
    )
    await nats.publish_event(subject, payload.model_dump(), headers=headers)


async def emit_agent_step(
    *,
    nats: NATSClient,
    ctx: CGContext,
    step_id: str,
    phase: str,
    metadata: Optional[Dict[str, Any]] = None,
    new_card_ids: Optional[List[str]] = None,
    timing: Optional[Dict[str, Any]] = None,
    include_llm_meta: bool = False,
) -> None:
    project_id, channel_id, agent_id, agent_turn_id, headers = _resolve_agent_turn_context(ctx)
    subject = evt_agent_step_subject(project_id, channel_id, agent_id)
    meta = _merge_metadata(metadata, timing=timing, include_llm_meta=include_llm_meta)
    payload = StepEventPayload(
        agent_turn_id=agent_turn_id,
        step_id=step_id,
        phase=phase,  # type: ignore[arg-type]
        metadata=meta,
        new_card_ids=new_card_ids or [],
    )
    await nats.publish_event(subject, payload.model_dump(), headers=headers)


async def emit_agent_task(
    *,
    nats: NATSClient,
    ctx: CGContext,
    status: str,
    output_box_id: Optional[str] = None,
    deliverable_card_id: Optional[str] = None,
    tool_result_card_id: Optional[str] = None,
    error: Optional[str] = None,
    error_code: Optional[str] = None,
    stats: Optional[Dict[str, Any]] = None,
    timing: Optional[Dict[str, Any]] = None,
    include_llm_meta: bool = False,
) -> None:
    project_id, channel_id, agent_id, agent_turn_id, headers = _resolve_agent_turn_context(ctx)
    subject = evt_agent_task_subject(project_id, channel_id, agent_id)
    merged_stats = _merge_stats(stats, timing=timing, include_llm_meta=include_llm_meta)
    payload = AgentResultPayload(
        agent_turn_id=agent_turn_id,
        status=status,  # type: ignore[arg-type]
        output_box_id=safe_str(output_box_id),
        deliverable_card_id=safe_str(deliverable_card_id),
        tool_result_card_id=safe_str(tool_result_card_id),
        error=error,
        error_code=error_code,
        stats=merged_stats,
    )
    await nats.publish_event(subject, payload.model_dump(), headers=headers)
