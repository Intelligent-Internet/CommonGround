from __future__ import annotations

import json
from typing import Any, Dict, Optional

from core.events import LLMStreamPayload
from core.subject import format_subject


def build_stream_subject(*, project_id: str, channel_id: str, agent_id: str) -> str:
    return format_subject(project_id, channel_id, "str", "agent", agent_id, "chunk")


def build_stream_start_payload(
    *,
    agent_turn_id: str,
    step_id: str,
    metadata: Dict[str, Any],
) -> LLMStreamPayload:
    return LLMStreamPayload(
        agent_turn_id=agent_turn_id,
        step_id=step_id,
        chunk_type="start",
        content="",
        index=0,
        metadata=metadata,
    )


def build_stream_content_payload(
    *,
    agent_turn_id: str,
    step_id: str,
    content: str,
) -> LLMStreamPayload:
    return LLMStreamPayload(
        agent_turn_id=agent_turn_id,
        step_id=step_id,
        chunk_type="content",
        content=content,
        index=0,
    )


def build_stream_end_payload(
    *,
    agent_turn_id: str,
    step_id: str,
    usage: Optional[Dict[str, Any]],
    response_cost: Optional[float],
    include_timing_metadata: bool,
    llm_timing: Dict[str, Any],
) -> LLMStreamPayload:
    metadata = {"llm_timing": llm_timing} if include_timing_metadata else {}
    return LLMStreamPayload(
        agent_turn_id=agent_turn_id,
        step_id=step_id,
        chunk_type="end",
        content="",
        index=0,
        usage=usage,
        response_cost=response_cost,
        metadata=metadata,
    )


async def publish_stream_payload(
    *,
    nats: Any,
    subject: str,
    payload: LLMStreamPayload,
    logger: Any,
) -> None:
    try:
        await nats.publish_core(
            subject,
            json.dumps(payload.model_dump(exclude_none=True, exclude_defaults=True)).encode("utf-8"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("stream publish failed: %s", exc)
