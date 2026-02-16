import pytest

from core.cg_context import CGContext
from core.subject import evt_agent_state_subject, evt_agent_step_subject, evt_agent_task_subject
from infra.event_emitter import emit_agent_state, emit_agent_step, emit_agent_task


class FakeNATS:
    def __init__(self) -> None:
        self.calls = []

    async def publish_event(self, subject, payload, headers=None, **kwargs):
        self.calls.append(
            {
                "subject": subject,
                "payload": payload,
                "headers": headers or {},
                "kwargs": kwargs,
            }
        )


@pytest.mark.asyncio
async def test_emit_agent_state_skips_missing_epoch() -> None:
    nats = FakeNATS()
    await emit_agent_state(
        nats=nats,
        ctx=CGContext(
            project_id="proj",
            channel_id="public",
            agent_id="agent",
            agent_turn_id="turn_1",
        ),
        turn_epoch=None,
        status="idle",
        output_box_id=None,
    )
    assert nats.calls == []


@pytest.mark.asyncio
async def test_emit_agent_step_merges_metadata_with_llm_timing() -> None:
    nats = FakeNATS()
    timing = {
        "llm": {
            "request_started_at": "2026-02-05T08:00:00Z",
            "response_received_at": "2026-02-05T08:00:01Z",
        }
    }
    await emit_agent_step(
        nats=nats,
        ctx=CGContext(
            project_id="proj",
            channel_id="public",
            agent_id="agent",
            agent_turn_id="turn_1",
            headers={"h": "1"},
        ),
        step_id="step_1",
        phase="executing",
        metadata={"llm_request_started_at": "preset"},
        timing=timing,
        include_llm_meta=True,
    )
    assert len(nats.calls) == 1
    call = nats.calls[0]
    assert call["subject"] == evt_agent_step_subject("proj", "public", "agent")
    meta = call["payload"]["metadata"]
    assert meta["timing"] == timing
    assert meta["llm_timing"] == timing["llm"]
    assert meta["llm_request_started_at"] == "preset"
    assert meta["llm_response_received_at"] == "2026-02-05T08:00:01Z"


@pytest.mark.asyncio
async def test_emit_agent_state_accepts_ctx() -> None:
    nats = FakeNATS()
    await emit_agent_state(
        nats=nats,
        ctx=CGContext(
            project_id="proj",
            channel_id="public",
            agent_id="agent",
            agent_turn_id="turn_2",
            headers={"h": "2"},
        ),
        turn_epoch=2,
        status="running",
        output_box_id="box_2",
    )
    assert len(nats.calls) == 1
    call = nats.calls[0]
    assert call["subject"] == evt_agent_state_subject("proj", "public", "agent")
    assert call["headers"] == {"h": "2"}
    assert call["payload"]["agent_turn_id"] == "turn_2"
    assert call["payload"]["output_box_id"] == "box_2"


@pytest.mark.asyncio
async def test_emit_agent_task_merges_stats_with_llm_timing() -> None:
    nats = FakeNATS()
    timing = {"llm": {"request_started_at": "2026-02-05T08:00:00Z"}}
    await emit_agent_task(
        nats=nats,
        ctx=CGContext(
            project_id="proj",
            channel_id="public",
            agent_id="agent",
            agent_turn_id="turn_1",
        ),
        status="failed",
        output_box_id="box_1",
        error="boom",
        stats={"duration_ms": 12, "timing": {"custom": True}},
        timing=timing,
        include_llm_meta=True,
    )
    assert len(nats.calls) == 1
    call = nats.calls[0]
    assert call["subject"] == evt_agent_task_subject("proj", "public", "agent")
    stats = call["payload"]["stats"]
    assert stats["duration_ms"] == 12
    assert stats["timing"] == timing
    assert stats["llm_timing"] == timing["llm"]
    assert stats["llm_request_started_at"] == "2026-02-05T08:00:00Z"
