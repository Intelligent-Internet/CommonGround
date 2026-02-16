import json

import pytest

from services.agent_worker.react_streaming import (
    build_stream_content_payload,
    build_stream_end_payload,
    build_stream_start_payload,
    build_stream_subject,
    publish_stream_payload,
)


class _DummyNats:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    async def publish_core(self, subject, payload):
        self.calls.append((subject, payload))
        if self.fail:
            raise RuntimeError("nats down")


class _DummyLogger:
    def __init__(self):
        self.errors = []

    def error(self, msg, *args):
        self.errors.append(msg % args if args else str(msg))


def test_build_stream_subject_uses_agent_chunk_subject() -> None:
    subject = build_stream_subject(project_id="proj_1", channel_id="public", agent_id="agent_1")
    assert subject == "cg.v1r3.proj_1.public.str.agent.agent_1.chunk"


def test_build_stream_payload_helpers() -> None:
    start = build_stream_start_payload(
        agent_turn_id="turn_1",
        step_id="step_1",
        metadata={"x": 1},
    )
    assert start.chunk_type == "start"
    assert start.metadata == {"x": 1}

    content = build_stream_content_payload(
        agent_turn_id="turn_1",
        step_id="step_1",
        content="hello",
    )
    assert content.chunk_type == "content"
    assert content.content == "hello"

    end = build_stream_end_payload(
        agent_turn_id="turn_1",
        step_id="step_1",
        usage={"total_tokens": 10},
        response_cost=0.12,
        include_timing_metadata=True,
        llm_timing={"duration_ms": 1},
    )
    assert end.chunk_type == "end"
    assert end.usage == {"total_tokens": 10}
    assert end.response_cost == 0.12
    assert end.metadata == {"llm_timing": {"duration_ms": 1}}


@pytest.mark.asyncio
async def test_publish_stream_payload_serializes_and_publishes() -> None:
    nats = _DummyNats()
    logger = _DummyLogger()
    payload = build_stream_content_payload(
        agent_turn_id="turn_1",
        step_id="step_1",
        content="hello",
    )
    await publish_stream_payload(
        nats=nats,
        subject="cg.v1r3.proj_1.public.str.agent.agent_1.chunk",
        payload=payload,
        logger=logger,
    )
    assert len(nats.calls) == 1
    subject, body = nats.calls[0]
    assert subject.endswith(".chunk")
    decoded = json.loads(body.decode("utf-8"))
    assert decoded["chunk_type"] == "content"
    assert decoded["content"] == "hello"
    assert logger.errors == []


@pytest.mark.asyncio
async def test_publish_stream_payload_logs_and_swallows_exceptions() -> None:
    nats = _DummyNats(fail=True)
    logger = _DummyLogger()
    payload = build_stream_start_payload(
        agent_turn_id="turn_1",
        step_id="step_1",
        metadata={},
    )
    await publish_stream_payload(
        nats=nats,
        subject="cg.v1r3.proj_1.public.str.agent.agent_1.chunk",
        payload=payload,
        logger=logger,
    )
    assert len(logger.errors) == 1
    assert "stream publish failed" in logger.errors[0]
