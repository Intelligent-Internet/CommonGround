import types

import pytest

from core.cg_context import CGContext
from infra.l0.tool_reports import publish_tool_result_report


class _DummyExecutionStore:
    def __init__(self, *, depth=None, fail=False):
        self.depth = depth
        self.fail = fail
        self.calls = []
        self.pool = _DummyPool()

    async def get_request_recursion_depth(self, *, project_id, correlation_id):
        self.calls.append((project_id, correlation_id))
        if self.fail:
            raise RuntimeError("db error")
        return self.depth


class _CapturePublish:
    def __init__(self):
        self.calls = []

    async def report_intent(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(status="accepted", error_code=None, wakeup_signals=())

    async def publish_wakeup_signals(self, signals):  # noqa: ANN001
        _ = signals


class _DummyTx:
    async def __aenter__(self):  # noqa: ANN201
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        return False


class _DummyConn:
    def transaction(self) -> _DummyTx:
        return _DummyTx()

    async def __aenter__(self):  # noqa: ANN201
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN201
        return False


class _DummyPool:
    def connection(self) -> _DummyConn:
        return _DummyConn()


def _base_payload() -> dict:
    return {
        "after_execution": "suspend",
        "status": "success",
        "tool_result_card_id": "card_1",
    }

def _base_target_ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=1,
        step_id="step_1",
        tool_call_id="call_1",
    )


def _base_source_ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="sys.pmo",
    )


@pytest.mark.asyncio
async def test_publish_tool_result_report_uses_existing_depth_header() -> None:
    store = _DummyExecutionStore(depth=9)
    capture = _CapturePublish()
    target_ctx = _base_target_ctx().with_transport(headers={"x-test": "1"})
    await publish_tool_result_report(
        nats=object(),
        execution_store=store,
        l0_engine=capture,
        source_ctx=_base_source_ctx(),
        target_ctx=target_ctx,
        payload=_base_payload(),
    )
    assert store.calls == []
    assert len(capture.calls) == 1
    published = capture.calls[0]
    intent = published["intent"]
    assert intent.message_type == "tool_result"
    assert intent.correlation_id == "call_1"
    assert intent.target.agent_id == "agent_1"
    assert intent.target.agent_turn_id == "turn_1"
    assert intent.target.expected_turn_epoch == 1


@pytest.mark.asyncio
async def test_publish_tool_result_report_defaults_depth_without_db_fallback() -> None:
    store = _DummyExecutionStore(depth=3)
    capture = _CapturePublish()
    target_ctx = _base_target_ctx().with_transport(headers={})
    await publish_tool_result_report(
        nats=object(),
        execution_store=store,
        l0_engine=capture,
        source_ctx=_base_source_ctx(),
        target_ctx=target_ctx,
        payload=_base_payload(),
    )
    assert store.calls == []
    published = capture.calls[0]
    intent = published["intent"]
    assert intent.target.agent_turn_id == "turn_1"
    assert intent.target.expected_turn_epoch == 1


@pytest.mark.asyncio
async def test_publish_tool_result_report_ignores_store_depth_lookup_errors() -> None:
    store = _DummyExecutionStore(fail=True)
    capture = _CapturePublish()
    target_ctx = _base_target_ctx().with_transport(headers={})
    await publish_tool_result_report(
        nats=object(),
        execution_store=store,
        l0_engine=capture,
        source_ctx=_base_source_ctx(),
        target_ctx=target_ctx,
        payload=_base_payload(),
    )
    assert store.calls == []
    published = capture.calls[0]
    intent = published["intent"]
    assert intent.target.agent_turn_id == "turn_1"
    assert intent.target.expected_turn_epoch == 1
