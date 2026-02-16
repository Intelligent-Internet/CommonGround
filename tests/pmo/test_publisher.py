import types

import pytest

from core.headers import RECURSION_DEPTH_HEADER
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

    async def report(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(wakeup_signals=())

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
        "tool_call_id": "call_1",
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "after_execution": "suspend",
        "status": "success",
        "tool_result_card_id": "card_1",
        "agent_id": "agent_1",
        "step_id": "step_1",
    }


@pytest.mark.asyncio
async def test_publish_tool_result_report_uses_existing_depth_header() -> None:
    store = _DummyExecutionStore(depth=9)
    capture = _CapturePublish()
    await publish_tool_result_report(
        nats=object(),
        execution_store=store,
        l0_engine=capture,
        project_id="proj_1",
        channel_id="public",
        payload=_base_payload(),
        headers={RECURSION_DEPTH_HEADER: "7"},
    )
    assert store.calls == []
    assert len(capture.calls) == 1
    published = capture.calls[0]
    assert published["recursion_depth"] == 7
    assert published["headers"][RECURSION_DEPTH_HEADER] == "7"


@pytest.mark.asyncio
async def test_publish_tool_result_report_backfills_depth_from_execution_store() -> None:
    store = _DummyExecutionStore(depth=3)
    capture = _CapturePublish()
    await publish_tool_result_report(
        nats=object(),
        execution_store=store,
        l0_engine=capture,
        project_id="proj_1",
        channel_id="public",
        payload=_base_payload(),
        headers={},
    )
    assert store.calls == [("proj_1", "call_1")]
    published = capture.calls[0]
    assert published["recursion_depth"] == 3
    assert published["headers"][RECURSION_DEPTH_HEADER] == "3"


@pytest.mark.asyncio
async def test_publish_tool_result_report_defaults_depth_when_lookup_fails() -> None:
    store = _DummyExecutionStore(fail=True)
    capture = _CapturePublish()
    await publish_tool_result_report(
        nats=object(),
        execution_store=store,
        l0_engine=capture,
        project_id="proj_1",
        channel_id="public",
        payload=_base_payload(),
        headers={},
    )
    assert store.calls == [("proj_1", "call_1")]
    published = capture.calls[0]
    assert published["recursion_depth"] == 0
    assert published["headers"][RECURSION_DEPTH_HEADER] == "0"
