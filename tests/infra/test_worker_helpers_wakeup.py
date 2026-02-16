import logging

import pytest

from infra.worker_helpers.wakeup import publish_idle_wakeup


class _FakeResult:
    def __init__(self, *, status: str, error_code: str | None = None) -> None:
        self.status = status
        self.error_code = error_code


class _FakeEngine:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    async def wakeup(self, **kwargs):
        self.calls.append(kwargs)
        if not self._results:
            return _FakeResult(status="accepted")
        nxt = self._results.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


@pytest.mark.asyncio
async def test_publish_idle_wakeup_returns_true_on_accepted_status() -> None:
    engine = _FakeEngine([_FakeResult(status="accepted")])

    ok = await publish_idle_wakeup(
        nats=object(),
        resource_store=object(),
        state_store=object(),
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        headers={},
        logger=logging.getLogger("test"),
        retry_count=2,
        retry_delay=0.0,
        l0_engine=engine,
    )

    assert ok is True
    assert len(engine.calls) == 1
    assert engine.calls[0]["headers"]["CG-Recursion-Depth"] == "0"


@pytest.mark.asyncio
async def test_publish_idle_wakeup_retries_when_status_error_then_succeeds() -> None:
    engine = _FakeEngine([
        _FakeResult(status="error", error_code="storage_error"),
        _FakeResult(status="accepted"),
    ])

    ok = await publish_idle_wakeup(
        nats=object(),
        resource_store=object(),
        state_store=object(),
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_1",
        headers={},
        logger=logging.getLogger("test"),
        retry_count=2,
        retry_delay=0.0,
        l0_engine=engine,
    )

    assert ok is True
    assert len(engine.calls) == 2
    assert all(call["headers"]["CG-Recursion-Depth"] == "0" for call in engine.calls)


@pytest.mark.asyncio
async def test_publish_idle_wakeup_returns_false_after_non_accepted_retries(caplog) -> None:
    engine = _FakeEngine([
        _FakeResult(status="error", error_code="storage_error"),
        _FakeResult(status="error", error_code="internal_error"),
    ])

    with caplog.at_level(logging.WARNING):
        ok = await publish_idle_wakeup(
            nats=object(),
            resource_store=object(),
            state_store=object(),
            project_id="proj_1",
            channel_id="public",
            agent_id="agent_1",
            headers={},
            logger=logging.getLogger("test"),
            retry_count=1,
            retry_delay=0.0,
            l0_engine=engine,
        )

    assert ok is False
    assert len(engine.calls) == 2
    assert "returned non-accepted status" in caplog.text


@pytest.mark.asyncio
async def test_publish_idle_wakeup_fallbacks_when_recursion_depth_invalid(caplog) -> None:
    engine = _FakeEngine([_FakeResult(status="accepted")])

    with caplog.at_level(logging.WARNING):
        ok = await publish_idle_wakeup(
            nats=object(),
            resource_store=object(),
            state_store=object(),
            project_id="proj_1",
            channel_id="public",
            agent_id="agent_1",
            headers={"CG-Recursion-Depth": "-1"},
            logger=logging.getLogger("test"),
            retry_count=0,
            retry_delay=0.0,
            l0_engine=engine,
        )

    assert ok is True
    assert len(engine.calls) == 1
    assert engine.calls[0]["headers"]["CG-Recursion-Depth"] == "0"
    assert "invalid recursion-depth header" in caplog.text
