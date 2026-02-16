from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict

import pytest

from infra.observability import otel


class _DummySpan:
    def __init__(self) -> None:
        self.attrs: Dict[str, Any] = {}
        self.recorded_exceptions: list[BaseException] = []
        self.status: Any = None

    def set_attribute(self, key: str, value: Any) -> None:
        self.attrs[str(key)] = value

    def record_exception(self, exc: BaseException) -> None:
        self.recorded_exceptions.append(exc)

    def set_status(self, status: Any) -> None:
        self.status = status


@pytest.mark.asyncio
async def test_traced_async_extracts_headers_and_injects_span(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: Dict[str, Any] = {}
    dummy_span = _DummySpan()

    @contextmanager
    def _fake_start_span(tracer: Any, name: str, **kwargs: Any):
        captured["tracer"] = tracer
        captured["name"] = name
        captured.update(kwargs)
        yield dummy_span

    monkeypatch.setattr(otel, "start_span", _fake_start_span)

    @otel.traced(
        tracer="t_async",
        name="demo.async",
        headers_arg="headers",
        span_arg="_span",
        attributes_getter=lambda args: {"cg.subject": str(args.get("subject"))},
        include_links=True,
        mark_error_on_exception=True,
    )
    async def _handler(subject: str, headers: Dict[str, str], _span: Any = None) -> Any:
        _span.set_attribute("cg.handler", "ok")
        return _span

    result = await _handler("s.1", {"traceparent": "tp"})
    assert result is dummy_span
    assert captured["tracer"] == "t_async"
    assert captured["name"] == "demo.async"
    assert captured["headers"] == {"traceparent": "tp"}
    assert captured["attributes"] == {"cg.subject": "s.1"}
    assert captured["include_links"] is True
    assert captured["mark_error_on_exception"] is True
    assert dummy_span.attrs["cg.handler"] == "ok"


def test_traced_sync_uses_named_context_arg(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: Dict[str, Any] = {}
    dummy_span = _DummySpan()

    @contextmanager
    def _fake_start_span(tracer: Any, name: str, **kwargs: Any):
        captured["tracer"] = tracer
        captured["name"] = name
        captured.update(kwargs)
        yield dummy_span

    monkeypatch.setattr(otel, "start_span", _fake_start_span)

    @otel.traced(
        tracer="t_sync",
        name="demo.sync",
        context_arg="ctx",
    )
    def _handler(ctx: Any) -> str:
        return "ok"

    out = _handler("ctx-value")
    assert out == "ok"
    assert captured["tracer"] == "t_sync"
    assert captured["name"] == "demo.sync"
    assert captured["context"] == "ctx-value"


def test_traced_supports_dynamic_name(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: Dict[str, Any] = {}
    dummy_span = _DummySpan()

    @contextmanager
    def _fake_start_span(tracer: Any, name: str, **kwargs: Any):
        captured["tracer"] = tracer
        captured["name"] = name
        captured.update(kwargs)
        yield dummy_span

    monkeypatch.setattr(otel, "start_span", _fake_start_span)

    @otel.traced(
        tracer="t_dyn",
        name_getter=lambda args: f"demo.{args.get('cmd')}",
    )
    def _handler(*, cmd: str) -> None:
        return None

    _handler(cmd="run")
    assert captured["tracer"] == "t_dyn"
    assert captured["name"] == "demo.run"


def test_mark_span_error_compacts_pydantic_validation_chain() -> None:
    span = _DummySpan()

    class ValidationError(Exception):
        __module__ = "pydantic_core._pydantic_core"

    root = ValidationError("1 validation error for DemoModel")
    wrapper = RuntimeError("bad request")
    wrapper.__cause__ = root

    assert otel.is_compact_exception(wrapper) is True
    otel.mark_span_error(span, wrapper)

    assert span.recorded_exceptions == []
    assert span.attrs["cg.error.type"] == "RuntimeError"
    assert span.attrs["cg.error.message"] == "bad request"


def test_mark_span_error_records_non_compact_exception() -> None:
    span = _DummySpan()
    exc = RuntimeError("boom")

    assert otel.is_compact_exception(exc) is False
    otel.mark_span_error(span, exc)

    assert len(span.recorded_exceptions) == 1
    assert span.recorded_exceptions[0] is exc
    assert span.attrs["cg.error.type"] == "RuntimeError"
    assert span.attrs["cg.error.message"] == "boom"
