from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.llm import LLMConfig, LLMRequest
from infra.llm_gateway import LLMService


def _fake_response() -> SimpleNamespace:
    msg = SimpleNamespace(content="ok", tool_calls=None, role="assistant")
    choice = SimpleNamespace(message=msg)
    return SimpleNamespace(choices=[choice], usage=None)


class _FakeStream:
    def __init__(self, chunks):
        self._chunks = iter(chunks)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._chunks)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


def _fake_stream_chunk(*, content: str | None = None, reasoning_content: str | None = None) -> SimpleNamespace:
    delta = SimpleNamespace(content=content, tool_calls=None, reasoning_content=reasoning_content)
    choice = SimpleNamespace(delta=delta)
    return SimpleNamespace(choices=[choice])


@pytest.mark.asyncio
async def test_config_tool_choice_overrides_default_auto_when_request_omits_tool_choice(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_acompletion(**params):
        captured.update(params)
        return _fake_response()

    import litellm

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)

    cfg_tool_choice = {"type": "function", "function": {"name": "add_numbers"}}
    cfg = LLMConfig(model="gpt-4o-mini", stream=False, tool_choice=cfg_tool_choice)
    req = LLMRequest(
        messages=[{"role": "user", "content": "hi"}],
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "add_numbers",
                    "description": "add two integers",
                    "parameters": {
                        "type": "object",
                        "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                        "required": ["a", "b"],
                    },
                },
            }
        ],
        config=cfg,
    )

    # Sanity: request did not explicitly set tool_choice, but value defaults to "auto".
    assert req.tool_choice == "auto"
    assert "tool_choice" not in req.model_fields_set

    svc = LLMService()
    await svc.completion(req)

    assert captured.get("tool_choice") == cfg_tool_choice


@pytest.mark.asyncio
async def test_request_tool_choice_explicit_wins_over_config(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_acompletion(**params):
        captured.update(params)
        return _fake_response()

    import litellm

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)

    cfg_tool_choice = {"type": "function", "function": {"name": "add_numbers"}}
    cfg = LLMConfig(model="gpt-4o-mini", stream=False, tool_choice=cfg_tool_choice)
    req = LLMRequest(
        messages=[{"role": "user", "content": "hi"}],
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "add_numbers",
                    "description": "add two integers",
                    "parameters": {
                        "type": "object",
                        "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                        "required": ["a", "b"],
                    },
                },
            }
        ],
        tool_choice="auto",
        config=cfg,
    )

    # Sanity: request explicitly set tool_choice.
    assert req.tool_choice == "auto"
    assert "tool_choice" in req.model_fields_set

    svc = LLMService()
    await svc.completion(req)

    assert captured.get("tool_choice") == "auto"


@pytest.mark.asyncio
async def test_non_stream_includes_reasoning_content(monkeypatch):
    async def fake_acompletion(**params):
        _ = params
        msg = SimpleNamespace(content="ok", tool_calls=None, role="assistant", reasoning_content="reasoning-final")
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=None)

    import litellm

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)

    req = LLMRequest(
        messages=[{"role": "user", "content": "hi"}],
        config=LLMConfig(model="gpt-4o-mini", stream=False),
    )

    svc = LLMService()
    resp = await svc.completion(req)

    assert resp.get("reasoning_content") == "reasoning-final"


@pytest.mark.asyncio
async def test_stream_aggregates_reasoning_content_from_delta(monkeypatch):
    async def fake_acompletion(**params):
        _ = params
        return _FakeStream(
            [
                _fake_stream_chunk(content="ok", reasoning_content="think-1"),
                _fake_stream_chunk(reasoning_content="think-2"),
            ]
        )

    def fake_stream_chunk_builder(_raw_chunks):
        msg = SimpleNamespace(content="ok", tool_calls=None, role="assistant", reasoning_content=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], usage=None)

    import litellm

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    monkeypatch.setattr(litellm, "stream_chunk_builder", fake_stream_chunk_builder)

    req = LLMRequest(
        messages=[{"role": "user", "content": "hi"}],
        config=LLMConfig(model="gpt-4o-mini", stream=True),
    )

    svc = LLMService()
    resp = await svc.completion(req)

    assert resp.get("reasoning_content") == "think-1think-2"


@pytest.mark.asyncio
async def test_worker_only_save_reasoning_content_not_forwarded_to_litellm(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_acompletion(**params):
        captured.update(params)
        return _fake_response()

    import litellm

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)

    req = LLMRequest(
        messages=[{"role": "user", "content": "hi"}],
        config=LLMConfig(
            model="gpt-4o-mini",
            stream=False,
            save_reasoning_content=True,
        ),
    )

    svc = LLMService()
    await svc.completion(req)

    assert "save_reasoning_content" not in captured
