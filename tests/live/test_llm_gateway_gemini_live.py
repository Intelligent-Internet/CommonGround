import json
import os

import pytest

from infra.llm_gateway import LLMService
from core.llm import LLMConfig, LLMRequest


@pytest.mark.live
@pytest.mark.asyncio
async def test_gemini_live_thought_signature_roundtrip():
    """
    Live test: calls Gemini (gemini-3-flash-preview) twice.
    1) First call should return tool_call with thought_signature.
    2) Second call echoes that signature back; should not 400.
    Skips if GEMINI_API_KEY missing.
    """
    if not os.getenv("GEMINI_API_KEY"):
        pytest.skip("GEMINI_API_KEY not set")

    svc = LLMService()

    base_cfg = dict(
        model="gemini-3-flash-preview",
        provider_options={
            "custom_llm_provider": "gemini",
            "allowed_openai_params": ["extra_body"],
            "extra_body": {"thinking_config": {"thinking_level": "HIGH"}},
        },
        stream=False,  # non-stream yields signature reliably
        temperature=1.0,
        max_tokens=128,
    )

    # First call: expect thought_signature inside tool_call
    req1 = LLMRequest(
        messages=[{"role": "user", "content": "Call add_numbers with 2 and 3, then wait."}],
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
        config=LLMConfig(**base_cfg),
    )

    resp1 = await svc.completion(req1)
    tool_calls = resp1.get("tool_calls") or []
    assert tool_calls, f"no tool_calls: {json.dumps(resp1, ensure_ascii=False, indent=2)}"
    tc = tool_calls[0]
    signature = (tc.get("provider_specific_fields") or {}).get("thought_signature") or (
        (tc.get("function") or {}).get("provider_specific_fields") or {}
    ).get("thought_signature")
    assert signature, f"no thought_signature in tool_call: {json.dumps(tc, ensure_ascii=False, indent=2)}"

    tool_call_id = tc.get("id")
    assert tool_call_id, "tool_call id missing"

    # Second call: echo signature back in assistant tool_call, include tool result
    messages = [
        {"role": "user", "content": "Call add_numbers with 2 and 3, then wait."},
        {
            "role": "assistant",
            "tool_calls": [
                {
                    "id": tool_call_id,
                    "type": "function",
                    "function": {
                        "name": "add_numbers",
                        "arguments": tc.get("function", {}).get("arguments", ""),
                        "provider_specific_fields": {"thought_signature": signature},
                    },
                }
            ],
        },
        {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": "5",
        },
    ]

    req2 = LLMRequest(messages=messages, config=LLMConfig(**base_cfg))
    resp2 = await svc.completion(req2)
    # Success criteria: no exception and response structure present (400 would have raised)
    assert isinstance(resp2, dict) and resp2.get("role") == "assistant"

    await _cleanup_clients()


@pytest.mark.live
@pytest.mark.asyncio
async def test_gemini_live_stream_signature():
    """
    Streaming path should surface thought_signature in tool_calls via stream_chunk_builder.
    """
    if not os.getenv("GEMINI_API_KEY"):
        pytest.skip("GEMINI_API_KEY not set")

    svc = LLMService()
    cfg = LLMConfig(
        model="gemini-3-flash-preview",
        provider_options={
            "custom_llm_provider": "gemini",
            "allowed_openai_params": ["extra_body"],
            "extra_body": {"thinking_config": {"thinking_level": "HIGH"}},
        },
        stream=True,
        temperature=1.0,
        max_tokens=128,
    )
    req = LLMRequest(
        messages=[{"role": "user", "content": "Use the tool to add 2 and 3."}],
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

    resp = await svc.completion(req)
    tcs = resp.get("tool_calls") or []
    assert tcs, f"no tool_calls in stream response: {json.dumps(resp, ensure_ascii=False, indent=2)}"
    tc0 = tcs[0]
    sig = (tc0.get("provider_specific_fields") or {}).get("thought_signature") or (
        (tc0.get("function") or {}).get("provider_specific_fields") or {}
    ).get("thought_signature")
    assert sig, f"no thought_signature in stream tool_call: {json.dumps(tc0, ensure_ascii=False, indent=2)}"

    await _cleanup_clients()


async def _cleanup_clients():
    """Close litellm/aiohttp sessions to avoid unclosed warnings in tests."""
    try:
        import litellm
        await litellm.close_litellm_async_clients()
    except Exception:
        pass
    try:
        import gc
        import aiohttp
        for obj in gc.get_objects():
            if isinstance(obj, aiohttp.ClientSession) and not obj.closed:
                await obj.close()
    except Exception:
        pass
