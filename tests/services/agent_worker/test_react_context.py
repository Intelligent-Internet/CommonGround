from types import SimpleNamespace

from core.llm import LLMConfig
from services.agent_worker.react_context import (
    build_card_metadata,
    build_stream_start_metadata,
    normalize_tool_args,
    sanitize_provider_options,
)


def test_normalize_tool_args_parses_json_and_updates_tool_call() -> None:
    tool_call = {"function": {"arguments": "{\"a\": 1}"}}
    parsed = normalize_tool_args(tool_call)
    assert parsed == {"a": 1}
    assert tool_call["function"]["arguments"] == {"a": 1}


def test_sanitize_provider_options_removes_sensitive_keys() -> None:
    cleaned = sanitize_provider_options(
        {"api_key": "x", "token": "y", "custom_llm_provider": "openai"}
    )
    assert cleaned == {"custom_llm_provider": "openai"}


def test_build_stream_start_metadata_redacts_api_keys() -> None:
    item = SimpleNamespace(agent_id="agent_1")
    profile = SimpleNamespace(
        card_id="profile_1",
        name="Planner",
        display_name="Planner Agent",
        tags=["core"],
        worker_target="agent_worker",
        llm_config=LLMConfig(
            model="gpt-4o-mini",
            api_key="x",
            provider_options={"api_key": "y", "custom_llm_provider": "openai"},
        ),
    )
    metadata = build_stream_start_metadata(item=item, profile=profile)
    assert metadata["agent_id"] == "agent_1"
    assert metadata["profile_id"] == "profile_1"
    assert "api_key" not in metadata["llm_config"]
    assert metadata["llm_config"]["provider_options"] == {"custom_llm_provider": "openai"}


def test_build_card_metadata_includes_trace_parent_and_extra() -> None:
    item = SimpleNamespace(agent_turn_id="turn_1", trace_id="trace_1", parent_step_id="parent_1")
    metadata = build_card_metadata(
        item=item,
        step_id="step_1",
        role="assistant",
        tool_call_id="call_1",
        extra={"k": "v"},
    )
    assert metadata["agent_turn_id"] == "turn_1"
    assert metadata["step_id"] == "step_1"
    assert metadata["role"] == "assistant"
    assert metadata["tool_call_id"] == "call_1"
    assert metadata["trace_id"] == "trace_1"
    assert metadata["parent_step_id"] == "parent_1"
    assert metadata["k"] == "v"
