from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class LLMConfig(BaseModel):
    model: str = "gpt-3.5-turbo"
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    temperature: float = 1.0
    max_tokens: Optional[int] = None
    top_p: Optional[float] = None
    stop: Optional[List[str]] = None
    stream: bool = True
    stream_options: Optional[Dict[str, Any]] = None
    # Optional tool choice override for LLM requests using tools.
    # Supports provider-specific dicts (e.g., {"type": "function", "function": {"name": "..."}}).
    tool_choice: Optional[Any] = None
    # Worker-only switch.
    # If True, agent_worker persists `reasoning_content` into thought card metadata and
    # replays it via to_api on subsequent turns. This flag MUST NOT be forwarded to LiteLLM.
    save_reasoning_content: bool = True

    # Generic container for provider-specific settings (e.g., custom_llm_provider, extra_body, etc.)
    # These are passed directly to litellm.acompletion as kwargs.
    provider_options: Optional[Dict[str, Any]] = None


class LLMRequest(BaseModel):
    messages: List[Dict[str, Any]]
    tools: Optional[List[Dict[str, Any]]] = None
    # Allow dict tool_choice (e.g., {"type":"function","function":{"name":"..."}}) or string.
    tool_choice: Optional[Any] = "auto"
    config: LLMConfig
