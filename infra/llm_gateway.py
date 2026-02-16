import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

import litellm
#litellm._step_on_debug()
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from core.llm import LLMRequest
from infra.llm.tool_specs import build_tool_specs as build_llm_tool_specs

logger = logging.getLogger(__name__)


MockChatHandler = Callable[[List[Dict[str, Any]], str], Awaitable[Dict[str, Any]]]


def _sanitize_tool_messages(messages: List[Dict[str, Any]], *, model: str) -> List[Dict[str, Any]]:
    # Gemini requires tool responses to immediately follow the assistant tool_calls.
    if "gemini" not in (model or "").lower():
        return messages
    last_idx = None
    last_tool_calls: List[Dict[str, Any]] = []
    for idx in range(len(messages) - 1, -1, -1):
        msg = messages[idx]
        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            last_idx = idx
            last_tool_calls = list(msg.get("tool_calls") or [])
            break
    if last_idx is None:
        return messages
    allowed_ids = {tc.get("id") for tc in last_tool_calls if isinstance(tc, dict) and tc.get("id")}
    if not allowed_ids:
        return messages
    sanitized: List[Dict[str, Any]] = []
    for idx, msg in enumerate(messages):
        if msg.get("role") != "tool":
            sanitized.append(msg)
            continue
        if idx < last_idx:
            continue
        tool_call_id = msg.get("tool_call_id")
        if tool_call_id and tool_call_id in allowed_ids:
            sanitized.append(msg)
    return sanitized


class LLMService:
    def __init__(self) -> None:
        pass

    @staticmethod
    def _normalize_reasoning_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            parts: List[str] = []
            for item in value:
                if isinstance(item, str):
                    parts.append(item)
                    continue
                if isinstance(item, dict):
                    text = item.get("text")
                    if text is None:
                        text = item.get("content")
                    if text is not None:
                        parts.append(str(text))
                    continue
                text = getattr(item, "text", None)
                if text is None:
                    text = getattr(item, "content", None)
                if text is not None:
                    parts.append(str(text))
            return "".join(parts) if parts else None
        if isinstance(value, dict):
            text = value.get("text")
            if text is None:
                text = value.get("content")
            if text is not None:
                return str(text)
            return None
        return str(value)

    @classmethod
    def _extract_reasoning_content(cls, obj: Any) -> Optional[str]:
        if obj is None:
            return None
        value = getattr(obj, "reasoning_content", None)
        text = cls._normalize_reasoning_text(value)
        if text:
            return text
        if isinstance(obj, dict):
            text = cls._normalize_reasoning_text(obj.get("reasoning_content"))
            if text:
                return text
        # Some providers expose reasoning under a generic `reasoning` field.
        value = getattr(obj, "reasoning", None)
        text = cls._normalize_reasoning_text(value)
        if text:
            return text
        if isinstance(obj, dict):
            text = cls._normalize_reasoning_text(obj.get("reasoning"))
            if text:
                return text
        return None

    @staticmethod
    def _usage_to_dict(value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if isinstance(value, dict):
            return dict(value)
        try:
            return json.loads(json.dumps(value, default=lambda o: getattr(o, "__dict__", str(o))))
        except Exception:
            return None

    @classmethod
    def _extract_usage(cls, obj: Any) -> Optional[Dict[str, Any]]:
        if obj is None:
            return None
        usage = getattr(obj, "usage", None)
        if usage is None and isinstance(obj, dict):
            usage = obj.get("usage")
        return cls._usage_to_dict(usage)

    @staticmethod
    def _extract_response_cost(obj: Any) -> Optional[float]:
        if obj is None:
            return None
        hidden = getattr(obj, "_hidden_params", None)
        if isinstance(hidden, dict):
            value = hidden.get("response_cost") if "response_cost" in hidden else hidden.get("cost")
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
        if isinstance(obj, dict):
            value = obj.get("response_cost") if "response_cost" in obj else obj.get("cost")
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
        return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (litellm.RateLimitError, litellm.APIConnectionError, litellm.Timeout)
        ),
    )
    async def completion(
        self, request: LLMRequest, on_chunk: Optional[Callable[[str], None]] = None
    ) -> Dict[str, Any]:
        """
        Execute LLM completion. Handles streaming and returns the final aggregated response.
        """
        kwargs = request.config.model_dump(exclude_none=True)
        config_tool_choice = kwargs.pop("tool_choice", None)
        # Worker-only setting; never forward to provider SDK.
        kwargs.pop("save_reasoning_content", None)
        model = kwargs.pop("model")
        stream = kwargs.pop("stream", False)

        messages = _sanitize_tool_messages(request.messages, model=model)
        params = {
            "model": model,
            "messages": messages,
            "stream": stream,
            **kwargs,
        }

        if request.tools:
            params["tools"] = request.tools
            # `LLMRequest.tool_choice` defaults to "auto" (truthy). If we use `or`, a config-level
            # override would never take effect unless callers explicitly set request.tool_choice to falsy.
            # Instead, treat config.tool_choice as the default when the request did not explicitly set it.
            tool_choice = request.tool_choice
            if config_tool_choice is not None and "tool_choice" not in request.model_fields_set:
                tool_choice = config_tool_choice
            if tool_choice:
                params["tool_choice"] = tool_choice

        # If caller set provider_options (e.g., custom_llm_provider, extra_body), pass them through
        if kwargs.get("provider_options"):
            params.update(kwargs.pop("provider_options"))

        logger.info("Calling LLM: %s (Stream: %s)", model, stream)

        if logger.isEnabledFor(logging.INFO):
            logger.info("LLM Request Messages:\n%s", json.dumps(messages, indent=2, ensure_ascii=False))
            if request.tools:
                logger.info("LLM Request Tools:\n%s", json.dumps(request.tools, indent=2, ensure_ascii=False))

        if stream and "stream_options" not in params:
            params["stream_options"] = {"include_usage": True}

        if stream:
            return await self._stream_handler(params, on_chunk)
        response = await litellm.acompletion(**params)
        formatted = self._format_response(response)
        if logger.isEnabledFor(logging.INFO):
            logger.info("LLM Response:\n%s", json.dumps(formatted, indent=2, ensure_ascii=False))
        return formatted

    async def _stream_handler(
        self, params: Dict[str, Any], on_chunk: Optional[Callable[[str], None]]
    ) -> Dict[str, Any]:
        response_stream = await litellm.acompletion(**params)

        full_content: List[str] = []
        full_reasoning_content: List[str] = []
        tool_calls: List[Dict[str, Any]] = []
        raw_chunks: List[Any] = []  # keep original chunks for stream_chunk_builder
        stream_usage: Optional[Dict[str, Any]] = None
        stream_cost: Optional[float] = None

        def _merge_tool_call(index: int, delta_call: Any) -> None:
            # Ensure list length
            while len(tool_calls) <= index:
                tool_calls.append({"id": "", "type": "function", "function": {"name": "", "arguments": ""}})

            current = tool_calls[index]

            # Convert to dict; litellm objects may be Pydantic or SimpleNamespace
            if hasattr(delta_call, "model_dump"):
                delta_dict = delta_call.model_dump()
            elif isinstance(delta_call, dict):
                delta_dict = delta_call
            else:
                try:
                    delta_dict = json.loads(json.dumps(delta_call, default=lambda o: getattr(o, "__dict__", str(o))))
                except Exception:
                    delta_dict = {}

            for key, val in delta_dict.items():
                if key == "function":
                    fn = current.setdefault("function", {})
                    if isinstance(val, dict):
                        if val.get("name"):
                            fn["name"] = fn.get("name", "") + val["name"]
                        if val.get("arguments"):
                            fn["arguments"] = fn.get("arguments", "") + val["arguments"]
                        # copy any other nested fields (e.g., provider_specific_fields)
                        for fk, fv in val.items():
                            if fk not in {"name", "arguments"} and fv is not None:
                                fn[fk] = fv
                    else:
                        fn.update({"value": val})
                elif key == "id" and val:
                    current["id"] = val
                elif val is not None:
                    # Preserve provider_specific_fields/type/etc.
                    current[key] = val

        async for chunk in response_stream:
            raw_chunks.append(chunk)
            usage_candidate = self._extract_usage(chunk)
            if usage_candidate:
                stream_usage = usage_candidate
            cost_candidate = self._extract_response_cost(chunk)
            if cost_candidate is not None:
                stream_cost = cost_candidate
            choices = getattr(chunk, "choices", None)
            if not choices:
                # e.g. usage-only chunk (choices=[]) when include_usage is enabled
                continue
            delta = choices[0].delta
            reasoning_chunk = self._extract_reasoning_content(delta)
            if reasoning_chunk:
                full_reasoning_content.append(reasoning_chunk)

            if delta.content:
                full_content.append(delta.content)
                if on_chunk:
                    try:
                        if asyncio.iscoroutinefunction(on_chunk):
                            await on_chunk(delta.content)
                        else:
                            on_chunk(delta.content)
                    except Exception as exc:  # noqa: BLE001
                        logger.error("Error in on_chunk callback: %s", exc)

            if delta.tool_calls:
                for tc_chunk in delta.tool_calls:
                    try:
                        index = tc_chunk.index
                    except Exception:
                        index = getattr(tc_chunk, "index", 0) if tc_chunk else 0
                    _merge_tool_call(index, tc_chunk)

        aggregated_content = "".join(full_content)
        aggregated_reasoning_content = "".join(full_reasoning_content)
        # Use litellm's builder to reconstruct final message (captures provider_specific_fields like thought_signature)
        try:
            built = litellm.stream_chunk_builder(raw_chunks)
            msg = built.choices[0].message
            reasoning_content = self._extract_reasoning_content(msg) or aggregated_reasoning_content
            result = {
                "content": msg.content if msg.content is not None else aggregated_content,
                "tool_calls": [tc.model_dump() for tc in msg.tool_calls] if getattr(msg, "tool_calls", None) else (tool_calls or None),
                "role": msg.role,
            }
            if reasoning_content:
                result["reasoning_content"] = reasoning_content
        except Exception as exc:  # noqa: BLE001
            logger.error("stream_chunk_builder failed, falling back to aggregated chunks: %s", exc)
            built = None
            result = {
                "content": aggregated_content,
                "tool_calls": tool_calls if tool_calls else None,
                "role": "assistant",
            }
            if aggregated_reasoning_content:
                result["reasoning_content"] = aggregated_reasoning_content
        finally:
            # Defensive cleanup to avoid aiohttp unclosed session warnings during ad-hoc calls
            try:
                closer = getattr(response_stream, "aclose", None) or getattr(response_stream, "close", None)
                if closer:
                    if asyncio.iscoroutinefunction(closer):
                        await closer()
                    else:
                        closer()
            except Exception:
                pass
        if logger.isEnabledFor(logging.INFO):
            logger.info("LLM Streamed Response:\n%s", json.dumps(result, indent=2, ensure_ascii=False))

        usage = stream_usage or (self._extract_usage(built) if built is not None else None)
        response_cost = stream_cost or (self._extract_response_cost(built) if built is not None else None)
        if response_cost is None and built is not None:
            completion_cost = getattr(litellm, "completion_cost", None)
            if completion_cost:
                try:
                    response_cost = completion_cost(completion_response=built)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("completion_cost failed: %s", exc)
        if response_cost is None and isinstance(usage, dict):
            cost_value = usage.get("cost") if "cost" in usage else usage.get("response_cost")
            if cost_value is not None:
                try:
                    response_cost = float(cost_value)
                except (TypeError, ValueError):
                    pass

        if usage is not None:
            result["usage"] = usage
        if response_cost is not None:
            result["response_cost"] = response_cost

        return result

    def _format_response(self, response) -> Dict[str, Any]:
        message = response.choices[0].message
        reasoning_content = self._extract_reasoning_content(message)
        result = {
            "content": message.content,
            "tool_calls": [tc.model_dump() for tc in message.tool_calls] if message.tool_calls else None,
            "role": message.role,
        }
        if reasoning_content:
            result["reasoning_content"] = reasoning_content
        usage = self._extract_usage(response)
        response_cost = self._extract_response_cost(response)
        if response_cost is None:
            completion_cost = getattr(litellm, "completion_cost", None)
            if completion_cost:
                try:
                    response_cost = completion_cost(completion_response=response)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("completion_cost failed: %s", exc)
        if response_cost is None and isinstance(usage, dict):
            cost_value = usage.get("cost") if "cost" in usage else usage.get("response_cost")
            if cost_value is not None:
                try:
                    response_cost = float(cost_value)
                except (TypeError, ValueError):
                    pass
        if usage is not None:
            result["usage"] = usage
        if response_cost is not None:
            result["response_cost"] = response_cost
        return result


class LLMWrapper:
    def __init__(self, service: LLMService, mock_chat: Optional[MockChatHandler] = None):
        self.service = service
        self._mock_chat = mock_chat

    @staticmethod
    def build_tool_specs(tool_defs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return build_llm_tool_specs(tool_defs)

    async def chat(
        self, request: LLMRequest, on_chunk: Optional[Callable[[str], Awaitable[None]]] = None
    ) -> Dict[str, Any]:
        """Call LLM."""
        if self._mock_chat and request.config.model and request.config.model.startswith("mock"):
            return await self._mock_chat(request.messages, request.config.model)

        return await self.service.completion(request, on_chunk=on_chunk)
