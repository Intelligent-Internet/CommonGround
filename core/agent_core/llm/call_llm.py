import os
import json
import uuid
from typing import List, Dict, Any, Optional
import logging
import asyncio # Add asyncio module
# Import LiteLLM library
import litellm
from litellm.exceptions import (
    AuthenticationError,
    BadRequestError,
    ContextWindowExceededError,
    RateLimitError,
    Timeout,
    APIConnectionError,
    ServiceUnavailableError,
    InternalServerError,
    APIError
)
import json_repair
from .utils import extract_tool_calls_from_content


# Learn more about calling the LLM: https://the-pocket.github.io/PocketFlow/utility_function/llm.html

logger = logging.getLogger(__name__)

# --- START: Added robust_retry_with_backoff decorator ---
import time
import random

def robust_retry_with_backoff(
    max_retries: int = 2,
    initial_delay: float = 5.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0
):
    """
    A robust retry decorator that implements an exponential backoff strategy with jitter.
    It only retries specific, recoverable litellm exceptions.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (
                    RateLimitError,
                    Timeout,
                    APIConnectionError,
                    ServiceUnavailableError,
                    InternalServerError
                ) as e:
                    last_exception = e
                    if attempt >= max_retries:
                        logger.error("llm_retry_exhausted", extra={"max_retries": max_retries + 1, "final_error_type": type(e).__name__, "final_error_message": str(e)}, exc_info=True)
                        break # Break the loop, the exception will be re-thrown after the loop

                    # Add jitter to avoid the "thundering herd" effect
                    jitter = delay * random.uniform(0.1, 0.5)
                    wait_time = min(delay + jitter, max_delay)
                    
                    logger.warning("llm_recoverable_error_retry", extra={"error_type": type(e).__name__, "error_message": str(e), "attempt": attempt + 1, "max_attempts": max_retries + 1, "wait_time": wait_time})
                    await asyncio.sleep(wait_time)
                    delay *= backoff_factor
                except (AuthenticationError, BadRequestError, ContextWindowExceededError, APIError) as e:
                    # For unrecoverable errors, fail and raise immediately
                    logger.error("llm_unrecoverable_error", extra={"error_type": type(e).__name__, "error_message": str(e)}, exc_info=True)
                    raise e
                except Exception as e:
                    # Catch all other unknown exceptions and treat them as unrecoverable
                    logger.error("llm_unexpected_error", extra={"error_type": type(e).__name__, "error_message": str(e)}, exc_info=True)
                    raise e
            
            # If all retries have failed, re-throw the last exception
            if last_exception:
                raise last_exception
            
            # This is a path that should theoretically not be reached, but is included for code completeness
            raise RuntimeError("LLM call failed after all retries without a specific exception.")

        return wrapper
    return decorator
# --- END: Added robust_retry_with_backoff decorator ---


# Define a custom exception to make our intent clearer
class FunctionCallErrorException(Exception):
    """A special exception used to force a retry of the LLM call at the application logic level."""
    pass


def estimate_prompt_tokens(
    model: str,
    text: Optional[str] = None,
    messages: Optional[List[Dict]] = None,
    system_prompt: Optional[str] = None,
    # New parameter: allow passing the full LLM config to find override values
    llm_config_for_tokenizer: Optional[Dict[str, Any]] = None
) -> int:
    """
    Estimates the number of tokens for a given input. Can accept a single text string or a list of messages.
    """
    # Prioritize using the model name specified for the tokenizer
    model_for_counting = model
    if llm_config_for_tokenizer and llm_config_for_tokenizer.get("litellm_token_counter_model"):
        model_for_counting = llm_config_for_tokenizer["litellm_token_counter_model"]
        logger.debug("token_counting_model_override", extra={"model_for_counting": model_for_counting, "override_source": "litellm_token_counter_model"})
    elif not model:
        logger.warning("token_estimation_no_model", extra={"model_provided": bool(model), "override_found": False, "return_value": 0})
        return 0

    if text is not None and messages is not None:
        raise ValueError("Provide either 'text' or 'messages' to estimate_prompt_tokens, not both.")

    messages_for_calc: List[Dict] = []

    if system_prompt:
        messages_for_calc.append({"role": "system", "content": system_prompt})

    if text is not None:
        messages_for_calc.append({"role": "user", "content": text})
    elif messages is not None:
        messages_for_calc.extend(messages)
    
    if not messages_for_calc:
        return 0

    try:
        return litellm.token_counter(model=model_for_counting, messages=messages_for_calc)
    except Exception as e:
        logger.warning("token_estimation_failed", extra={"model_for_counting": model_for_counting, "error_message": str(e), "return_value": 0})
        return 0

class LLMResponseAggregator:
    """
    Helper class to aggregate streaming LLM responses.
    This can be expanded or moved to a separate utility file if it grows complex.
    """
    def __init__(self, agent_id: str, parent_agent_id: Optional[str], events: Optional[Any], run_id: Optional[str], stream_id: str, llm_model_id: Optional[str], associated_task_nums_for_event: Optional[List[int]] = None, module_id_for_event: Optional[str] = None, dispatch_id_for_event: Optional[str] = None):
        self.agent_id = agent_id
        self.parent_agent_id = parent_agent_id
        self.events = events
        self.run_id = run_id
        self.associated_task_nums_for_event = associated_task_nums_for_event
        self.module_id_for_event = module_id_for_event
        self.dispatch_id_for_event = dispatch_id_for_event
        self.stream_id = stream_id
        self.llm_model_id = llm_model_id

        self.full_content = ""
        self.full_reasoning_content = ""
        self.current_tool_call_chunks: Dict[int, Dict] = {}
        self.raw_chunks: List[Any] = [] # To store raw chunks for litellm.stream_chunk_builder
        self.model_id_used: Optional[str] = None # To store the model ID from the response
        self.actual_usage: Optional[Dict[str, int]] = None # <--- Added

    def _get_contextual_data_for_event(self) -> Optional[Dict]:
        """Builds the contextual data dictionary for events."""
        contextual_data = {}
        if self.associated_task_nums_for_event is not None: # Legacy
            contextual_data["associated_task_nums"] = self.associated_task_nums_for_event
        if self.module_id_for_event:
            contextual_data["module_id"] = self.module_id_for_event
        if self.dispatch_id_for_event:
            contextual_data["dispatch_id"] = self.dispatch_id_for_event
        return contextual_data if contextual_data else None

    async def process_chunk(self, chunk: Any):
        self.raw_chunks.append(chunk)
        if os.environ.get("DEBUG_LLM", "0") == "1":
            logger.debug("llm_chunk_received", extra={"agent_id": self.agent_id, "chunk_data": str(chunk)})

        # +++ Added: Capture usage chunk +++
        if hasattr(chunk, "usage") and chunk.usage is not None:
            try:
                if hasattr(chunk.usage, "dict"):
                    self.actual_usage = chunk.usage.dict()
                else:
                    self.actual_usage = dict(chunk.usage)
                logger.debug("actual_token_usage_captured", extra={"agent_id": self.agent_id, "usage_data": self.actual_usage})
            except Exception as e_usage:
                logger.error("usage_chunk_processing_failed", extra={"agent_id": self.agent_id, "error_message": str(e_usage)}, exc_info=True)

        if not hasattr(chunk, "choices") or not chunk.choices:
            logger.debug("llm_chunk_no_choices", extra={"agent_id": self.agent_id})
            return
        
        # Store model_id if available in the first chunk (or any chunk)
        if not self.model_id_used and hasattr(chunk, "model") and chunk.model:
            self.model_id_used = chunk.model
            logger.debug("model_id_captured", extra={"agent_id": self.agent_id, "model_id_used": self.model_id_used})


        delta = chunk.choices[0].delta
        logger.debug("llm_delta_received", extra={"agent_id": self.agent_id, "delta_data": str(delta)})

        if hasattr(delta, "reasoning_content") and delta.reasoning_content is not None:
            # print(delta.reasoning_content, end="", flush=True) # Direct print removed
            self.full_reasoning_content += delta.reasoning_content
            if self.events:
                await self.events.emit_llm_chunk(
                    run_id=self.run_id, agent_id=self.agent_id, parent_agent_id=self.parent_agent_id, chunk_type="reasoning_content",
                    content=delta.reasoning_content, stream_id=self.stream_id, llm_id=self.llm_model_id,
                    contextual_data=self._get_contextual_data_for_event()
                )

        if hasattr(delta, "content") and delta.content is not None:
            self.full_content += delta.content
            # ==================== START OF FIX ====================
            if "<tool_call>" in self.full_content or "<tool_code>" in self.full_content:
                raise FunctionCallErrorException("Detected '<tool_call>' or '<tool_code>' in stream, forcing retry.")
            # ===================== END OF FIX =====================
            if self.events:
                await self.events.emit_llm_chunk(
                    run_id=self.run_id, agent_id=self.agent_id, parent_agent_id=self.parent_agent_id, chunk_type="content",
                    content=delta.content, stream_id=self.stream_id, llm_id=self.llm_model_id,
                    contextual_data=self._get_contextual_data_for_event()
                )

        if hasattr(delta, "tool_calls") and delta.tool_calls:
            for tc_chunk in delta.tool_calls:
                index = tc_chunk.index if hasattr(tc_chunk, "index") else 0
                if index not in self.current_tool_call_chunks:
                    self.current_tool_call_chunks[index] = {"id": None, "type": "function", "function": {"name": "", "arguments": ""}}
                
                if hasattr(tc_chunk, "id") and tc_chunk.id:
                    self.current_tool_call_chunks[index]["id"] = tc_chunk.id
                
                if hasattr(tc_chunk, "function"):
                    if hasattr(tc_chunk.function, "name") and tc_chunk.function.name:
                        self.current_tool_call_chunks[index]["function"]["name"] += tc_chunk.function.name
                        if self.events:
                            await self.events.emit_llm_chunk(
                                run_id=self.run_id, agent_id=self.agent_id, parent_agent_id=self.parent_agent_id, chunk_type="tool_name",
                                content=tc_chunk.function.name, stream_id=self.stream_id, llm_id=self.llm_model_id,
                                contextual_data=self._get_contextual_data_for_event()
                            )
                    if hasattr(tc_chunk.function, "arguments") and tc_chunk.function.arguments:
                        self.current_tool_call_chunks[index]["function"]["arguments"] += tc_chunk.function.arguments
                        if self.events:
                            await self.events.emit_llm_chunk(
                                run_id=self.run_id, agent_id=self.agent_id, parent_agent_id=self.parent_agent_id, chunk_type="tool_args",
                                content=tc_chunk.function.arguments, # Keep existing decode
                                stream_id=self.stream_id, llm_id=self.llm_model_id,
                                contextual_data=self._get_contextual_data_for_event()
                            )
    
    def get_aggregated_response(self, messages_for_llm: List[Dict]) -> Dict:
        # Reconstruct full messages if needed by litellm or for logging
        # full_messages_history = litellm.stream_chunk_builder(self.raw_chunks, messages=messages_for_llm)
        # logger.debug(f"Agent {self.agent_id}: Full message history after stream_chunk_builder: {full_messages_history}")


        tool_calls_list = list(self.current_tool_call_chunks.values())
        for i, tool_call in enumerate(tool_calls_list):
            if "arguments" in tool_call["function"]:
                try:
                    # Attempt to repair JSON, ensure_ascii=False to handle unicode properly
                    fixed_args = json_repair.repair_json(tool_call["function"]["arguments"], ensure_ascii=False)
                    tool_calls_list[i]["function"]["arguments"] = fixed_args
                except Exception as e:
                    logger.debug("tool_args_json_repair_failed", extra={"agent_id": self.agent_id, "error_message": str(e), "original_args": tool_call['function']['arguments']})
                    # Keeping original potentially broken args if repair fails. Could set to "{}" as an alternative.

        # Fallback parsing is no longer attempted - if these tags are detected, a retry should have been triggered in process_chunk.
        # The original fallback logic has been removed as we now treat these cases as errors that require a retry.
        
        logger.debug("llm_response_aggregated", extra={"agent_id": self.agent_id, "content_length": len(self.full_content), "tool_calls_count": len(tool_calls_list), "reasoning_length": len(self.full_reasoning_content)})
        
        # If model_id_used was not found in chunks, try to get it from the final response object (if available)
        # This part depends on how call_litellm_acompletion returns the final object after stream.
        # For now, we rely on chunk.model.

        return {
            "reasoning": self.full_reasoning_content,
            "content": self.full_content,
            "tool_calls": tool_calls_list,
            "model_id_used": self.model_id_used, # Include the model ID used for this call
            "actual_usage": self.actual_usage # <--- Add to the return value
        }

# Apply the new decorator
@robust_retry_with_backoff(max_retries=2, initial_delay=5, max_delay=60)
async def call_litellm_acompletion(
    messages: List[Dict[str, Any]],
    llm_config: Dict[str, Any],
    stream: bool = True,
    system_prompt_content: Optional[str] = None,
    api_tools_list: Optional[List[Dict]] = None,
    tool_choice: Optional[str] = None,
    stream_id: Optional[str] = None,
    events: Optional[Any] = None,
    agent_id_for_event: Optional[str] = None,
    run_id_for_event: Optional[str] = None,
    contextual_data_for_event: Optional[Dict] = None,
    run_context: Optional[Dict] = None, # <--- Added run_context parameter
    **kwargs
) -> Dict[str, Any]:
    
    # ==================== START OF REFACTOR ====================
    # 1. Introduce application-level retry loop
    app_level_max_retries = llm_config.get("max_retries", 2) # Get retry count from config
    last_app_level_exception = None
    
    final_messages = list(messages)
    # Only process system_prompt_content on the first attempt
    if system_prompt_content:
        if final_messages and final_messages[0].get("role") == "system":
            final_messages[0]["content"] = system_prompt_content
        else:
            final_messages.insert(0, {"role": "system", "content": system_prompt_content})

    for attempt in range(app_level_max_retries + 1):
        # Move stream_id generation inside the loop to ensure a new ID for each retry
        current_stream_id = stream_id or str(uuid.uuid4())
        
        try:
            model_name = llm_config.get("model")
            if not model_name:
                raise ValueError("The 'model' key is missing in the provided llm_config.")

            base_params = {
                **llm_config,
                "messages": final_messages,
                "stream": True,
                **kwargs
            }
            
            if api_tools_list:
                base_params["tools"] = api_tools_list
                if tool_choice:
                    base_params["tool_choice"] = tool_choice
            
            if stream:
                base_params.setdefault("stream_options", {})["include_usage"] = True

            base_params = {k: v for k, v in base_params.items() if v is not None}
            
            params_for_this_attempt = {**base_params, "stream_id": current_stream_id}

            if events and agent_id_for_event and run_id_for_event:
                await events.emit_llm_stream_started(
                    run_id=run_id_for_event, agent_id=agent_id_for_event, 
                    parent_agent_id=kwargs.get('parent_agent_id'), stream_id=current_stream_id, 
                    llm_id=model_name, contextual_data=contextual_data_for_event
                )
                params_for_event = json.loads(json.dumps(params_for_this_attempt, default=str))
                await events.emit_llm_request_params(
                    run_id=run_id_for_event, agent_id=agent_id_for_event, stream_id=current_stream_id,
                    llm_id=model_name, params=params_for_event, contextual_data=contextual_data_for_event
                )
            
            logger.info("litellm_call_attempt", extra={"attempt": attempt + 1, "max_attempts": app_level_max_retries + 1, "model_name": model_name, "stream_id": current_stream_id})
            
            FILTERED_KEYS = ["stream_id", "parent_agent_id", "wait_seconds_on_retry", "max_retries"]
            params_for_litellm = {k: v for k, v in params_for_this_attempt.items() if k not in FILTERED_KEYS}
            
            response_aggregator = LLMResponseAggregator(
                agent_id=agent_id_for_event,
                parent_agent_id=kwargs.get('parent_agent_id'),
                events=events,
                run_id=run_id_for_event,
                stream_id=current_stream_id,
                llm_model_id=model_name,
                associated_task_nums_for_event=contextual_data_for_event.get("associated_task_nums") if contextual_data_for_event else None,
                module_id_for_event=contextual_data_for_event.get("module_id") if contextual_data_for_event else None,
                dispatch_id_for_event=contextual_data_for_event.get("dispatch_id") if contextual_data_for_event else None
            )

            llm_response_stream = await litellm.acompletion(**params_for_litellm)
            
            async for chunk in llm_response_stream:
                await response_aggregator.process_chunk(chunk)

            aggregated_response = response_aggregator.get_aggregated_response(messages_for_llm=final_messages)

            content = aggregated_response.get("content", "").strip()
            tool_calls = aggregated_response.get("tool_calls", [])
            reasoning = aggregated_response.get("reasoning", "").strip()
            
            if not content and not tool_calls:
                raise FunctionCallErrorException("Received completely empty response from LLM, forcing retry.")

            aggregated_response['final_stream_id'] = current_stream_id
            
            # ... (token usage stats logic as before) ...
            if run_context:
                stats = run_context['runtime']['token_usage_stats']
                usage = aggregated_response.get("actual_usage")
                if usage and isinstance(usage, dict):
                    prompt_tokens = usage.get("prompt_tokens", 0)
                    completion_tokens = usage.get("completion_tokens", 0)
                    stats["total_prompt_tokens"] += prompt_tokens
                    stats["total_completion_tokens"] += completion_tokens
                    stats["total_successful_calls"] += 1
                    current_call_total = prompt_tokens + completion_tokens
                    if current_call_total > stats["max_context_window"]:
                        stats["max_context_window"] = current_call_total
                    if events:
                        await events.send_json(
                            run_id=run_id_for_event,
                            message={"type": "token_usage_update", "data": stats}
                        )

            if events:
                await events.emit_llm_stream_ended(
                    run_id=run_id_for_event, agent_id=agent_id_for_event, parent_agent_id=kwargs.get('parent_agent_id'),
                    stream_id=current_stream_id, contextual_data=contextual_data_for_event
                )

            # Success, return the result directly and break the loop
            return aggregated_response

        except FunctionCallErrorException as e_retry:
            last_app_level_exception = e_retry
            logger.warning("app_level_retry_triggered", extra={"stream_id": current_stream_id, "reason": str(e_retry), "attempt": attempt + 1, "max_attempts": app_level_max_retries + 1})

            if attempt == 0:
                message_to_extend = [
                    {"role": "assistant", "content": ""},
                    {
                    "role": "user",
                    "content": (
                        "You just made an empty response, which is not acceptable. Let's try again. DO NOT apologize, just continue from where you left off and proceed with my request. "
                        "My request is: " + messages[-1].get("content", "")
                    )
                }
                ]
                final_messages.extend(message_to_extend)
            elif attempt == 1:
                message_to_append = {
                    "role": "user",
                    "content": (
                        "You must ensure that you make a tool call or just say sth, regardless of the situation. "
                        "Not making any reponse is not an option. "
                        "If you are unsure, please ask the user for more information or clarification. "
                    )
                }
                final_messages.append(message_to_append)
            else:
                message_to_append = {
                    "role": "assistant",
                    "content": (
                        "It appears that I am unable to make further progress. For this final attempt, I will just say sth, or call a tool to conclude this flow. "
                        "[To Principal: If you see this message, please review my reasoning and content to assess my progress. "
                        "If there has been no meaningful advancement, consider restarting this workflow with revised requirements.]"
                    )
                }
                final_messages.append(message_to_append)
            if run_context:
                stats = run_context['runtime']['token_usage_stats']
                stats["total_failed_calls"] += 1
                if events:
                    await events.send_json(
                        run_id=run_id_for_event,
                        message={"type": "token_usage_update", "data": stats}
                    )
            
            if events and agent_id_for_event and run_id_for_event:
                await events.emit_llm_stream_failed(
                    run_id=run_id_for_event, 
                    agent_id=agent_id_for_event, 
                    parent_agent_id=kwargs.get('parent_agent_id'),
                    stream_id=current_stream_id, 
                    reason=f"Forcing retry due to: {e_retry}", 
                    contextual_data=contextual_data_for_event
                )

            if attempt > app_level_max_retries:
                logger.error("app_level_retries_exhausted", extra={"max_retries": app_level_max_retries + 1, "final_error": str(e_retry)}, exc_info=True)
                # Break the loop, the final error will be handled after the loop
                break
            
            # Wait for a short period before retrying
            await asyncio.sleep(llm_config.get("wait_seconds_on_retry", 3) * (attempt + 1)) # Exponential backoff
            continue # Continue to the next iteration

        except asyncio.CancelledError:
            # ... (CancelledError handling logic remains unchanged) ...
            logger.warning("llm_call_cancelled", extra={"stream_id": current_stream_id})
            if events and agent_id_for_event and run_id_for_event:
                 await events.emit_llm_stream_failed(
                    run_id=run_id_for_event, 
                    agent_id=agent_id_for_event, 
                    parent_agent_id=kwargs.get('parent_agent_id'),
                    stream_id=current_stream_id, 
                    reason="Operation was cancelled by user request.", 
                    contextual_data=contextual_data_for_event
                )
            raise
            
        except Exception as e_outer:
            # ... (Other exception handling logic remains unchanged) ...
            # Note: The return here will break out of our retry loop, which is correct because these are unrecoverable errors
            if isinstance(e_outer, (RateLimitError, Timeout, APIConnectionError, ServiceUnavailableError, InternalServerError)):
                if run_context:
                    stats = run_context['runtime']['token_usage_stats']
                    stats["total_failed_calls"] += 1
                    if events:
                        await events.send_json(
                            run_id=run_id_for_event,
                            message={"type": "token_usage_update", "data": stats}
                        )
            
            logger.error("call_litellm_outer_error", extra={"error_type": type(e_outer).__name__, "error_message": str(e_outer)}, exc_info=True)
            if events and agent_id_for_event and run_id_for_event:
                 await events.emit_llm_stream_failed(
                    run_id=run_id_for_event, agent_id=agent_id_for_event, parent_agent_id=kwargs.get('parent_agent_id'),
                    stream_id=current_stream_id, reason=f"Unrecoverable error: {type(e_outer).__name__} - {str(e_outer)}", 
                    contextual_data=contextual_data_for_event
                )
            return {"error": f"{type(e_outer).__name__}: {str(e_outer)}", "error_type": type(e_outer).__name__, "actual_usage": None, "content": None, "tool_calls": [], "reasoning": None, "model_id_used": None}

    # 2. If the loop finishes without success, return the final error
    if last_app_level_exception:
        final_error_message = f"LLM call failed after all application-level retries. Last reason: {last_app_level_exception}"
        logger.error("final_app_level_error", extra={"error_message": final_error_message}, exc_info=True)
        return {"error": final_error_message, "error_type": "ForceRetryExhausted", "actual_usage": None, "content": None, "tool_calls": [], "reasoning": None, "model_id_used": None}
    
    # This point should not be reached in theory
    raise RuntimeError("LLM call logic finished unexpectedly.")
    # ===================== END OF REFACTOR =====================
