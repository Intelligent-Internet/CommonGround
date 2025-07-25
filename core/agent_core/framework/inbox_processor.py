import logging
import uuid
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from ..events.event_strategies import EVENT_STRATEGY_REGISTRY
from ..events.ingestors import INGESTOR_REGISTRY, markdown_formatter_ingestor
from ..llm.call_llm import estimate_prompt_tokens
from ..llm.config_resolver import LLMConfigResolver

logger = logging.getLogger(__name__)


class InboxProcessor:
    """
    Handles the processing of an agent's inbox, converting inbox items
    into messages ready for the LLM.
    """
    def __init__(self, profile: Dict, context: Dict):
        self.profile = profile
        self.context = context
        self.state = context.get('state', {})
        self.team_state = context.get('refs', {}).get('team', {})
        self.run_context = context.get('refs', {}).get('run', {})
        self.agent_id = context.get('meta', {}).get('agent_id', 'unknown_agent')
        self.turn_manager = self.run_context.get('runtime', {}).get('turn_manager')

    def _create_user_turn_from_inbox_item(self, item: Dict) -> Optional[str]:
        """
        Creates and logs a user_turn based on a USER_PROMPT inbox item.
        Returns the ID of the newly created user_turn, or None if not created.
        """
        state = self.state
        team_state = self.team_state
        
        prompt_content = item.get("payload", {}).get("prompt")
        if not prompt_content:
            return None

        user_turn_id = f"turn_user_{uuid.uuid4().hex[:8]}"
        last_agent_turn_id = state.get("last_turn_id")
        
        flow_id_to_use = None
        if last_agent_turn_id:
            last_turn = next((t for t in reversed(team_state.get("turns", [])) if t.get("turn_id") == last_agent_turn_id), None)
            if last_turn:
                flow_id_to_use = last_turn.get("flow_id")
        
        if not flow_id_to_use:
            flow_id_to_use = f"flow_user_root_{uuid.uuid4().hex[:8]}"

        user_turn = {
            "turn_id": user_turn_id,
            "run_id": self.context.get("meta", {}).get("run_id"),
            "flow_id": flow_id_to_use,
            "agent_info": {
                "agent_id": "User", "profile_logical_name": "user_input", "profile_instance_id": None,
            },
            "turn_type": "user_turn",
            "status": "completed",
            "start_time": item.get("metadata", {}).get("created_at", datetime.now(timezone.utc).isoformat()),
            "end_time": item.get("metadata", {}).get("created_at", datetime.now(timezone.utc).isoformat()),
            "source_turn_ids": [last_agent_turn_id] if last_agent_turn_id else [],
            "source_tool_call_id": None,
            "inputs": {"prompt": prompt_content},
            "outputs": {},
            "llm_interaction": None,
            "tool_interactions": [],
            "metadata": {"client_source": "websocket"},
            "error_details": None,
        }
        team_state.setdefault("turns", []).append(user_turn)
        logger.debug("user_turn_created", extra={"agent_id": self.agent_id, "user_turn_id": user_turn_id})
        
        return user_turn_id

    async def _handle_tool_result_dehydration(self, payload: Dict) -> Dict:
        """
        Handles the dehydration logic for tool results.
        If dehydration is needed, stores the content in the Knowledge Base (KB)
        and replaces it with a token. Otherwise, returns the original payload.
        """
        kb = self.run_context.get('runtime', {}).get("knowledge_base")
        if not kb or not self._should_dehydrate_tool_result(payload):
            return payload  # No dehydration needed, return original payload

        tool_content = payload.get("content")
        tool_name = payload.get("tool_name", "unknown_tool")
        tool_call_id = payload.get("tool_call_id")
        content_size = len(str(tool_content)) if tool_content else 0
    
        kb_item = {
            "item_type": "TOOL_RESULT",
            "content": tool_content,
            "metadata": {
                "source_tool_name": tool_name,
                "source_agent_id": self.agent_id,
                "tool_call_id": tool_call_id,
                "dehydration_reason": self._get_dehydration_reason(payload),
                "original_message_role": "tool",
                "size_bytes": content_size,
                "access_count": 0
            }
        }
    
        add_result = await kb.add_item(kb_item)
    
        if add_result.get("status", "").startswith("success"):
            token = add_result.get("token")
            if token:
                dehydrated_payload = payload.copy()
                dehydrated_payload["content"] = token
                logger.info("tool_result_dehydrated", extra={"tool_name": tool_name, "token": token, "tool_call_id": tool_call_id})
                return dehydrated_payload
    
        logger.error("tool_result_dehydration_failed", extra={"tool_name": tool_name, "error_message": add_result.get('message', 'unknown error')}, exc_info=True)
        return payload

    def _should_dehydrate_tool_result(self, payload: Dict) -> bool:
        """Determine if a tool result should be dehydrated to KB."""
        content = payload.get("content")
        if not content:
            return False
            
        # Size-based strategy
        content_size = len(str(content)) if content else 0
        if content_size > 1024:  # Over 1KB
            return True
            
        # Tool-based strategy
        tool_name = payload.get("tool_name", "")
        dehydrate_tools = ["web_search", "jina_visit", "jina_search", "dispatch_submodules", "generate_markdown_report"]
        if tool_name in dehydrate_tools:
            return True
            
        return False

    def _get_dehydration_reason(self, payload: Dict) -> str:
        """Get the reason for dehydration."""
        content_size = len(str(payload.get("content", "")))
        if content_size > 1024:
            return "size_threshold"
        
        tool_name = payload.get("tool_name", "")
        if tool_name in ["web_search", "jina_visit", "jina_search"]:
            return "tool_policy"
            
        return "unknown"

    async def process(self) -> Dict[str, Any]:
        """
        Processes all items in the inbox, applying strategies and ingestors.
        Returns a dictionary containing messages for the LLM and processing logs.
        """
        inbox = self.state.get("inbox", [])
        if not inbox:
            return {"messages_for_llm": list(self.state.get("messages", [])), "processing_log": [], "processed_item_ids": []}

        logger.debug("inbox_processing_started", extra={"agent_id": self.agent_id, "inbox_item_count": len(inbox)})
        
        # Sort inbox by priority to handle critical items first.
        priority_map = {
            "TOOL_RESULT": 0,          # Highest priority: tool results must be processed first.
            "OBSERVER_FAILURE": 5,     # Internal system errors.
            "AGENT_STARTUP_BRIEFING": 8, # Initial briefing.
            "PARTNER_DIRECTIVE": 10,
            "PRINCIPAL_COMPLETED": 10,
            "INTERNAL_DIRECTIVE": 15,  # Internal directives.
            "SELF_REFLECTION_PROMPT": 20, # Self-reflection is lower priority.
            "WORK_MODULES_STATUS_UPDATE": 90, # Status updates are background context.
            "PRINCIPAL_ACTIVITY_UPDATE": 90,
            "USER_PROMPT": 100,         # Lowest priority: new user input is handled last.
        }
        inbox.sort(key=lambda item: priority_map.get(item.get("source"), 99))
        
        sorted_sources = [item.get('source') for item in inbox]
        logger.debug("inbox_sorted_by_priority", extra={"agent_id": self.agent_id, "source_order": sorted_sources})

        messages_for_llm = list(self.state.get("messages", []))
        processing_log = []
        processed_item_ids = []
        items_to_keep = []
        
        # Garbage Collection (TTL) for persistent items
        items_to_process_and_survivors = []
        for item in inbox:
            metadata = item.get("metadata", {})
            max_turns = metadata.get("max_turns_in_inbox")
            is_persistent = item.get("consumption_policy") == "persistent_until_consumed"

            if is_persistent and max_turns is not None:
                turn_count = metadata.get("turn_count_in_inbox", 0) + 1
                metadata["turn_count_in_inbox"] = turn_count
                if turn_count > max_turns:
                    logger.warning("inbox_item_expired", extra={"item_id": item['item_id'], "source": item['source'], "turn_count": turn_count-1})
                    continue # Skip this item, don't add to survivors
            items_to_process_and_survivors.append(item)

        # Now process the filtered list
        inbox = items_to_process_and_survivors
        items_to_keep = []

        for item in inbox:
            item_id = item.get("item_id", "unknown_item")
            source = item.get("source", "unknown_source")
            ingestor_func = None
            try:
                payload = item["payload"]
                
                if item.get("source") == "USER_PROMPT":
                    new_user_turn_id = self._create_user_turn_from_inbox_item(item)
                    if new_user_turn_id:
                        # Pass the "baton" so the next agent_turn can correctly link to this user_turn.
                        self.state["last_turn_id"] = new_user_turn_id

                profile_strategies = self.profile.get("inbox_handling_strategies", [])
                strategy_override = next((s for s in profile_strategies if s.get("source") == source), None)
                
                handling_strategy = None
                handling_strategy_source = "fallback"
                if strategy_override:
                    handling_strategy_source = "profile"
                    ingestor_name = strategy_override.get("ingestor")
                    ingestor_func = INGESTOR_REGISTRY.get(ingestor_name) if ingestor_name else markdown_formatter_ingestor
                    injection_mode = strategy_override.get("injection_mode", "append_as_new_message")
                    params = strategy_override.get("params", {})
                elif source in EVENT_STRATEGY_REGISTRY:
                    handling_strategy = EVENT_STRATEGY_REGISTRY[source]
                    handling_strategy_source = "global"
                    ingestor_func = handling_strategy.ingestor
                    injection_mode = handling_strategy.default_injection_mode
                    params = handling_strategy.default_params
                else: # Fallback
                    ingestor_func = markdown_formatter_ingestor
                    injection_mode = "append_as_new_message"
                    params = {"role": "user"}

                if not ingestor_func:
                    raise ValueError(f"No ingestor function found for source '{source}'")

                # Dehydrate tool results before processing, if applicable.
                if False:
                    dehydrated_payload = await self._handle_tool_result_dehydration(payload)
                else:
                    dehydrated_payload = payload
                
                injected_content = ingestor_func(dehydrated_payload, params, self.context)

                role = params.get("role", "user")
                is_persistent = params.get("is_persistent_in_memory", False)
                
                new_message = {"role": role, "content": injected_content}

                # If this message comes from the startup briefing, add an internal flag
                # to prevent it from being handed over again in the future.
                if source == "AGENT_STARTUP_BRIEFING":
                    new_message["_internal"] = {
                        "_no_handover": True,
                        "_source_event": "AGENT_STARTUP_BRIEFING"
                    }
                
                if role == "tool":
                    new_message["tool_call_id"] = dehydrated_payload.get("tool_call_id")
                    new_message["name"] = dehydrated_payload.get("tool_name")

                # Update the corresponding tool_interaction in the Turn model.
                if source == "TOOL_RESULT":
                    tool_call_id_from_result = dehydrated_payload.get("tool_call_id")
                    if self.turn_manager and tool_call_id_from_result:
                        self.turn_manager.update_tool_interaction_result(
                            context=self.context,
                            tool_call_id=tool_call_id_from_result,
                            result_payload=dehydrated_payload.get("content"),
                            is_error=dehydrated_payload.get("is_error", False)
                        )
                    else:
                        logger.warning("turn_manager_not_found_in_inbox_processor")

                if injection_mode == "append_as_new_message":
                    messages_for_llm.append(new_message)
                elif injection_mode == "prepend_to_role":
                    found = False
                    for msg in messages_for_llm:
                        if msg.get("role") == role:
                            msg["content"] = f"{injected_content}\n\n---\n\n{msg['content']}"
                            found = True
                            break
                    if not found:
                        messages_for_llm.append(new_message)
                
                # Set a flag upon processing the initial briefing.
                if source == "AGENT_STARTUP_BRIEFING":
                    self.state.setdefault("flags", {})["initial_briefing_delivered"] = True
                    logger.info("startup_briefing_processed", extra={"agent_id": self.agent_id, "initial_briefing_delivered": True})

                if is_persistent:
                    self.state.setdefault("messages", []).append(new_message)

                predicted_tokens = 0
                if injected_content:
                    resolver = LLMConfigResolver(shared_llm_configs=self.run_context.get("config", {}).get("shared_llm_configs_ref", {}))
                    llm_config = resolver.resolve(self.profile)
                    model_name = llm_config.get("model")
                    predicted_tokens = estimate_prompt_tokens(model=model_name, text=injected_content, llm_config_for_tokenizer=llm_config)

                # 4. Log processing
                processing_log.append({
                    "item_id": item_id, "source": source,
                    "handling_strategy_source": handling_strategy_source,
                    "ingestor_used": ingestor_func.__name__,
                    "injection_mode": injection_mode,
                    "injected_content": injected_content,
                    "triggering_observer_id": item.get("metadata", {}).get("triggering_observer_id"),
                    "predicted_token_count": predicted_tokens
                })
                processed_item_ids.append(item_id)

                # 5. Handle consumption
                if item.get("consumption_policy") != "consume_on_read":
                    items_to_keep.append(item)

            except Exception as e:
                ingestor_name_for_log = ingestor_func.__name__ if ingestor_func else "unknown"
                logger.error("inbox_ingestor_failed", extra={"agent_id": self.agent_id, "ingestor_name": ingestor_name_for_log, "item_id": item_id, "error_message": str(e)}, exc_info=True)
                
                injected_content = (
                    f"<system_error context_source='internal_event_processor'>\n"
                    f"  <error_details>\n"
                    f"    <summary>A critical internal error occurred while I was processing information to prepare for my response. A piece of context (from an event '{source}') could not be prepared.</summary>\n"
                    f"    <reason>{str(e)}</reason>\n"
                    f"  </error_details>\n"
                    f"  <instruction>\n"
                    f"    **Action Required: You MUST inform the user about this internal error.**\n"
                    f"    1.  First, formulate your primary response to the user based on the rest of the available, uncorrupted context.\n"
                    f"    2.  Then, at the end of your response, you MUST append a notification to the user about this issue. For example, add a line like: 'Note: An internal error occurred while processing some background information, which might affect the completeness of this response.'\n"
                    f"    3.  You MUST NOT stop your work. Continue the task to the best of your ability with the remaining information.\n"
                    f"  </instruction>\n"
                    f"</system_error>"
                )
                messages_for_llm.append({"role": "system", "content": injected_content})
                
                items_to_keep.append(item)

        self.state["inbox"] = items_to_keep
        logger.debug("inbox_processing_complete", extra={"agent_id": self.agent_id, "consumed_items": len(processed_item_ids), "remaining_items": len(items_to_keep)})
        
        return {
            "messages_for_llm": messages_for_llm,
            "processing_log": processing_log,
            "processed_item_ids": processed_item_ids
        }
