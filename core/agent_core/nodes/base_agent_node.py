"""
Defines the AgentNode, a generic, profile-driven agent executor.
"""

import logging
import uuid
import json
from datetime import datetime, timezone
from pocketflow import AsyncNode
from ..llm.call_llm import estimate_prompt_tokens, call_litellm_acompletion
from ..framework.tool_registry import get_tool_by_name, get_tools_for_profile, format_tools_for_prompt_by_toolset
import json_repair
import os
from typing import Dict, Any, Optional, List
from enum import Enum, auto

from ..framework.profile_utils import get_active_profile_by_name, get_profile_by_instance_id
from ..framework.agent_strategy_helpers import get_formatted_api_tools
from ..framework.inbox_processor import InboxProcessor
from ..utils.context_helpers import get_nested_value_from_context, VModelAccessor
from ..events.event_triggers import trigger_view_model_update
from ..events.event_strategies import EVENT_STRATEGY_REGISTRY
from ..events.ingestors import INGESTOR_REGISTRY, markdown_formatter_ingestor, _apply_simple_template_interpolation
from ..utils.message_utils import tool_call_safenet
from ..config.logging_config import agent_id_var, turn_id_var

logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_EMPTY_RESPONSES = 3  # Threshold for consecutive empty LLM responses.

class NodeExecutionOutcome(Enum):
    """Defines the possible outcomes of a node's execution."""
    SUCCESS_CONTINUE_WITH_TOOL = auto()
    SUCCESS_FINALIZE_NODE_WORK = auto()
    RECOVERABLE_ERROR = auto()
    CRITICAL_NODE_FAILURE = auto()


class AgentNode(AsyncNode):
    """
    A generic agent node whose behavior is driven by an Agent Profile.
    """
    def __init__(self,
                 profile_id: str,  # The logical name of the profile (e.g., 'Principal', 'Associate_WebSearcher').
                 agent_id_override: Optional[str] = None,
                 profile_instance_id_override: Optional[str] = None,  # Directly specify the profile instance UUID.
                 parent_agent_id: Optional[str] = None,  # The ID of the agent that spawned this one.
                 shared_for_init: Optional[Dict] = None,
                 **kwargs):
        super().__init__(max_retries=kwargs.pop('max_retries', 2), wait=kwargs.pop('wait', 3), **kwargs)

        self.profile_id = profile_id
        self.agent_id = agent_id_override or profile_id 
        agent_id_var.set(self.agent_id)  # Set context variable
        self.profile_instance_id_override = profile_instance_id_override
        self.parent_agent_id = parent_agent_id
        
        self.loaded_profile: Optional[Dict] = None

        if not shared_for_init:
            raise ValueError(f"AgentNode '{self.agent_id}': 'shared_for_init' (SubContext object) must be provided to __init__ for profile loading.")

        if "state" not in shared_for_init:
            shared_for_init["state"] = {}
        shared_for_init["state"]["agent_start_utc_timestamp"] = datetime.now(timezone.utc).isoformat()
        shared_for_init["state"]["parent_agent_id"] = self.parent_agent_id
        shared_for_init["state"]["agent_id"] = self.agent_id
        
        if "meta" not in shared_for_init: shared_for_init["meta"] = {}
        shared_for_init["meta"]["agent_id"] = self.agent_id
        shared_for_init["meta"]["parent_agent_id"] = self.parent_agent_id

        self._load_profile(shared_for_init)

        loaded_profile_instance_id = self.loaded_profile.get('profile_id', 'N/A')
        loaded_profile_name = self.loaded_profile.get('name', 'N/A')
        logger.info("agent_node_initialized", extra={"agent_id": self.agent_id, "parent_agent_id": self.parent_agent_id, "profile_instance_id": loaded_profile_instance_id, "profile_name": loaded_profile_name})


    def _load_profile(self, context: Dict):
        """
        Loads the agent profile from the context object's agent_profiles_store.
        Prioritizes profile_instance_id_override if provided, otherwise uses profile_id (logical name).
        Raises ValueError if the profile (including fallback) is not found.
        """
        agent_profiles_store = context['refs']['run']['config'].get("agent_profiles_store", {})
        
        loaded_successfully = False
        if self.profile_instance_id_override:
            logger.debug("profile_load_by_instance_id_attempt", extra={"agent_id": self.agent_id, "profile_instance_id_override": self.profile_instance_id_override})
            self.loaded_profile = get_profile_by_instance_id(agent_profiles_store, self.profile_instance_id_override)
            if self.loaded_profile:
                loaded_successfully = True
                logger.debug("profile_loaded_by_instance_id", extra={"agent_id": self.agent_id, "profile_instance_id_override": self.profile_instance_id_override, "profile_name": self.loaded_profile.get('name')})
            else:
                logger.warning("profile_instance_id_not_found", extra={"agent_id": self.agent_id, "profile_instance_id_override": self.profile_instance_id_override, "profile_id": self.profile_id})

        if not loaded_successfully:
            logger.debug("profile_load_by_logical_name_attempt", extra={"agent_id": self.agent_id, "profile_id": self.profile_id})
            self.loaded_profile = get_active_profile_by_name(agent_profiles_store, self.profile_id)
            if self.loaded_profile:
                loaded_successfully = True
                logger.debug("profile_loaded_by_logical_name", extra={"agent_id": self.agent_id, "profile_id": self.profile_id, "profile_instance_id": self.loaded_profile.get('profile_id')})

        if not loaded_successfully:
            fallback_logical_name = "Associate_GenericExecutor_EN" if "Associate" in self.agent_id else "Principal"
            logger.warning("profile_fallback_attempt", extra={"agent_id": self.agent_id, "profile_instance_id_override": self.profile_instance_id_override, "profile_id": self.profile_id, "fallback_logical_name": fallback_logical_name})
            self.loaded_profile = get_active_profile_by_name(agent_profiles_store, fallback_logical_name)
            if self.loaded_profile:
                logger.warning("profile_loaded_by_fallback", extra={"agent_id": self.agent_id, "fallback_logical_name": fallback_logical_name, "profile_instance_id": self.loaded_profile.get('profile_id')})
            else:
                available_profiles_summary = []
                for inst_id, prof_data in agent_profiles_store.items():
                    available_profiles_summary.append(f"  - Name: {prof_data.get('name', 'N/A')}, InstanceID: {inst_id}, Active: {prof_data.get('is_active')}, Deleted: {prof_data.get('is_deleted')}, Rev: {prof_data.get('rev')}")
                logger.error("profile_load_critical_failure", extra={"agent_id": self.agent_id, "profile_instance_id_override": self.profile_instance_id_override, "profile_id": self.profile_id, "fallback_logical_name": fallback_logical_name, "available_profiles": available_profiles_summary}, exc_info=True)
                raise ValueError(f"AgentProfile not found for agent '{self.agent_id}' (tried instance_id '{self.profile_instance_id_override}', name '{self.profile_id}', fallback '{fallback_logical_name}').")
        
        if self.profile_instance_id_override and self.loaded_profile and self.loaded_profile.get('name') != self.profile_id:
            original_profile_id_param = self.profile_id
            loaded_profile_actual_name = self.loaded_profile.get('name')
            logger.debug("profile_id_updated_for_consistency", extra={"agent_id": self.agent_id, "profile_instance_id_override": self.profile_instance_id_override, "original_profile_id_param": original_profile_id_param, "loaded_profile_actual_name": loaded_profile_actual_name})
            self.profile_id = loaded_profile_actual_name


    def _update_assistant_message_in_state(self, state: Dict, llm_response: Dict):
        """Finds and updates the placeholder assistant message in state['messages']."""
        placeholder_message_id = llm_response.get('placeholder_message_id')
        message_to_update = None
        if placeholder_message_id:
            for msg in reversed(state.get("messages", [])):
                if msg.get("id") == placeholder_message_id:
                    message_to_update = msg
                    break
        
        if message_to_update:
            message_to_update["content"] = llm_response.get("content") or ""
            if llm_response.get("reasoning"):
                message_to_update["reasoning_content"] = llm_response.get("reasoning")
            if llm_response.get("tool_calls"):
                message_to_update["tool_calls"] = llm_response.get("tool_calls")
            
            message_to_update["timestamp"] = datetime.now(timezone.utc).isoformat()
            message_to_update['turn_id'] = state.get("current_turn_id")
            
            logger.debug("placeholder_message_updated", extra={"agent_id": self.agent_id, "placeholder_message_id": placeholder_message_id})
        else:
            logger.warning("placeholder_message_not_found", extra={"agent_id": self.agent_id, "placeholder_message_id": placeholder_message_id})
            assistant_message = {
                "role": "assistant",
                "content": llm_response.get("content") or "",
                "turn_id": state.get("current_turn_id"),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            if llm_response.get("reasoning"):
                assistant_message["reasoning_content"] = llm_response.get("reasoning")
            if llm_response.get("tool_calls"):
                assistant_message["tool_calls"] = llm_response.get("tool_calls")
            state.setdefault("messages", []).append(assistant_message)

    async def _process_observers(self, observer_type: str, context: Dict):
        state = context["state"]
        observer_configs = self.loaded_profile.get(f"{observer_type}_observers", [])
        if not observer_configs:
            return

        logger.debug("processing_observers", extra={"agent_id": self.agent_id, "observer_count": len(observer_configs), "observer_type": observer_type})
        for config in observer_configs:
            observer_id = config.get("id", "unnamed_observer")
            try:
                condition_str = config.get("condition", "True")
                
                # Evaluate condition
                should_run = False
                if condition_str == "True":
                    should_run = True
                else:
                    # For security, only allow access to a safe evaluation context
                    eval_globals = {
                        "v": VModelAccessor(context),  # 'v' accessor for syntactic sugar
                        "get_nested_value_from_context": get_nested_value_from_context,  # For backward compatibility
                        "context_obj": context,
                        "any": any,
                        "all": all,
                        "len": len,
                        "str": str,
                        "int": int,
                    }
                    should_run = eval(condition_str, eval_globals)

                if should_run:
                    logger.info("observer_condition_met", extra={"agent_id": self.agent_id, "observer_id": observer_id})
                    action_config = config.get("action", {})
                    action_type = action_config.get("type")

                    if action_type == "add_to_inbox":
                        target_agent_id = action_config.get("target_agent_id", "self")
                        inbox_item_template = action_config.get("inbox_item", {})
                        
                        # Basic validation
                        if not inbox_item_template.get("source"):
                            raise ValueError("Observer action 'add_to_inbox' requires 'inbox_item.source'")

                        raw_payload = inbox_item_template.get("payload", {})
                        resolved_payload = raw_payload
                        
                        if isinstance(raw_payload, str) and raw_payload.strip().startswith('{{') and raw_payload.strip().endswith('}}'):
                            path_to_resolve = raw_payload.strip('{} ')
                            actual_data = get_nested_value_from_context(context, path_to_resolve)
                            
                            if actual_data is not None:
                                resolved_payload = actual_data
                            else:
                                logger.warning("observer_template_resolve_failed", extra={"observer_id": observer_id, "template": raw_payload, "path": path_to_resolve})
                                resolved_payload = None

                        new_item = {
                            "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
                            "source": inbox_item_template["source"],
                            "payload": resolved_payload,
                            "consumption_policy": inbox_item_template.get("consumption_policy", "consume_on_read"),
                            "metadata": {
                                "created_at": datetime.now(timezone.utc).isoformat(),
                                "triggering_observer_id": observer_id,
                            }
                        }
                        
                        # This is a simplified version. A real implementation would need to handle target_agent_id properly.
                        # For now, we assume "self".
                        state.setdefault("inbox", []).append(new_item)
                        logger.debug("observer_inbox_item_added", extra={"observer_id": observer_id, "item_source": new_item['source']})

                    elif action_type == "update_state":
                        updates = action_config.get("updates", [])
                        for update_op in updates:
                            op = update_op.get("operation")
                            path = update_op.get("path")
                            if not op or not path: continue

                            if op == "set":
                                self._set_nested_value(state, path, update_op.get("value"))
                            elif op == "increment":
                                current_val = get_nested_value_from_context(context, path, 0)
                                self._set_nested_value(state, path, current_val + 1)
                            logger.debug("observer_state_updated", extra={"observer_id": observer_id, "operation": op, "path": path})
            except Exception as e:
                error_message = f"Failed to execute observer '{observer_id}': {e}"
                logger.error("observer_execution_failed", extra={"agent_id": self.agent_id, "observer_id": observer_id, "error_message": str(e)}, exc_info=True)
                # Create an inbox item to report the failure
                state.setdefault("inbox", []).append({
                    "item_id": f"inbox_observer_fail_{uuid.uuid4().hex[:8]}",
                    "source": "OBSERVER_FAILURE",
                    "payload": {
                        "failed_observer_id": observer_id,
                        "error_message": str(e)
                    },
                    "consumption_policy": "consume_on_read",
                    "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                })


    async def _construct_system_prompt(self, context: Dict) -> Dict[str, Any]:
        """
        Constructs the system prompt from segments defined in the agent profile.
        Also returns a detailed log of the construction process.
        """
        prompt_config = self.loaded_profile.get("system_prompt_construction", {})
        segments = prompt_config.get("system_prompt_segments", [])
        text_definitions = self.loaded_profile.get("text_definitions", {})
        
        prompt_parts = []
        construction_log = []

        for segment in sorted(segments, key=lambda s: s.get("order", 99)):
            segment_id = segment.get("id", "unnamed_segment")
            segment_type = segment.get("type")
            rendered_content = ""

            # Check condition first
            condition_str = segment.get("condition", "True")
            should_render = False
            if condition_str == "True":
                should_render = True
            else:
                try:
                    eval_globals = {
                        "get_nested_value_from_context": get_nested_value_from_context,
                        "context_obj": context,
                        "any": any,
                        "all": all,
                        "len": len,
                        "str": str,
                        "int": int,
                    }
                    should_render = eval(condition_str, eval_globals)
                except Exception as e:
                    logger.warning("system_prompt_condition_failed", extra={"segment_id": segment_id, "error_message": str(e)})
                    should_render = False

            if should_render:
                try:
                    if segment_type == "static_text":
                        rendered_content = text_definitions.get(segment.get("content_key"), segment.get("content", ""))
                    
                    elif segment_type == "state_value":
                        source_path = segment.get("source_state_path")
                        ingestor_id = segment.get("ingestor_id")
                        
                        if not source_path:
                            logger.warning("system_prompt_missing_source_path", extra={"segment_id": segment_id, "type": "state_value"})
                            rendered_content = ""
                        else:
                            raw_value = get_nested_value_from_context(context, source_path)
                            
                            if ingestor_id and ingestor_id in INGESTOR_REGISTRY:
                                ingestor_func = INGESTOR_REGISTRY[ingestor_id]
                                ingestor_params = segment.get("ingestor_params", {})
                                rendered_content = ingestor_func(raw_value, ingestor_params, context)
                            elif raw_value is not None:
                                rendered_content = str(raw_value)
                            else:
                                # If raw_value is None and no ingestor, content remains empty
                                rendered_content = ""

                    elif segment_type == "tool_description":
                        applicable_tools = get_tools_for_profile(self.loaded_profile, context, self.agent_id)
                        tools_by_toolset = {}
                        for tool_info in applicable_tools:
                            toolset = tool_info.get("toolset_name", tool_info.get("name", "unknown_toolset"))
                            if toolset not in tools_by_toolset:
                                tools_by_toolset[toolset] = []
                            tools_by_toolset[toolset].append(tool_info)
                        rendered_content = format_tools_for_prompt_by_toolset(tools_by_toolset)

                    if isinstance(rendered_content, str):
                        rendered_content = _apply_simple_template_interpolation(rendered_content, context)
                        
                except Exception as e:
                    logger.error("system_prompt_segment_error", extra={"segment_id": segment_id, "segment_type": segment_type, "error_message": str(e)}, exc_info=True)
                    # Inject an error message into the prompt if a segment fails.
                    rendered_content = (
                        f"\n\n---\n"
                        f"**[[CRITICAL SYSTEM PROMPT FAILURE]]**\n"
                        f"**Alert:** A core part of your instructions (System Prompt Segment ID: '{segment_id}') failed to generate due to an internal error: {e}\n"
                        f"**Your operational context is now incomplete and potentially unreliable.**\n"
                        f"**Mandatory Action:**\n"
                        f"1.  In your very next response to the user, you MUST start your message by stating: 'Warning: A critical internal error has occurred, and my operational instructions may be incomplete. I will proceed with caution, but my response might not be fully accurate.'\n"
                        f"2.  After this warning, proceed with the user's request to the best of your ability using the remaining instructions.\n"
                        f"3.  Do not refer to this error message again unless directly asked by the user.\n"
                        f"---\n\n"
                    )

            prompt_parts.append(rendered_content)
            
            construction_log.append({
                "segment_id": segment_id,
                "order": segment.get("order", 99),
                "type": segment_type,
                "condition_met": should_render,
                "rendered_content": rendered_content,
            })

        final_prompt = "\n\n".join(filter(None, prompt_parts))
        
        return {
            "final_prompt": final_prompt,
            "construction_log": construction_log,
        }

    def _process_tool_calls(self, llm_response: Dict, context: Dict):
        state = context["state"]        
        turn_manager = context['refs']['run']['runtime'].get('turn_manager')

        tool_calls = llm_response.get("tool_calls")
        if tool_calls:
            # For now, we only process the first tool call.
            tool_call_to_process = tool_calls[0]
            tool_name = tool_call_to_process.get("function", {}).get("name")
            tool_arguments_str = tool_call_to_process.get("function", {}).get("arguments", "{}")
            tool_call_id = tool_call_to_process.get("id")
            
            # Step 1: Validate if the tool exists
            tool_info = get_tool_by_name(tool_name)
            if not tool_info:
                error_msg = f"LLM called an unregistered tool: '{tool_name}'."
                logger.error("tool_not_registered", extra={"agent_id": self.agent_id, "tool_name": tool_name, "tool_call_id": tool_call_id}, exc_info=True)
                
                # Step 1a: Record this failed attempt in the Turn
                if turn_manager:
                    turn_manager.record_failed_tool_interaction(context, tool_call_to_process, error_msg)

                # Step 1b: Send an error result to the agent's inbox so it knows it made a mistake
                state.setdefault("inbox", []).append({
                    "item_id": f"inbox_error_{uuid.uuid4().hex[:8]}",
                    "source": "TOOL_RESULT",
                    "payload": {"tool_name": tool_name, "tool_call_id": tool_call_id, "is_error": True, "content": error_msg},
                    "consumption_policy": "consume_on_read",
                    "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                })
                state["current_action"] = None # Clear action
                return # Return early
            
            # Normal flow: Only proceed to parse arguments and create a 'running' interaction if the tool is valid.
            try:
                arguments = json_repair.loads(tool_arguments_str)
                if not isinstance(arguments, dict):
                    raise ValueError("Parsed arguments are not a dictionary.")
            except Exception as e:
                error_msg = f"LLM provided invalid JSON arguments for tool '{tool_name}': {e}. Arguments string: '{tool_arguments_str}'"
                logger.error("tool_arguments_invalid", extra={"agent_id": self.agent_id, "tool_name": tool_name, "tool_call_id": tool_call_id, "arguments_string": tool_arguments_str, "error_message": str(e)}, exc_info=True)
                
                if turn_manager:
                    turn_manager.record_failed_tool_interaction(context, tool_call_to_process, error_msg)

                state.setdefault("inbox", []).append({
                    "item_id": f"inbox_error_{uuid.uuid4().hex[:8]}",
                    "source": "TOOL_RESULT",
                    "payload": {"tool_name": tool_name, "tool_call_id": tool_call_id, "is_error": True, "content": error_msg},
                    "consumption_policy": "consume_on_read",
                    "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                })
                state["current_action"] = None
                return

            if turn_manager:
                turn_manager.add_tool_interaction(context, tool_call_to_process)
            else:
                logger.error("turn_manager_not_found_for_tool_interaction", extra={"agent_id": self.agent_id, "tool_name": tool_name}, exc_info=True)

            state["current_action"] = {
                "type": tool_name, "tool_name": tool_name, **arguments,
                "implementation_type": tool_info.get("implementation_type", "internal"),
                "tool_call_id": tool_call_id
            }
            state["current_tool_call_id"] = tool_call_id
            logger.info("tool_call_decision_made", extra={"agent_id": self.agent_id, "tool_name": tool_name})
        else:
            state["current_action"] = None
            logger.info("no_tool_call_in_response", extra={"agent_id": self.agent_id})

    def _set_nested_value(self, d: Dict, path: str, value: Any):
        """Safely sets a value in a nested dictionary based on a dot-separated path."""
        keys = path.split('.')
        for key in keys[:-1]:
            d = d.setdefault(key, {})
        d[keys[-1]] = value

    def _decide_next_action_with_flow_decider(self, context: Dict) -> str:
        """
        (V2) Determines the next action based on a list of rules in the profile's 'flow_decider'.
        """
        state = context["state"]
        
        # Fallback to old mechanism if flow_decider is not defined
        if "flow_decider" not in self.loaded_profile:
            logger.warning("flow_decider_not_found", extra={"agent_id": self.agent_id})
            return self._determine_next_action_fallback(context)

        rules = self.loaded_profile.get("flow_decider", [])
        for rule in rules:
            rule_id = rule.get("id", "unnamed_rule")
            condition_str = rule.get("condition", "False")
            
            try:
                eval_globals = {
                    "v": VModelAccessor(context),
                    "get_nested_value_from_context": get_nested_value_from_context,
                    "context_obj": context,
                    "any": any, "all": all, "len": len, "str": str, "int": int,
                }
                if eval(condition_str, eval_globals):
                    logger.debug("flow_decider_rule_matched", extra={"agent_id": self.agent_id, "rule_id": rule_id})
                    action_config = rule["action"]
                    action_type = action_config["type"]

                    if action_type == "continue_with_tool":
                        return state.get("current_action", {}).get("tool_name")
                    
                    elif action_type == "end_agent_turn":
                        # This action signals the flow should end.
                        # We can store the outcome in the state for the finalizer.
                        state["_flow_decider_outcome"] = {
                            "outcome": action_config.get("outcome", "error"),
                            "message": action_config.get("error_message", "Flow ended by decider rule.")
                        }
                        # Returning a special value that post_async will handle
                        return "END_FLOW"

                    elif action_type == "loop_with_inbox_item":
                        payload = action_config.get("payload", {})
                        if not payload.get("content_key"):
                            logger.error("flow_decider_missing_content_key", extra={"rule_id": rule_id}, exc_info=True)
                            continue
            
                        state.setdefault("inbox", []).append({
                            "item_id": f"inbox_{rule_id}_{uuid.uuid4().hex[:4]}",
                            "source": "SELF_REFLECTION_PROMPT",
                            "payload": payload,
                            "consumption_policy": "consume_on_read",
                            "metadata": {"created_at": datetime.now(timezone.utc).isoformat(), "triggering_rule_id": rule_id}
                        })
                        return "default"
        
                    elif action_type == "await_user_input":
                        return "await_user_input"

                    else:
                        logger.error("flow_decider_unknown_action", extra={"agent_id": self.agent_id, "action_type": action_type, "rule_id": rule_id}, exc_info=True)
                        
            except Exception as e:
                logger.error("flow_decider_condition_error", extra={"agent_id": self.agent_id, "rule_id": rule_id, "error_message": str(e)}, exc_info=True)

        logger.warning("flow_decider_no_rule_matched", extra={"agent_id": self.agent_id})
        return "default"

    def _determine_next_action_fallback(self, context: Dict) -> str:
        # This is the old logic, kept for compatibility.
        state = context["state"]
        if state.get("current_action"):
            return state["current_action"]["tool_name"]
        
        output_handler_config = self.loaded_profile.get("output_handling_config", {}).get("behavior_parameters_for_default_handler", {})
        action_on_no_tool_call = output_handler_config.get("action_on_no_tool_call", "default")

        if action_on_no_tool_call != "default":
             logger.debug("no_tool_call_profile_action", extra={"agent_id": self.agent_id, "action_on_no_tool_call": action_on_no_tool_call})
             return action_on_no_tool_call
        
        logger.debug("no_tool_call_default_loop", extra={"agent_id": self.agent_id})
        return "default"

    def _resolve_dangling_tool_calls(self, context: Dict):
        """
        (V2 - Symmetrical & Inbox-Aware Check)
        Checks the previous assistant's tool_calls and injects a failure event for those calls
        that have no response in either the message history or the current inbox.
        """
        state = context.get("state", {})
        messages = state.get("messages", [])
        inbox = state.get("inbox", []) # <--- New: Get a reference to the inbox
        if not messages:
            return

        last_assistant_message = None
        last_assistant_message_index = -1
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].get("role") == "assistant":
                last_assistant_message = messages[i]
                last_assistant_message_index = i
                break
        
        if not last_assistant_message or not last_assistant_message.get("tool_calls"):
            return

        expected_tool_call_ids = {tc["id"] for tc in last_assistant_message.get("tool_calls", [])}

        responded_tool_call_ids = set()
        for i in range(last_assistant_message_index + 1, len(messages)):
            msg = messages[i]
            if msg.get("role") == "assistant": break
            if msg.get("role") == "tool" and "tool_call_id" in msg:
                responded_tool_call_ids.add(msg["tool_call_id"])
        
        for item in inbox:
            if item.get("source") == "TOOL_RESULT":
                payload = item.get("payload", {})
                if payload.get("tool_call_id"):
                    responded_tool_call_ids.add(payload["tool_call_id"])

        unresponded_ids = expected_tool_call_ids - responded_tool_call_ids

        if not unresponded_ids:
            logger.debug("symmetry_check_passed", extra={"agent_id": self.agent_id, "tool_call_count": len(expected_tool_call_ids)})
            return

        logger.warning("symmetry_check_failed", extra={"agent_id": self.agent_id, "dangling_call_count": len(unresponded_ids), "unresponded_ids": list(unresponded_ids)})

        for tool_call in last_assistant_message["tool_calls"]:
            tool_call_id = tool_call["id"]
            if tool_call_id in unresponded_ids:
                tool_name = tool_call.get("function", {}).get("name", "unknown_tool")
                tool_result_payload = {
                    "tool_name": tool_name,
                    "tool_call_id": tool_call_id,
                    "is_error": True,
                    "content": {
                        "error": "tool_call_failed", 
                        "message": "The tool did not produce a response, or its execution was interrupted before a result could be processed. Or, if you haved called more than one tool, the tool call was dropped as this agent only supports one tool call per turn.",
                    }
                }
                state.setdefault('inbox', []).insert(0, {
                    "item_id": f"inbox_resolved_{uuid.uuid4().hex[:8]}",
                    "source": "TOOL_RESULT",
                    "payload": tool_result_payload,
                    "consumption_policy": "consume_on_read",
                    "metadata": {
                        "created_at": datetime.now(timezone.utc).isoformat(), 
                        "resolver": "dangling_call_resolver_v2"
                    }
                })
                logger.warning("dangling_tool_call_resolved", extra={"agent_id": self.agent_id, "tool_name": tool_name, "tool_call_id": tool_call_id})

    async def prep_async(self, context: Dict) -> Dict:
        logger.debug("prep_async_started", extra={"agent_id": self.agent_id})
        self.shared = context
        turn_manager = context['refs']['run']['runtime'].get('turn_manager')

        try:
            context["loaded_profile"] = self.loaded_profile

            await self._process_observers('pre_turn', context)
            
            self._resolve_dangling_tool_calls(context)

            # --- START: Refactored Inbox Processing ---
            inbox_processor = InboxProcessor(self.loaded_profile, context)
            processing_result = await inbox_processor.process()
            # --- END: Refactored Inbox Processing ---

            messages_for_llm = processing_result["messages_for_llm"]
            stream_id = f"stream_{self.agent_id}_{uuid.uuid4().hex[:8]}"
            
            turn_id = turn_manager.start_new_turn(context, stream_id)
            turn_id_var.set(turn_id)
            
            system_prompt_details = await self._construct_system_prompt(context)
            system_prompt = system_prompt_details["final_prompt"]
            
            hydrated_messages = await self._hydrate_messages(messages_for_llm)
            logger.debug("messages_hydrated", extra={"agent_id": self.agent_id, "before_count": len(messages_for_llm), "after_count": len(hydrated_messages)})
            
            cleaned_messages = self._clean_messages_for_llm(hydrated_messages)
            logger.debug("messages_cleaned", extra={"agent_id": self.agent_id, "message_count": len(cleaned_messages)})

            # ==================== SAFENET INSERTION POINT ====================
            # As the final step before token estimation and packaging, run the safenet.
            final_messages_for_llm = tool_call_safenet(cleaned_messages, self.agent_id)
            if len(final_messages_for_llm) != len(cleaned_messages):
                logger.warning("tool_call_safenet_modified_messages", extra={"agent_id": self.agent_id, "original_count": len(cleaned_messages), "final_count": len(final_messages_for_llm)})
            # ===============================================================

            from ..llm.config_resolver import LLMConfigResolver
            
            resolver = LLMConfigResolver(shared_llm_configs=context['refs']['run']['config'].get("shared_llm_configs_ref", {}))
            final_llm_config = resolver.resolve(self.loaded_profile)

            predicted_total_tokens = estimate_prompt_tokens(
                model=final_llm_config.get("model"),
                messages=final_messages_for_llm, # <-- Use the sanitized messages
                system_prompt=system_prompt,
                llm_config_for_tokenizer=final_llm_config
            )
            
            api_tools_list = get_formatted_api_tools(self, context)

            self.max_retries = final_llm_config.get("max_retries", self.max_retries)
            self.wait = final_llm_config.get("wait_seconds_on_retry", self.wait)
            
            llm_call_package = {
                "messages_for_llm": final_messages_for_llm, # <-- Use the sanitized messages
                "system_prompt_content": system_prompt,
                "final_llm_config": final_llm_config,
                "api_tools_list": api_tools_list,
                "stream_id": stream_id,
                "context_for_exec": context,
                "predicted_total_tokens": predicted_total_tokens
            }
            
            turn_manager.enrich_turn_inputs(context, turn_id, processing_result, llm_call_package, system_prompt_details)
            
            return llm_call_package
        except Exception as e:
            error_msg = f"Unhandled exception in prep_async: {e}"
            logger.error("prep_async_unhandled_exception", extra={"agent_id": self.agent_id, "error_message": str(e)}, exc_info=True)
            if turn_manager:
                turn_manager.fail_current_turn(context, error_msg)
            raise
    
    async def exec_async(self, prep_res: Dict) -> Dict:
        """
        (Modified) Calls the LLM and returns the aggregated result, or a standard error dictionary on failure.
        """
        logger.debug("exec_async_started", extra={"agent_id": self.agent_id})
        context = prep_res["context_for_exec"]
        flow_specific_state = context["state"]
        events = context['refs']['run']['runtime'].get("event_manager")
        run_id = context['meta'].get("run_id")
        initial_params = flow_specific_state.get("initial_parameters", {})

        # Create a placeholder message ID
        placeholder_message_id = f"msg_{prep_res['stream_id']}"
        placeholder_message = {
            "role": "assistant",
            "content": "",
            "id": placeholder_message_id
        }
        flow_specific_state.setdefault("messages", []).append(placeholder_message)
        logger.debug("placeholder_message_added", extra={"agent_id": self.agent_id, "placeholder_message_id": placeholder_message_id})

        if events:
            await trigger_view_model_update(context, "flow_view")
            await events.emit_turns_sync(context)

        contextual_data_for_event = {}
        if flow_specific_state.get("_associated_task_nums_for_event") is not None:
            contextual_data_for_event["associated_task_nums"] = flow_specific_state.get("_associated_task_nums_for_event")
        if initial_params.get("module_id"):
            contextual_data_for_event["module_id"] = initial_params.get("module_id")
        if initial_params.get("executing_associate_id"):
            contextual_data_for_event["dispatch_id"] = initial_params.get("executing_associate_id")

        try:
            aggregated_llm_output = await call_litellm_acompletion(
                messages=prep_res["messages_for_llm"],
                system_prompt_content=prep_res.get("system_prompt_content"),
                api_tools_list=prep_res["api_tools_list"],
                stream_id=prep_res["stream_id"],
                llm_config=prep_res["final_llm_config"],
                events=events,
                agent_id_for_event=self.agent_id,
                run_id_for_event=run_id,
                parent_agent_id=self.parent_agent_id,
                contextual_data_for_event=contextual_data_for_event,
                run_context=context['refs']['run']
            )
            
            aggregated_llm_output['placeholder_message_id'] = placeholder_message_id
            
            turn_manager = context['refs']['run']['runtime'].get('turn_manager')
            if turn_manager:
                turn_manager.update_llm_interaction_end(context, aggregated_llm_output)

            team_state = context['refs']['team']
            turn_id = context["state"].get("current_turn_id")
            current_turn = next((t for t in reversed(team_state.get("turns", [])) if t.get("turn_id") == turn_id), None)
            if current_turn:
                llm_interaction_ref = current_turn.get("llm_interaction")
                if llm_interaction_ref and os.getenv("CAPTURE_LLM_REQUEST_BODY", "false").lower() == "true":
                    params_to_capture = prep_res["final_llm_config"].copy()
                    params_to_capture.pop("api_key", None)
                    full_request_payload = {
                        "messages": prep_res["messages_for_llm"],
                        "system": prep_res.get("system_prompt_content"),
                        "tools": prep_res["api_tools_list"],
                        "parameters": params_to_capture
                    }
                    llm_interaction_ref["final_request"] = full_request_payload
                    logger.info("llm_request_captured", extra={"agent_id": self.agent_id, "capture_enabled": True})

            return aggregated_llm_output

        except Exception as e:
            error_msg = f"LLM call failed for agent {self.agent_id}: {str(e)}"
            logger.error("llm_call_failed", extra={"agent_id": self.agent_id, "error_message": str(e)}, exc_info=True)
            return {
                "error": error_msg,
                "error_type": type(e).__name__,
                "placeholder_message_id": placeholder_message_id,
                "actual_usage": None, 
                "content": None, 
                "tool_calls": [], 
                "reasoning": None, 
                "model_id_used": None
            }
    
    async def post_async(self, context: Dict, prep_res: Dict, exec_res: Dict) -> str:
        logger.debug("post_async_started", extra={"agent_id": self.agent_id})
        state = context["state"]
        events_for_post = context['refs']['run']['runtime'].get("event_manager")
        run_id_for_post = context['meta'].get("run_id")
        turn_manager = context['refs']['run']['runtime'].get('turn_manager')
        llm_response = exec_res
        next_action = "error_in_post"

        try:
            if "error" in llm_response and llm_response["error"]:
                error_message = llm_response["error"]
                logger.error("post_processing_llm_error", extra={"agent_id": self.agent_id, "error_message": error_message}, exc_info=True)

                if turn_manager:
                    turn_manager.fail_current_turn(context, error_message)
                
                self._update_assistant_message_in_state(state, llm_response)

                if events_for_post:
                    await events_for_post.emit_error(
                        run_id=run_id_for_post,
                        agent_id=self.agent_id,
                        error_message=f"Agent '{self.agent_id}' encountered a critical error: {error_message}"
                    )
                
                next_action = "error"
                return next_action
            
            if turn_manager:
                turn_manager.update_llm_interaction_end(context, llm_response)

            self._process_tool_calls(llm_response, context)
            if isinstance(llm_response.get("tool_calls"), list) and len(llm_response["tool_calls"]) > 1:
                logger.warning("multiple_tool_calls_detected", extra={"agent_id": self.agent_id, "total_calls": len(llm_response['tool_calls']), "dropped_calls": llm_response['tool_calls'][1:]})
                llm_response["tool_calls"] = llm_response["tool_calls"][:1]  # Keep only the first call
            self._update_assistant_message_in_state(state, llm_response)
            # 2. Execute Post-Turn Observers
            await self._process_observers('post_turn', context)
            
            # 3. Decide the next action based on the Profile
            next_action = self._decide_next_action_with_flow_decider(context)
            
            logger.info("turn_completed", extra={"agent_id": self.agent_id, "next_action": next_action})
            return next_action
        except Exception as e:
            error_msg = f"Unhandled exception in post_async: {e}"
            logger.error("post_async_unhandled_exception", extra={"agent_id": self.agent_id, "error_message": str(e)}, exc_info=True)
            if turn_manager:
                turn_manager.fail_current_turn(context, error_msg)
            # Before propagating the exception, we will finalize the turn in the finally block
            raise
        finally:

            tool_info = get_tool_by_name(next_action) if next_action else None
            is_flow_ending_tool = tool_info.get("ends_flow", False) if tool_info else False
            if next_action in ["END_FLOW", "error"] or is_flow_ending_tool:
                self._finalize_dangling_tool_in_turn(context)

            if turn_manager:
                turn_manager.finalize_current_turn(context, next_action)

            if events_for_post:
                current_turn_id = state.get("current_turn_id")
                if current_turn_id:
                    await events_for_post.emit_turn_completed(
                        run_id=run_id_for_post,
                        turn_id=current_turn_id,
                        agent_id=self.agent_id
                    )                
                await trigger_view_model_update(context, "flow_view")
                await trigger_view_model_update(context, "timeline_view")
                await trigger_view_model_update(context, "kanban_view")
                await events_for_post.emit_turns_sync(context)
    
    def _extract_purpose_from_tool_result(self, payload: Dict, context: Dict) -> str:
        """Extract purpose/context from tool result to create unique source_uri."""
        tool_name = payload.get("tool_name", "unknown")
        tool_content = payload.get("content", {})
        
        # Try to get purpose from current action context
        current_action = context.get("state", {}).get("current_action", {})
        agent_profile = self.loaded_profile.get("name", "unknown")
        
        # Generate purpose based on tool type and context
        if tool_name in ["jina_search", "web_search"]:
            if isinstance(tool_content, dict):
                query = str(tool_content.get("query", ""))[:50]  # First 50 chars
                purpose = f"search_{hash(query) % 10000}"  # Hash to keep it short
            else:
                purpose = "search_results"
        
        elif tool_name == "jina_visit":
            if isinstance(tool_content, dict):
                url = tool_content.get("url", "")
                if url:
                    # Use domain and path hash for purpose
                    from urllib.parse import urlparse
                    try:
                        parsed = urlparse(url)
                        domain = parsed.netloc or "unknown"
                        purpose = f"visit_{domain}_{hash(url) % 10000}"
                    except:
                        purpose = "page_content"
                else:
                    purpose = "page_content"
            else:
                purpose = "page_content"
                
        elif tool_name == "dispatch_submodules":
            # For dispatcher, include some context about the assignment
            if isinstance(tool_content, dict):
                assignments = tool_content.get("assignments", [])
                if assignments:
                    # Use first assignment's module_id for context
                    first_module = assignments[0].get("module_id_to_assign", "")
                    purpose = f"dispatch_{first_module[:10]}"
                else:
                    purpose = "dispatch_general"
            else:
                purpose = "dispatch_result"
                
        elif tool_name == "generate_markdown_report":
            purpose = "final_report"
            
        else:
            # For other tools, use agent profile and tool name
            purpose = f"{agent_profile}_{tool_name}".replace("_", "")[:20]
        
        return purpose
    
    async def _hydrate_messages(self, dehydrated_messages: List[Dict]) -> List[Dict]:
        """
        [Refactored] Simplified message hydration logic, fully delegated to the Knowledge Base (KB).
        """
        kb = self.shared.get('refs', {}).get('run', {}).get('runtime', {}).get("knowledge_base")
        if not kb:
            logger.warning("kb_not_available_for_hydration", extra={"agent_id": self.agent_id})
            return dehydrated_messages

        hydrated_messages = []
        for msg in dehydrated_messages:
            hydrated_msg = msg.copy()
            try:
                # Call the KB's public hydration method, which can handle various nested structures
                hydrated_msg['content'] = await kb.hydrate_content(msg.get('content'))
            except Exception as e:
                logger.error("message_hydration_failed", extra={"agent_id": self.agent_id, "error_message": str(e)}, exc_info=True)
                # Keep the original (dehydrated) content as a fallback
                hydrated_msg['content'] = msg.get('content')
            
            hydrated_messages.append(hydrated_msg)
        
        logger.debug("message_hydration_complete", extra={"message_count": len(hydrated_messages)})
        return hydrated_messages
        
    def _clean_messages_for_llm(self, messages: List[Dict]) -> List[Dict]:
        """Cleans messages, removes internal fields, and ensures all content is LLM-processable text."""
        cleaned_messages = []
        
        for msg in messages:
            # Create a clean copy of the message
            cleaned_msg = {}
            
            # Keep only the standard fields required by the LLM
            for key in ["role", "content", "tool_calls", "tool_call_id", "name"]:
                if key in msg:
                    value = msg[key]
                    
                    # Ensure content is a string
                    if key == "content":
                        if isinstance(value, dict):
                            # If content is a dictionary, convert it to a JSON string
                            import json
                            cleaned_msg[key] = json.dumps(value, ensure_ascii=False)
                            logger.debug("dict_content_converted_to_json", extra={"message_role": msg.get('role')})
                        elif value is None:
                            cleaned_msg[key] = ""  # Prevent None content
                        else:
                            cleaned_msg[key] = str(value)  # Ensure it is a string
                    else:
                        cleaned_msg[key] = value
            
            # Filter out internal fields (those starting with _)
            internal_fields = [k for k in msg.keys() if k.startswith('_')]
            if internal_fields:
                logger.debug("internal_fields_removed", extra={"internal_fields": internal_fields})
            
            cleaned_messages.append(cleaned_msg)
        
        return cleaned_messages

    def _finalize_dangling_tool_in_turn(self, context: Dict):
        """
        When the flow is about to terminate, finds the running tool interaction in the current turn and updates its status to 'completed'.
        This is a fallback mechanism specifically for tools like finish_flow that end the process immediately.
        """
        state = context.get("state", {})
        team_state = context.get("refs", {}).get("team", {})
        
        current_turn_id = state.get("current_turn_id")
        current_tool_call_id = state.get("current_tool_call_id")

        # Both current turn ID and tool call ID must exist
        if not (current_turn_id and current_tool_call_id and team_state.get("turns")):
            return

        # Find the current Turn
        current_turn = next((t for t in reversed(team_state.get("turns", [])) if t.get("turn_id") == current_turn_id), None)
        
        if current_turn:
            # Find the corresponding tool_interaction in this turn that is still 'running'
            tool_interaction_to_update = next((
                ti for ti in current_turn.get("tool_interactions", []) 
                if ti.get("tool_call_id") == current_tool_call_id and ti.get("status") == "running"
            ), None)

            if tool_interaction_to_update:
                # Manually update the status and end time
                tool_interaction_to_update["status"] = "completed"
                tool_interaction_to_update["end_time"] = datetime.now(timezone.utc).isoformat()
                tool_interaction_to_update["result_payload"] = {"status": "finalized", "reason": "Flow is ending."}
                logger.info("tool_interaction_finalized", extra={"tool_call_id": current_tool_call_id, "status": "completed", "reason": "flow_ending"})
