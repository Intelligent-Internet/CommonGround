
import logging
import uuid
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# Import Turn model definitions
from ..models.turn import Turn, LLMInteraction, ToolInteraction

# Get logger
logger = logging.getLogger(__name__)

class TurnManager:
    """
    A service class dedicated to managing the lifecycle of an Agent Turn.
    It encapsulates all direct operations on the team_state['turns'] list.
    """

    def _get_turn_by_id(self, team_state: Dict, turn_id: str) -> Optional[Turn]:
        """A safe internal helper to find a Turn from the turns list by its ID."""
        if not turn_id or "turns" not in team_state:
            return None
        # Search backwards, as the most recent turn is usually the one of interest
        return next((t for t in reversed(team_state["turns"]) if t.get("turn_id") == turn_id), None)
    
    def add_turn(self, team_state: Dict, turn_object: Turn):
        """Generic method to add a pre-constructed Turn object."""
        team_state.setdefault("turns", []).append(turn_object)
        logger.debug("custom_turn_added", extra={"turn_id": turn_object['turn_id'], "turn_type": turn_object['turn_type']})

    def start_new_turn(self, context: Dict, stream_id: str) -> str:
        """
        Creates a new Turn based on the current context and adds it to team_state.
        
        Args:
            context: The current Agent's SubContext.
            stream_id: The pre-generated unique stream_id for this LLM call.

        Returns:
            The ID of the newly created Turn.
        """
        state = context["state"]
        team_state = context['refs']['team']
        agent_id = context['meta']['agent_id']
        
        turn_id = f"turn_{agent_id}_{uuid.uuid4().hex[:8]}"
        state["current_turn_id"] = turn_id # Store current turn_id in private state

        last_completed_turn_id = state.get("last_turn_id")
        source_turn_ids = [last_completed_turn_id] if last_completed_turn_id else []
        
        # Determine flow_id
        flow_id_to_use = None
        if source_turn_ids:
            last_turn = self._get_turn_by_id(team_state, source_turn_ids[0])
            if last_turn:
                flow_id_to_use = last_turn.get("flow_id")
        
        if not flow_id_to_use:
            flow_id_to_use = f"flow_root_{uuid.uuid4().hex[:8]}"
            logger.warning("flow_id_created_new_root", extra={"agent_id": agent_id, "flow_id": flow_id_to_use})

        new_turn: Turn = {
            "turn_id": turn_id,
            "run_id": context['meta']['run_id'],
            "flow_id": flow_id_to_use,
            "agent_info": {
                "agent_id": agent_id,
                "profile_logical_name": context['loaded_profile'].get("name"),
                "profile_instance_id": context['loaded_profile'].get("profile_id"),
                "assigned_role_name": context['meta'].get("assigned_role_name"),
            },
            "turn_type": "agent_turn",
            "status": "running",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "end_time": None,
            "source_turn_ids": source_turn_ids,
            "source_tool_call_id": None, # Will be populated in enrich_turn_inputs
            "inputs": {"processed_inbox_items": []},
            "outputs": {},
            "llm_interaction": {
                "status": "running",
                "attempts": [{"stream_id": stream_id, "status": "pending", "error": None}],
                "final_request": None,
                "final_response": None,
                "predicted_usage": None,
                "actual_usage": None,
            },
            "tool_interactions": [],
            "metadata": {},
            "error_details": None,
        }
        
        team_state.setdefault("turns", []).append(new_turn)
        logger.debug("new_turn_started", extra={"agent_id": agent_id, "turn_id": turn_id})
        return turn_id

    def enrich_turn_inputs(self, context: Dict, turn_id: str, processing_result: Dict, llm_call_package: Dict, system_prompt_details: Dict):
        """
        Populates the current Turn with inbox processing results, system prompt construction logs, etc.
        """
        team_state = context['refs']['team']
        current_turn = self._get_turn_by_id(team_state, turn_id)
        if not current_turn:
            logger.error("turn_not_found_for_enrichment", extra={"turn_id": turn_id})
            return

        # Populate inputs
        current_turn["inputs"]["processed_inbox_items"] = processing_result.get("processing_log", [])
        current_turn["inputs"]["system_prompt_construction"] = {
            "log": system_prompt_details["construction_log"],
            "final_prompt": system_prompt_details["final_prompt"]
        }
        
        # Populate source_tool_call_id
        for log_item in processing_result.get("processing_log", []):
            if log_item.get("source") == "TOOL_RESULT":
                current_turn["source_tool_call_id"] = log_item.get("payload", {}).get("tool_call_id")
                break
        
        # Populate predicted usage for LLM interaction
        if current_turn.get("llm_interaction"):
            predicted_total_tokens = llm_call_package.get("predicted_total_tokens", 0)
            current_turn["llm_interaction"]["predicted_usage"] = {
                "prompt_tokens": predicted_total_tokens,
                "total_tokens": predicted_total_tokens
            }
            logger.info("token_prediction_completed", extra={"agent_id": context['meta']['agent_id'], "turn_id": turn_id, "predicted_tokens": predicted_total_tokens})


    def add_tool_interaction(self, context: Dict, tool_call: Dict):
        """Records a new tool call interaction in the current Turn."""
        state = context["state"]
        team_state = context['refs']['team']
        turn_id = state.get("current_turn_id")
        current_turn = self._get_turn_by_id(team_state, turn_id)
        if not current_turn:
            logger.error("turn_not_found_for_tool_interaction", extra={"turn_id": turn_id})
            return

        tool_interaction: ToolInteraction = {
            "tool_call_id": tool_call.get("id"),
            "tool_name": tool_call.get("function", {}).get("name"),
            "start_time": datetime.now(timezone.utc).isoformat(),
            "end_time": None,
            "status": "running",
            "input_params": json.loads(tool_call.get("function", {}).get("arguments", "{}")),
            "result_payload": None,
            "error_details": None,
        }
        current_turn.setdefault("tool_interactions", []).append(tool_interaction)
        logger.debug("tool_interaction_added", extra={"turn_id": turn_id, "tool_name": tool_interaction['tool_name']})

    def update_tool_interaction_result(self, context: Dict, tool_call_id: str, result_payload: Any, is_error: bool):
        """
        Finds the corresponding Turn and ToolInteraction by tool_call_id and updates its result.
        This is a core method for the InboxProcessor to call.
        """
        team_state = context['refs']['team']
        if "turns" not in team_state: return

        for turn in reversed(team_state["turns"]):
            for ti in turn.get("tool_interactions", []):
                if ti.get("tool_call_id") == tool_call_id and ti.get("status") == "running":
                    ti["status"] = "error" if is_error else "completed"
                    ti["end_time"] = datetime.now(timezone.utc).isoformat()
                    ti["result_payload"] = result_payload
                    if is_error:
                        ti["error_details"] = str(result_payload)
                    logger.debug("tool_interaction_result_updated", extra={"turn_id": turn['turn_id'], "tool_call_id": tool_call_id, "status": ti['status']})
                    return # Found and updated, can exit

    def record_failed_tool_interaction(self, context: Dict, tool_call: Dict, error_reason: str):
        """Records a completed error interaction in the current Turn for an invalid or immediately failed tool call."""
        state = context["state"]
        team_state = context['refs']['team']
        turn_id = state.get("current_turn_id")
        current_turn = self._get_turn_by_id(team_state, turn_id)
        if not current_turn:
            logger.error("turn_not_found_for_failed_tool_interaction", extra={"turn_id": turn_id})
            return

        now_iso = datetime.now(timezone.utc).isoformat()

        failed_interaction: ToolInteraction = {
            "tool_call_id": tool_call.get("id"),
            "tool_name": tool_call.get("function", {}).get("name"),
            "start_time": now_iso,
            "end_time": now_iso,
            "status": "error",
            "input_params": {},
            "result_payload": None,
            "error_details": error_reason,
        }
        current_turn.setdefault("tool_interactions", []).append(failed_interaction)
        logger.warning("failed_tool_interaction_recorded", extra={"turn_id": turn_id, "tool_name": failed_interaction['tool_name'], "reason": error_reason})

    def update_llm_interaction_end(self, context: Dict, llm_response: Dict):
        """Updates the final result of the LLM interaction for the current Turn."""
        state = context["state"]
        team_state = context['refs']['team']
        turn_id = state.get("current_turn_id")
        current_turn = self._get_turn_by_id(team_state, turn_id)
        if not current_turn or not current_turn.get("llm_interaction"):
            return

        llm_interaction = current_turn["llm_interaction"]
        llm_interaction["status"] = "completed"
        actual_usage = llm_response.get("actual_usage")
        if actual_usage:
            llm_interaction["actual_usage"] = actual_usage
            logger.debug("actual_token_usage_recorded", extra={"turn_id": turn_id, "usage_data": actual_usage})
        
        llm_interaction["final_response"] = {
            "content": llm_response.get("content"),
            "tool_calls": llm_response.get("tool_calls"),
            "reasoning": llm_response.get("reasoning"),
            "model_id_used": llm_response.get("model_id_used"),
        }
        # Update attempt status
        if llm_interaction.get("attempts"):
            last_attempt = llm_interaction["attempts"][-1]
            if last_attempt['status'] == 'pending':
                last_attempt["status"] = "failed" if llm_response.get("error") else "success"
                last_attempt["error"] = llm_response.get("error")


    def fail_current_turn(self, context: Dict, error_message: str):
        """Marks the current Turn as being in an error state."""
        state = context["state"]
        team_state = context['refs']['team']
        turn_id = state.get("current_turn_id")
        current_turn = self._get_turn_by_id(team_state, turn_id)
        if not current_turn or current_turn.get("status") == "error":
            return

        current_turn["status"] = "error"
        current_turn["end_time"] = datetime.now(timezone.utc).isoformat()
        current_turn["error_details"] = error_message
        logger.error("turn_failed", extra={"turn_id": turn_id, "error_message": error_message}, exc_info=True)
        
        if current_turn.get("llm_interaction"):
            llm_interaction = current_turn["llm_interaction"]
            llm_interaction["status"] = "error"
            if llm_interaction.get("attempts"):
                llm_interaction["attempts"][-1]["status"] = "failed"
                llm_interaction["attempts"][-1]["error"] = error_message


    def cancel_current_turn(self, context: Dict):
        """Marks the currently running Turn as cancelled."""
        team_state = context['refs']['team']
        if "turns" not in team_state: return

        last_running_turn = next((t for t in reversed(team_state["turns"]) if t.get("status") == "running"), None)
        if not last_running_turn: return

        last_running_turn["status"] = "cancelled"
        last_running_turn["end_time"] = datetime.now(timezone.utc).isoformat()
        last_running_turn.setdefault("error_details", "Flow was cancelled.")
        
        # Also cancel the LLM interaction
        if (llm_interaction := last_running_turn.get("llm_interaction")) and llm_interaction.get("status") == "running":
            llm_interaction["status"] = "cancelled"
            logger.debug("llm_interaction_cancelled", extra={"turn_id": last_running_turn['turn_id']})
        
        logger.info("turn_cancelled_by_manager", extra={"turn_id": last_running_turn["turn_id"]})

    def finalize_current_turn(self, context: Dict, next_action: Optional[str] = None):
        """
        Finalizes the status of the current Turn after a successful cycle.
        """
        state = context["state"]
        team_state = context['refs']['team']
        turn_id = state.get("current_turn_id")
        current_turn = self._get_turn_by_id(team_state, turn_id)
        if not current_turn: return

        # Only update to 'completed' if the Turn is still 'running'
        if current_turn.get("status") == "running":
            current_turn["status"] = "completed"
            current_turn["end_time"] = datetime.now(timezone.utc).isoformat()
            if next_action:
                current_turn["outputs"] = {"next_action": next_action}
            logger.debug("turn_finalized_as_completed", extra={"turn_id": turn_id})

        # Pass the "baton" regardless of success or failure
        state['last_turn_id'] = turn_id
        logger.debug("turn_baton_passed", extra={"last_turn_id": turn_id, "final_status": current_turn.get('status')})

    def create_restart_delimiter_turn(self, team_state: Dict, run_id: str, old_flow_id: str, source_turn_id: str) -> str:
        """
        Creates and injects a special "delimiter" Turn to visually separate a restarted flow in the FlowView.

        Args:
            team_state: The shared team_state dictionary.
            run_id: The current run ID.
            old_flow_id: The flow_id of the terminated flow.
            source_turn_id: The ID of the last Turn in the old flow, to which this delimiter will connect.

        Returns:
            The ID of the newly created delimiter Turn.
        """
        delimiter_turn_id = f"delimiter_{uuid.uuid4().hex[:8]}"
        
        delimiter_turn: Turn = {
            "turn_id": delimiter_turn_id,
            "run_id": run_id,
            "flow_id": old_flow_id,  # Inherit the old flow_id to keep it in the same visual stream
            "agent_info": {
                "agent_id": "System",
                "profile_logical_name": "FlowControl",
                "profile_instance_id": "N/A",
                "assigned_role_name": "System"
            },
            "turn_type": "restart_delimiter_turn",  # A special type that ViewModelGenerator will recognize
            "status": "completed",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "end_time": datetime.now(timezone.utc).isoformat(),
            "source_turn_ids": [source_turn_id],
            "source_tool_call_id": None,
            "inputs": {"reason": "Principal flow was forcefully restarted by the Partner agent."},
            "outputs": {}, "llm_interaction": None, "tool_interactions": [],
            "metadata": {"description": "This is a system-generated turn to mark a restart point in the execution flow."},
            "error_details": None,
        }
        
        self.add_turn(team_state, delimiter_turn)
        logger.info("restart_delimiter_turn_injected", extra={"delimiter_turn_id": delimiter_turn_id, "source_turn_id": source_turn_id})
        
        return delimiter_turn_id

    def create_aggregation_turn(
        self, 
        team_state: Dict, 
        run_id: str, 
        dispatch_turn: Dict, 
        last_turn_ids_of_subflows: List[str], 
        dispatch_tool_call_id: str,
        aggregation_summary: str
    ) -> str:
        """
        Creates and injects a special "aggregation" Turn to gather parallel sub-flows.

        Args:
            team_state: The shared team_state dictionary.
            run_id: The current run ID.
            dispatch_turn: The Turn that originally called the dispatcher tool.
            last_turn_ids_of_subflows: A list of the last Turn IDs from each completed sub-flow.
            dispatch_tool_call_id: The tool_call_id for the dispatch_submodules call.
            aggregation_summary: A summary of the aggregated results.

        Returns:
            The ID of the newly created aggregation Turn.
        """
        aggregation_turn_id = f"agg_{dispatch_tool_call_id}"
        
        aggregation_turn: Turn = {
            "turn_id": aggregation_turn_id,
            "run_id": run_id,
            "flow_id": dispatch_turn.get("flow_id"),
            "agent_info": dispatch_turn.get("agent_info"),
            "turn_type": "aggregation_turn",
            "status": "completed",
            "start_time": datetime.now(timezone.utc).isoformat(),
            "end_time": datetime.now(timezone.utc).isoformat(),
            "source_turn_ids": last_turn_ids_of_subflows,
            "source_tool_call_id": dispatch_tool_call_id,
            "inputs": {"source_turn_count": len(last_turn_ids_of_subflows)},
            "outputs": {"aggregated_results_summary": aggregation_summary},
            "llm_interaction": None,
            "tool_interactions": [],
            "metadata": {"dispatch_tool_call_id": dispatch_tool_call_id},
            "error_details": None,
        }
        
        self.add_turn(team_state, aggregation_turn)
        logger.info("aggregation_turn_created_by_manager", extra={"aggregation_turn_id": aggregation_turn_id, "dispatch_tool_call_id": dispatch_tool_call_id})
        
        return aggregation_turn_id
