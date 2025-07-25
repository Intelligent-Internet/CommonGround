# File path: nodes/custom_nodes/dispatcher_node.py

import logging
import asyncio
import copy
import uuid 
from datetime import datetime, timezone # Added timezone
from typing import List, Dict, Any
from pocketflow import AsyncParallelBatchNode # Ensure this is the correct base class
from ...framework.tool_registry import tool_registry
# from nodes.base_agent_node import AgentNode # Not directly used here for instantiation
from ...state.management import _create_flow_specific_state_template 
from ...framework.profile_utils import get_active_profile_by_name
from ...framework.handover_service import HandoverService
# +++ START: New imports +++
# from utils.server_manager import initialize_mcp_session_for_context # No longer needed
# +++ END: New imports +++

logger = logging.getLogger(__name__)

DESCRIPTION = """
Called by the Principal to validate and assign a Work Module to an Associate Agent for execution.
  - assignments: List of assignments to be made. Each assignment targets **one** Work Module.
    - A module can only be assigned to one Associate Agent at a time.
"""

@tool_registry(
    name="dispatch_submodules",
    # Associate the tool with our newly created protocol
    handover_protocol="principal_to_associate_briefing", 
    description=DESCRIPTION,
    parameters={
        "type": "object",
        "properties": {
            # Only control parameters are kept here now
            "assignments": {
                "type": "array",
                "description": "List of work module assignments. Each assignment targets one pending ('pending' or 'pending_review' status) work module.",
                "items": {
                    "type": "object",
                    "properties": {
                        # --- Control Parameters (not defined by protocol) ---
                        "agent_profile_logical_name": {
                            "type": "string",
                            "description": "The logical name of the Associate Agent Profile to use for this module."
                        },
                        "assigned_role_name": {
                            "type": "string",
                            "description": "The role name assigned for this execution, e.g., 'Market_Researcher'."
                        }
                        # --- Context Parameters (will be auto-merged by handover_protocol) ---
                        # "module_id_to_assign", "assignment_specific_instructions", "inherit_messages_from"
                        # have been removed as they are now defined by principal_to_associate_briefing.yaml
                    },
                    # The "required" list will also be auto-merged from the handover_protocol
                    "required": ["agent_profile_logical_name", "assigned_role_name"]
                }
            },
            # --- Context Parameters (moved to protocol) ---
            # "shared_context_for_all_assignments" is now moved to the handover protocol
        },
        "required": ["assignments"]
    },
    default_knowledge_item_type="DISPATCH_SUBMODULES_RESULT" 
)
class DispatcherNode(AsyncParallelBatchNode):
    """
    DispatcherNode: Validates and assigns tasks in dispatchable states (not 'deleted', 'deprecated', 'completed') from `team_state.plan` to Associate Agents,
    and updates their status.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logger.debug("dispatcher_node_initialized")

    async def prep_async(self, shared: Dict) -> List[Dict]:
        logger.debug("dispatcher_prep_async_started")
        # shared is principal's SubContext
        principal_state = shared["state"]
        principal_team_state = shared['refs']['team']
        work_modules = principal_team_state.get("work_modules", {})
        agent_profiles_store = shared['refs']['run']['config'].get("agent_profiles_store")
        current_tool_call_id = principal_state.get("current_tool_call_id", f"dtcid_unknown_{uuid.uuid4().hex[:4]}")

        if not agent_profiles_store:
            logger.error("dispatcher_prep_agent_profiles_missing")
            principal_state["_temp_dispatcher_prep_failures"] = [{"error": "Agent profiles store not available."}]
            return []
        if not isinstance(work_modules, dict):
            logger.error("dispatcher_prep_work_modules_malformed")
            principal_state["_temp_dispatcher_prep_failures"] = [{"error": "Work modules are malformed or missing."}]
            return []

        dispatch_action = principal_state.get("current_action", {})
        assignments_input = dispatch_action.get("assignments", [])
        shared_context_for_all = dispatch_action.get("shared_context_for_all_assignments", "")

        tasks_for_parallel_exec = []
        failed_assignments_at_prep = []
        assigned_module_ids_in_this_call = set()

        for assign_idx, assignment_item in enumerate(assignments_input):
            module_id = assignment_item.get("module_id_to_assign")
            agent_profile_logical_name = assignment_item.get("agent_profile_logical_name")
            assigned_role_name = assignment_item.get("assigned_role_name")

            # [Requirement 2.1] Prevent duplicate dispatch in a single call
            if module_id in assigned_module_ids_in_this_call:
                err_msg = f"Duplicate assignment for module_id '{module_id}' in a single call."
                logger.warning("dispatcher_prep_duplicate_assignment", extra={"module_id": module_id, "error_message": err_msg})
                failed_assignments_at_prep.append({"input": assignment_item, "reason": err_msg})
                continue
            if module_id:
                assigned_module_ids_in_this_call.add(module_id)

            module_to_assign = work_modules.get(module_id)
            if not module_to_assign:
                failed_assignments_at_prep.append({"input": assignment_item, "reason": f"Work Module ID '{module_id}' not found."})
                continue

            # [Requirement 2.2] Allow dispatching from 'pending' or 'pending_review'
            current_module_status = module_to_assign.get("status", "unknown").lower()
            if current_module_status not in ["pending", "pending_review"]:
                err_msg = f"Work Module '{module_id}' has status '{current_module_status}', but must be 'pending' or 'pending_review' to be dispatched."
                failed_assignments_at_prep.append({"input": assignment_item, "reason": err_msg})
                continue

            actual_profile_details = get_active_profile_by_name(agent_profiles_store, agent_profile_logical_name)
            if not actual_profile_details:
                failed_assignments_at_prep.append({"input": assignment_item, "reason": f"Profile '{agent_profile_logical_name}' not found or inactive."})
                continue
            
            assignment_package = {
                "original_assignment_input": assignment_item,
                "resolved_profile_instance_id": actual_profile_details.get("profile_id"),
                "resolved_profile_logical_name": agent_profile_logical_name,
                "assigned_role_name": assigned_role_name,
                "module_to_execute": copy.deepcopy(module_to_assign),
                "shared_context_for_all_assignments": shared_context_for_all,
                "executing_associate_id": f"Assoc_{agent_profile_logical_name.replace('Associate_', '')[:10]}_{module_id.replace('WM_', '')}",
                "dispatch_tool_call_id_ref": current_tool_call_id,
                "shared_for_exec_context": shared
            }
            tasks_for_parallel_exec.append(assignment_package)

        principal_state["_temp_dispatcher_prep_failures"] = failed_assignments_at_prep
        logger.info("dispatcher_prep_async_completed", extra={
            "valid_assignments": len(tasks_for_parallel_exec),
            "failed_prep_checks": len(failed_assignments_at_prep)
        })
        return tasks_for_parallel_exec

    async def exec_async(self, assignment_package: Dict) -> Dict:
        parent_context = assignment_package.pop("shared_for_exec_context") # This is Principal's SubContext
        run_context_global = parent_context['refs']['run'] # This is the global RunContext
        team_state_global = parent_context['refs']['team'] # This is the global TeamState
        events = run_context_global['runtime'].get("event_manager")
        run_id = run_context_global['meta'].get("run_id")

        if "_ongoing_associate_tasks" not in run_context_global['sub_context_refs']:
            run_context_global['sub_context_refs']["_ongoing_associate_tasks"] = {}

        module_to_execute = assignment_package["module_to_execute"]
        module_id = module_to_execute["module_id"]
        executing_associate_id = assignment_package["executing_associate_id"]
        profile_logical_name = assignment_package["resolved_profile_logical_name"]
        
        logger.info("dispatcher_exec_assignment_started", extra={
            "module_id": module_id,
            "profile_logical_name": profile_logical_name,
            "executing_associate_id": executing_associate_id
        })

        module_to_update = copy.deepcopy(team_state_global.get("work_modules", {}).get(module_id))
        if not module_to_update:
            return {"error": f"Module {module_id} not found at execution time."}
        
        start_time_iso = datetime.now(timezone.utc).isoformat()
        module_to_update["status"] = "ongoing"
        module_to_update["updated_at"] = start_time_iso
        module_to_update.setdefault("assignee_history", []).append({
            "dispatch_id": executing_associate_id, "agent_id": executing_associate_id,
            "started_at": start_time_iso, "ended_at": None, "outcome": "running"
        })
        team_state_global["work_modules"][module_id] = module_to_update
        if events and hasattr(events, "emit_work_module_updated"):
            await events.emit_work_module_updated(run_id, module_to_update)

        try:
            from ...events.event_triggers import trigger_view_model_update
            await trigger_view_model_update(parent_context, "kanban_view")
            logger.info("dispatcher_kanban_view_update_triggered", extra={"module_id": module_id, "new_status": "ongoing"})
        except Exception as e_trigger:
            logger.error("dispatcher_kanban_view_update_failed", extra={"module_id": module_id, "error": str(e_trigger)}, exc_info=True)

        history_entry = {
            "dispatch_id": executing_associate_id, "dispatch_tool_call_id_ref": assignment_package["dispatch_tool_call_id_ref"],
            "module_id": module_id, "profile_logical_name": profile_logical_name, "start_timestamp": None,
            "end_timestamp": None, "status": "LAUNCHING", "final_summary": None, "error_details": None
        }
        team_state_global.setdefault("dispatch_history", []).append(history_entry)
        logger.info("dispatcher_history_entry_added", extra={"executing_associate_id": executing_associate_id, "status": "LAUNCHING"})

        try:
            # Build a temporary source_context to simulate the state when the Principal calls the tool
            temp_source_context_for_handover = {
                "state": { 
                    "current_action": {
                        # Place the current assignment's parameters into current_action.parameters
                        "parameters": assignment_package.get("original_assignment_input", {})
                    }
                },
                "refs": parent_context["refs"],
                "meta": parent_context["meta"]
            }
            
            # Call HandoverService
            inbox_item_data = await HandoverService.execute(
                "principal_to_associate_briefing", 
                temp_source_context_for_handover
            )

        except Exception as e:
            logger.error("dispatcher_handover_service_failed", extra={"executing_associate_id": executing_associate_id, "error": str(e)}, exc_info=True)
            return {"error": f"Failed to prepare context handover: {e}"}

        associate_sub_context_state = _create_flow_specific_state_template()
        associate_sub_context_state.setdefault("inbox", []).append({
            "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
            "source": inbox_item_data["source"],
            "payload": inbox_item_data["payload"],
            "consumption_policy": "consume_on_read",
            "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
        })
        
        principal_last_turn_id = parent_context['state'].get("last_turn_id")
        associate_sub_context_state['last_turn_id'] = principal_last_turn_id
        logger.debug("dispatcher_last_turn_id_passed", extra={"last_turn_id": principal_last_turn_id, "executing_associate_id": executing_associate_id})

        principal_agent_id = parent_context['meta'].get("agent_id")
        assigned_role_name = assignment_package.get("assigned_role_name")
        
        associate_sub_context: Dict[str, Any] = {
            "meta": {
                "run_id": run_id,
                "agent_id": executing_associate_id,
                "parent_agent_id": principal_agent_id,
                "assigned_role_name": assigned_role_name,
                "module_id": module_id,
                "module_description": module_to_execute.get("description"),
                "profile_logical_name": profile_logical_name,
                "profile_instance_id": assignment_package["resolved_profile_instance_id"],
                "dispatch_tool_call_id_ref": assignment_package["dispatch_tool_call_id_ref"],
            },
            "state": associate_sub_context_state,
            "runtime_objects": {},
            "refs": { "run": run_context_global, "team": team_state_global }
        }
        
        logger.info("dispatcher_associate_starting", extra={"executing_associate_id": executing_associate_id})
        
        completed_associate_context = None
        associate_exec_status = "error"
        last_turn_id = None
        new_messages_from_associate = []
        try:
            if run_context_global:
                run_context_global['sub_context_refs']["_ongoing_associate_tasks"][executing_associate_id] = associate_sub_context
                logger.info("dispatcher_associate_task_registered", extra={"executing_associate_id": executing_associate_id})
            from ...flow import run_associate_async
            completed_associate_context = await run_associate_async(associate_sub_context)
            
            final_associate_state = completed_associate_context.get("state", {})
            last_turn_id = final_associate_state.get("last_turn_id")

            if not final_associate_state.get("error_message"):
                associate_exec_status = "success"
        except Exception as e:
            logger.error("dispatcher_associate_critical_error", extra={"executing_associate_id": executing_associate_id, "error": str(e)}, exc_info=True)
            if completed_associate_context is None: completed_associate_context = {}
            final_associate_state = completed_associate_context.setdefault("state", {})
            final_associate_state["error_message"] = f"Dispatcher critical error: {str(e)}"
            final_associate_state.setdefault("deliverables", {})["error"] = f"Dispatcher critical error: {str(e)}"
        
        finally:
            end_time_iso = datetime.now(timezone.utc).isoformat()
            final_outcome = "completed_success" if associate_exec_status == "success" else "completed_error"
            
            final_associate_state = completed_associate_context.get("state", {}) if completed_associate_context else {}
            deliverables_from_associate = final_associate_state.get("deliverables", {})
            error_details_from_associate = final_associate_state.get("error_message")
            
            all_messages = final_associate_state.get("messages", [])
            
            # Filter out messages that are marked as not for handover (e.g., initial briefings).
            # The msg.get("_internal", {}) ensures safe access even if the _internal key doesn't exist.
            new_messages_from_associate = [
                msg for msg in all_messages if not msg.get("_internal", {}).get("_no_handover")
            ]

            logger.info("dispatcher_messages_extracted", extra={"executing_associate_id": executing_associate_id, "new_message_count": len(new_messages_from_associate)})

            history_entry_to_update = next((h for h in team_state_global.get("dispatch_history", []) if h.get("dispatch_id") == executing_associate_id), None)
            if history_entry_to_update:
                status_map = {"success": "COMPLETED_SUCCESS", "error": "COMPLETED_ERROR"}
                history_entry_to_update["status"] = status_map.get(associate_exec_status, "COMPLETED_ERROR")
                history_entry_to_update["end_timestamp"] = end_time_iso
                history_entry_to_update["error_details"] = error_details_from_associate
                if deliverables_from_associate:
                    summary = ", ".join(deliverables_from_associate.keys())
                    history_entry_to_update["final_summary"] = f"Deliverables: {summary}"
                logger.info("dispatcher_history_updated", extra={"executing_associate_id": executing_associate_id, "new_status": history_entry_to_update['status']})
            
            history_list = module_to_update.get("assignee_history", [])
            entry_to_update = next((h for h in reversed(history_list) if h.get("dispatch_id") == executing_associate_id and h.get("outcome") == "running"), None)
            if entry_to_update:
                entry_to_update["ended_at"] = end_time_iso
                entry_to_update["outcome"] = final_outcome

            module_to_update.setdefault("context_archive", []).append({
                "dispatch_id": executing_associate_id, "archived_at": end_time_iso,
                "messages": final_associate_state.get("messages", []), "deliverables": deliverables_from_associate
            })
            
            module_to_update["status"] = "pending_review"
            module_to_update["review_info"] = {
                "trigger": "associate_completed" if associate_exec_status == "success" else "associate_failed",
                "message": "Associate completed its work." if associate_exec_status == "success" else "Associate failed with an exception.",
                "error_details": error_details_from_associate
            }
            module_to_update["updated_at"] = end_time_iso
            team_state_global["work_modules"][module_id] = module_to_update
            if events and hasattr(events, "emit_work_module_updated"):
                await events.emit_work_module_updated(run_id, module_to_update)

            if run_context_global and executing_associate_id in run_context_global['sub_context_refs'].get("_ongoing_associate_tasks", {}):
                del run_context_global['sub_context_refs']["_ongoing_associate_tasks"][executing_associate_id]
                logger.info("dispatcher_associate_task_deregistered", extra={"executing_associate_id": executing_associate_id})

        return {
            "executing_associate_id": executing_associate_id, 
            "module_id": module_id,
            "agent_profile_logical_name_used": profile_logical_name, 
            "status_of_associate_execution": associate_exec_status,
            "deliverables_from_associate": deliverables_from_associate, 
            "error_detail_from_associate": error_details_from_associate,
            "last_turn_id": last_turn_id,
            "new_messages_from_associate": new_messages_from_associate
        }

    async def post_async(self, shared: Dict, prep_res: List[Dict], exec_res_list: List[Dict]):
        principal_state = shared["state"]
        dispatch_tool_call_id = principal_state.get("current_tool_call_id", f"dtcid_unknown_{uuid.uuid4().hex[:4]}")

        logger.debug("dispatcher_post_async_aggregating", extra={"execution_count": len(exec_res_list), "dispatch_tool_call_id": dispatch_tool_call_id})

        failed_assignments_from_prep = principal_state.pop("_temp_dispatcher_prep_failures", [])
        
        num_launched_modules = len(exec_res_list)
        num_successful_executions = sum(1 for res in exec_res_list if res.get("status_of_associate_execution") == "success")
        num_failed_executions = num_launched_modules - num_successful_executions
        num_prep_failures = len(failed_assignments_from_prep)
        original_assignments_requested_count = num_launched_modules + num_prep_failures

        overall_dispatch_op_status = "TOTAL_FAILURE"
        if original_assignments_requested_count == 0:
            overall_dispatch_op_status = "NO_ASSIGNMENTS_REQUESTED"
        elif num_launched_modules > 0:
            if num_successful_executions == num_launched_modules:
                overall_dispatch_op_status = "SUCCESS" if num_prep_failures == 0 else "PARTIAL_SUCCESS_SOME_PREP_FAILED"
            elif num_successful_executions > 0:
                overall_dispatch_op_status = "PARTIAL_SUCCESS_ASSOCIATES_SOME_FAILED" if num_prep_failures == 0 else "PARTIAL_SUCCESS_MIXED_RESULTS"
            else:
                overall_dispatch_op_status = "TOTAL_FAILURE_ASSOCIATES_ALL_FAILED" if num_prep_failures == 0 else "TOTAL_FAILURE_PREP_AND_ASSOC_FAILED"
        elif num_prep_failures > 0 and num_launched_modules == 0 :
             overall_dispatch_op_status = "TOTAL_FAILURE_ALL_PREP_FAILED"
        
        dispatch_op_message = (
            f"Dispatch operation concluded for {original_assignments_requested_count} requested assignment(s). "
            f"{num_launched_modules} module(s) were dispatched. "
            f"Of those, {num_successful_executions} completed successfully and are now 'pending_review'. "
            f"{num_failed_executions} failed and are also 'pending_review' for analysis. "
            f"{num_prep_failures} assignment(s) failed pre-check and were not dispatched."
        )

        llm_output_assignment_results = []
        for exec_result in exec_res_list:
            if "error" in exec_result: continue
            llm_output_assignment_results.append({
                "module_id": exec_result.get("module_id"),
                "associate_id": exec_result.get("executing_associate_id"),
                "execution_status": exec_result.get("status_of_associate_execution"),
                "deliverables": exec_result.get("deliverables_from_associate", {}),
                "error_details": exec_result.get("error_detail_from_associate"),
                "new_messages_from_associate": exec_result.get("new_messages_from_associate", [])
            })

        final_tool_output_content = {
            "status": overall_dispatch_op_status,
            "message": dispatch_op_message,
            "assignment_execution_results": llm_output_assignment_results,
            "failed_preparation_details": failed_assignments_from_prep
        }

        # --- Create Aggregation Turn (Conditionally) ---
        if exec_res_list:
            team_state_from_refs_post = shared['refs']['team']
            run_id_from_meta_post = shared['meta']['run_id']
            turn_manager = shared['refs']['run']['runtime'].get('turn_manager')
            
            # Find the Turn that initiated this dispatch
            dispatch_turn = turn_manager._get_turn_by_id(team_state_from_refs_post, principal_state.get("current_turn_id")) if turn_manager else None

            if dispatch_turn and turn_manager:
                last_turn_ids_of_subflows = [res.get("last_turn_id") for res in exec_res_list if res.get("last_turn_id")]
                
                # Call TurnManager to create the aggregation turn
                aggregation_turn_id = turn_manager.create_aggregation_turn(
                    team_state=team_state_from_refs_post,
                    run_id=run_id_from_meta_post,
                    dispatch_turn=dispatch_turn,
                    last_turn_ids_of_subflows=last_turn_ids_of_subflows,
                    dispatch_tool_call_id=dispatch_tool_call_id,
                    aggregation_summary=f"{num_successful_executions}/{num_launched_modules} successful."
                )
                
                # Pass the "baton" to the new aggregation turn
                principal_state['last_turn_id'] = aggregation_turn_id
                logger.debug("dispatcher_relay_baton_passed", extra={"aggregation_turn_id": aggregation_turn_id})
            else:
                logger.error("dispatcher_aggregation_turn_creation_failed", extra={"dispatch_tool_call_id": dispatch_tool_call_id, "reason": "Could not find dispatch_turn or turn_manager"})
        else:
            # If exec_res_list is empty, it means all tasks failed in the preparation phase, so no aggregation turn is created.
            # last_turn_id remains unchanged; the next Principal turn will connect directly to the current dispatch_turn.
            logger.info("dispatcher_aggregation_turn_skipped", extra={"dispatch_tool_call_id": dispatch_tool_call_id, "reason": "no_subtasks_executed"})
        # --- End Aggregation Turn ---

        tool_result_payload = {
            "tool_name": self._tool_info["name"],
            "tool_call_id": principal_state.get('current_tool_call_id'),
            "is_error": overall_dispatch_op_status.startswith("TOTAL_FAILURE"),
            "content": final_tool_output_content
        }

        principal_state.setdefault('inbox', []).append({
            "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
            "source": "TOOL_RESULT",
            "payload": tool_result_payload,
            "consumption_policy": "consume_on_read",
            "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
        })
        
        logger.info("dispatcher_post_async_completed", extra={"overall_status": overall_dispatch_op_status})
        
        principal_state["current_action"] = None
        
        try:
            from ...events.event_triggers import trigger_view_model_update

            await trigger_view_model_update(shared, "flow_view")
            await trigger_view_model_update(shared, "timeline_view")
            events_for_post_sync = shared['refs']['run']['runtime'].get("event_manager")
            if events_for_post_sync:
                await events_for_post_sync.emit_turns_sync(shared)
            await trigger_view_model_update(shared, "kanban_view")
        except Exception as e:
            logger.error("dispatcher_view_model_update_failed", extra={"error": str(e)}, exc_info=True)

        return "default"

    async def run_batch_async(self, shared: Dict, prep_res_list: List[Dict]) -> List[Dict]:
        tasks = []
        for prep_item in prep_res_list:
            tasks.append(self.exec_async(prep_item))
        
        exec_res_list = await asyncio.gather(*tasks, return_exceptions=True)

        processed_exec_res_list = []
        for i, res_or_exc in enumerate(exec_res_list):
            original_prep_item = prep_res_list[i]
            module_id_for_error = original_prep_item.get("module_to_execute", {}).get("module_id", "unknown_module")

            if isinstance(res_or_exc, Exception):
                logger.error("dispatcher_batch_exec_exception", extra={"module_id": module_id_for_error, "error": str(res_or_exc)}, exc_info=res_or_exc)
                processed_exec_res_list.append({
                    "executing_associate_id": f"ErrorAssoc_{module_id_for_error}",
                    "module_id": module_id_for_error,
                    "agent_profile_logical_name_used": original_prep_item.get("resolved_profile_logical_name", "unknown_profile"),
                    "status_of_associate_execution": "error",
                    "deliverables_from_associate": {"error": f"Dispatcher critical error during parallel execution: {str(res_or_exc)}"},
                    "error_detail_from_associate": str(res_or_exc)
                })
            elif "error" in res_or_exc:
                logger.error("dispatcher_batch_exec_error", extra={"module_id": module_id_for_error, "error": res_or_exc['error']})
                processed_exec_res_list.append({
                    "executing_associate_id": f"ErrorAssoc_{module_id_for_error}",
                    "module_id": module_id_for_error,
                    "agent_profile_logical_name_used": original_prep_item.get("resolved_profile_logical_name", "unknown_profile"),
                    "status_of_associate_execution": "error",
                    "deliverables_from_associate": {"error": res_or_exc['error']},
                    "error_detail_from_associate": res_or_exc['error']
                })
            else:
                processed_exec_res_list.append(res_or_exc)
        return processed_exec_res_list
