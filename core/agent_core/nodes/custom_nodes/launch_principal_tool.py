import logging
import asyncio
import copy
import json
import uuid # Import uuid
from typing import Dict, Any, Optional

from pocketflow import AsyncNode
from ...framework.tool_registry import tool_registry
from ...state.management import create_principal_context
from ...framework.profile_utils import get_active_profile_instance_id_by_name
from datetime import datetime, timezone
from ...flow import run_principal_async
from ...framework.handover_service import HandoverService

logger = logging.getLogger(__name__)

@tool_registry(
    name="LaunchPrincipalExecutionTool",
    description="Called by the Partner Agent to launch the Principal Agent's execution flow (fresh start or iteration) based on the confirmed plan and team configuration. Returns an error if the Principal is already running.",
    parameters={
        "type": "object",
        "properties": {
            "iteration_mode": {
                "type": "string",
                "enum": ["start_fresh", "continue_from_previous"],
                "description": "Specifies the launch mode for the Principal. 'start_fresh' indicates a completely new start, while 'continue_from_previous' indicates an iteration based on the results of the previous round."
            },
            "force_terminate_and_relaunch": {
                "type": "boolean",
                "description": "If true and the Principal is running, forcibly terminate the current Principal task, archive its state, and then proceed with the launch. Defaults to false.",
                "default": False
            }
        },
        "required": ["iteration_mode"]
    },
    handover_protocol="partner_to_principal_initial_briefing"
)
class LaunchPrincipalExecutionTool(AsyncNode):
    """
    LaunchPrincipalExecutionTool (Partner's tool):
    Responsible for initiating the Principal Agent's workflow.
    It prepares the principal_context and starts the run_principal_async flow.
    """

    async def prep_async(self, shared: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preparation stage.
        Extracts control parameters and prepares the necessary context references for both exec_async and post_async.
        """
        logger.info("launch_principal_prep_async_started")
        partner_state = shared.get("state")
        if not partner_state:
            logger.error("launch_principal_prep_state_missing")
            return {"error": "Partner context state missing."}

        current_action = partner_state.get("current_action", {})
        if not current_action:
            logger.error("launch_principal_prep_current_action_missing")
            return {"error": "Current action missing in partner state."}

        # Return a complete dictionary containing all information needed for subsequent steps
        return {
            # Control parameters
            "iteration_mode": current_action.get("iteration_mode"),
            "force_terminate_and_relaunch": current_action.get("force_terminate_and_relaunch", False),
            
            # Complete context reference for exec_async
            "shared_context_for_exec": shared,
            
            # Specific state reference for post_async
            "partner_context_state_ref": partner_state
        }

    async def exec_async(self, prep_res: Dict[str, Any]) -> Dict[str, Any]:
        if "error" in prep_res:
            return prep_res

        partner_context = prep_res.get("shared_context_for_exec")
        if not partner_context:
            return {"error": "Internal error: shared_context_for_exec not found in prep_res."}

        run_context_global = partner_context['refs']['run']
        team_state_global = run_context_global["team_state"]
        current_action = partner_context.get("state", {}).get("current_action", {})

        iteration_mode = prep_res["iteration_mode"]

        # --- START: Pre-launch check for an actionable plan ---
        # In 'start_fresh' mode, ensure that there is a plan with pending tasks before launching the Principal.
        if iteration_mode == "start_fresh":
            work_modules = team_state_global.get("work_modules", {})
            # Define what constitutes a "non-completed" status that requires action.
            non_completed_statuses = {"pending", "pending_review", "in_progress", "ongoing"}
            
            # Check if there is at least one module in a non-completed state.
            has_uncompleted_modules = any(
                module.get("status") in non_completed_statuses for module in work_modules.values()
            )

            # If the plan is empty or contains no actionable tasks, prevent launch and guide the Partner.
            if not work_modules or not has_uncompleted_modules:
                logger.warning("launch_principal_prevented_empty_plan", extra={"run_id": run_context_global['meta'].get("run_id")})
                return {
                    "status": "ignored_empty_plan",
                    "message": "The project plan (Work Modules) is empty or contains no pending tasks. You MUST call the 'manage_work_modules' tool first to create a plan before launching the Principal."
                }
        # --- END: Pre-launch check for an actionable plan ---
        force_terminate_and_relaunch = prep_res.get("force_terminate_and_relaunch", False)
        principal_sub_context_obj = None
        run_id_for_log = run_context_global['meta'].get("run_id", "UNKNOWN_RUN_ID")
        last_turn_id_for_new_flow = None

        principal_is_running_in_team_state = team_state_global.get("is_principal_flow_running", False)
        principal_task_handle = run_context_global['runtime'].get("principal_flow_task_handle")
        principal_task_is_active = principal_task_handle and not principal_task_handle.done()

        if principal_is_running_in_team_state != principal_task_is_active:
            logger.warning("launch_principal_status_mismatch", extra={
            "run_id": run_id_for_log,
            "team_state_running": principal_is_running_in_team_state,
            "task_handle_active": principal_task_is_active
        })
            team_state_global["is_principal_flow_running"] = principal_task_is_active
            principal_is_running_in_team_state = principal_task_is_active
        
        current_principal_is_effectively_running = principal_is_running_in_team_state

        if current_principal_is_effectively_running:
            if force_terminate_and_relaunch:
                logger.info("launch_principal_force_terminating", extra={"run_id": run_id_for_log})

                existing_principal_sub_context = run_context_global['sub_context_refs'].get("_principal_context_ref")
                turn_manager = run_context_global['runtime'].get('turn_manager')

                if existing_principal_sub_context and turn_manager:
                    # Archive messages
                    p_state = existing_principal_sub_context["state"]
                    if "archived_messages_history" not in p_state: p_state["archived_messages_history"] = []
                    p_state["archived_messages_history"].append({
                        "iteration": p_state.get("current_iteration_count", "unknown_forced_termination"),
                        "timestamp": datetime.now().isoformat(),
                        "messages": copy.deepcopy(p_state.get("messages", [])),
                        "reason_for_archival": "Forced termination by Partner"
                    })
                    logger.info("launch_principal_messages_archived", extra={"run_id": run_id_for_log, "reason": "forced_termination"})

                    # Find and mark old running turns as interrupted
                    last_principal_turn = turn_manager._get_turn_by_id(team_state_global, p_state.get("last_turn_id"))
                    
                    if last_principal_turn:
                        old_flow_id = last_principal_turn.get("flow_id")
                        for turn in team_state_global.get("turns", []):
                            if turn.get("flow_id") == old_flow_id and turn.get("status") == "running":
                                turn["status"] = "interrupted"
                                turn["end_time"] = datetime.now(timezone.utc).isoformat()
                                turn["error_details"] = "Flow was terminated and restarted by the user."
                        
                        # Call TurnManager to inject a delimiter turn
                        delimiter_turn_id = turn_manager.create_restart_delimiter_turn(
                            team_state=team_state_global,
                            run_id=run_id_for_log,
                            old_flow_id=old_flow_id,
                            source_turn_id=last_principal_turn.get("turn_id")
                        )
                        
                        # Pass the baton to the delimiter turn
                        last_turn_id_for_new_flow = delimiter_turn_id

                if principal_task_handle:
                    principal_task_handle.cancel()
                    try:
                        await asyncio.wait_for(principal_task_handle, timeout=5.0)
                    except asyncio.CancelledError:
                        logger.info("launch_principal_task_cancelled", extra={"run_id": run_id_for_log})
                    except asyncio.TimeoutError:
                        logger.warning("launch_principal_cancel_timeout", extra={"run_id": run_id_for_log})
                    except Exception as e:
                        logger.error("launch_principal_cancel_error", extra={"run_id": run_id_for_log, "error": str(e)}, exc_info=True)
                else:
                    logger.warning("launch_principal_no_task_handle", extra={"run_id": run_id_for_log})

                team_state_global["is_principal_flow_running"] = False
                logger.info("launch_principal_team_state_updated", extra={"run_id": run_id_for_log, "is_running": False, "reason": "force_termination"})
                
                run_context_global['runtime']["principal_flow_task_handle"] = None
                current_principal_is_effectively_running = False
            else:
                logger.warning("launch_principal_already_running", extra={"run_id": run_id_for_log, "force_terminate": False})
                status_summary = "Principal is currently active."
                existing_principal_sub_context = run_context_global['sub_context_refs'].get("_principal_context_ref")
                if existing_principal_sub_context and "state" in existing_principal_sub_context:
                    p_state = existing_principal_sub_context["state"]
                    status_summary_for_partner = p_state.get("status_summary_for_partner", {})
                    current_stage = status_summary_for_partner.get("current_stage", "unknown")
                    status_summary = f"Principal is active. Current stage: {current_stage}, Iteration: {p_state.get('current_iteration_count', 'N/A')}."
                
                return {
                    "status": "ignored_principal_running",
                    "message": "Principal is already running. Launch command ignored.",
                    "current_principal_status": status_summary
                }

        if iteration_mode == "start_fresh":
            logger.info("launch_principal_start_fresh_mode", extra={"run_id": run_id_for_log})
            
            confirmed_profile_names_list = current_action.get("confirmed_associate_profiles_details_list")

            agent_profiles_store_global = run_context_global['config'].get("agent_profiles_store", {})
            resolved_instance_ids_for_principal = []
            if confirmed_profile_names_list:
                for profile_name in confirmed_profile_names_list:
                    instance_id = get_active_profile_instance_id_by_name(agent_profiles_store_global, profile_name)
                    if instance_id:
                        resolved_instance_ids_for_principal.append(instance_id)
                    else:
                        logger.warning("launch_principal_profile_not_found", extra={"profile_name": profile_name})
            
            team_state_global["profiles_list_instance_ids"] = resolved_instance_ids_for_principal
            logger.info("launch_principal_profiles_updated", extra={"run_id": run_id_for_log, "mode": "start_fresh", "profile_count": len(resolved_instance_ids_for_principal)})

            try:
                # Call HandoverService, passing the Partner's full context directly
                inbox_item_data = await HandoverService.execute(
                    "partner_to_principal_initial_briefing", 
                    partner_context
                )

            except Exception as e:
                logger.error("launch_principal_handover_failed", extra={"error": str(e)}, exc_info=True)
                return {"error": f"Failed to prepare context handover: {e}"}

            partner_agent_id = partner_context.get("meta", {}).get("agent_id")
            principal_sub_context_obj = create_principal_context(
                run_context_ref=run_context_global, 
                parent_agent_id=partner_agent_id,
                iteration_mode=iteration_mode
            )

            # Set the "parent turn" for the new flow to be the delimiter turn, if one was created
            if last_turn_id_for_new_flow:
                principal_sub_context_obj["state"]["last_turn_id"] = last_turn_id_for_new_flow

            run_context_global['sub_context_refs']["_principal_context_ref"] = principal_sub_context_obj
            
            principal_sub_context_obj["state"].setdefault("inbox", []).append({
                "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
                "source": inbox_item_data["source"],
                "payload": inbox_item_data["payload"],
                "consumption_policy": "consume_on_read",
                "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
            })
            logger.info("launch_principal_handover_injected")
            logger.info("launch_principal_subcontext_created", extra={"mode": "start_fresh", "iteration_count": principal_sub_context_obj['state']['current_iteration_count']})

        elif iteration_mode == "continue_from_previous":
            logger.info("launch_principal_continue_mode", extra={"run_id": run_id_for_log})
            principal_sub_context_obj = run_context_global['sub_context_refs'].get("_principal_context_ref")
            if not principal_sub_context_obj:
                logger.error("launch_principal_continue_no_context", extra={"run_id": run_id_for_log})
                return {"error": "LaunchPrincipalExecutionTool: Cannot 'continue_from_previous' as no existing Principal SubContext found."}

            p_state = principal_sub_context_obj["state"]
            
            if "archived_messages_history" not in p_state: p_state["archived_messages_history"] = []
            p_state["archived_messages_history"].append({
                "iteration": p_state.get("current_iteration_count", "unknown"),
                "timestamp": datetime.now().isoformat(),
                "messages": copy.deepcopy(p_state.get("messages", []))
            })
            
            p_state["current_iteration_count"] = p_state.get("current_iteration_count", 0) + 1
            logger.info("launch_principal_continuing", extra={"run_id": run_id_for_log, "new_iteration": p_state['current_iteration_count']})

            p_state["tool_calls"] = [] 
            p_state["current_action"] = None 
            p_state["current_tool_call_id"] = None 
            
            p_state["execution_milestones"] = [] 
            p_state["status_summary_for_partner"] = { 
                "is_completed_by_principal": False,
                "current_stage": "initializing_iteration", 
                "plan_progress": {"completed": 0, "total": 0}, 
                "key_findings_summary": "", 
                "blockers": "", 
                "last_update_timestamp": datetime.now().isoformat()
            }

            new_directives = current_action.get("new_directives_for_iteration", {})
            if new_directives:
                if new_directives.get("user_query_adjustment"):
                    team_state_global["question"] = new_directives["user_query_adjustment"]
                    logger.info("launch_principal_question_updated", extra={"run_id": run_id_for_log})

                if new_directives.get("plan_modifications"):
                    logger.warning("launch_principal_plan_modifications_ignored", extra={"run_id": run_id_for_log})
                
                directive_message_content = f"Starting new iteration {p_state.get('current_iteration_count', 'N/A')}.\n"
                if new_directives.get("user_query_adjustment"):
                    directive_message_content += f"New focus/query: {new_directives['user_query_adjustment']}\n"
                if new_directives.get("additional_notes"):
                    directive_message_content += f"Additional notes: {new_directives['additional_notes']}\n"
                
                inbox_item = {
                    "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
                    "source": "PARTNER_DIRECTIVE",
                    "payload": {"directive_type": "iteration_start", "content": directive_message_content.strip()},
                    "consumption_policy": "consume_on_read",
                    "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                }
                p_state.setdefault("inbox", []).append(inbox_item)
                logger.info("launch_principal_directive_added", extra={"run_id": run_id_for_log})
            
            confirmed_profile_names_list_iter = current_action.get("confirmed_associate_profiles_details_list")
            if confirmed_profile_names_list_iter:
                agent_profiles_store_global_iter = run_context_global['config'].get("agent_profiles_store", {})
                resolved_instance_ids_for_iter = []
                for profile_name in confirmed_profile_names_list_iter:
                    instance_id = get_active_profile_instance_id_by_name(agent_profiles_store_global_iter, profile_name)
                    if instance_id:
                        resolved_instance_ids_for_iter.append(instance_id)
                    else:
                         logger.warning("launch_principal_iteration_profile_not_found", extra={"profile_name": profile_name})
                team_state_global["profiles_list_instance_ids"] = resolved_instance_ids_for_iter 
                logger.info("launch_principal_profiles_updated", extra={"run_id": run_id_for_log, "mode": "iteration", "profile_count": len(resolved_instance_ids_for_iter)})
            
            principal_sub_context_obj.get("meta", {})["iteration_mode"] = iteration_mode

        if not principal_sub_context_obj:
             logger.error("launch_principal_subcontext_not_established", extra={"run_id": run_id_for_log})
             return {"error": "LaunchPrincipalExecutionTool: Principal SubContext not established."}

        try:
            principal_completion_event = asyncio.Event()
            run_context_global['runtime']["principal_completion_event"] = principal_completion_event
            logger.info("launch_principal_completion_event_created", extra={"run_id": run_id_for_log})

            principal_run_id_from_meta = principal_sub_context_obj['meta'].get('run_id')
            logger.info("launch_principal_flow_launching", extra={
                "principal_run_id": principal_run_id_from_meta,
                "iteration_count": principal_sub_context_obj['state']['current_iteration_count']
            })
            principal_task = asyncio.create_task(run_principal_async(principal_context=principal_sub_context_obj))
            run_context_global['runtime']["principal_flow_task_handle"] = principal_task

            team_state_global["is_principal_flow_running"] = True 
            logger.info("launch_principal_team_state_updated", extra={"run_id": run_id_for_log, "is_running": True})
            
            event_manager = run_context_global['runtime'].get("event_manager")
            websocket_obj = event_manager.websocket if event_manager else None
            
            parent_run_id_for_key_gen = run_context_global['meta'].get("run_id")
            iteration_count_for_key_gen = principal_sub_context_obj['state']['current_iteration_count']
            principal_subtask_internal_key = f"{parent_run_id_for_key_gen}_principal_{iteration_count_for_key_gen}"

            if websocket_obj and hasattr(websocket_obj.state, 'active_run_tasks'):
                websocket_obj.state.active_run_tasks[principal_subtask_internal_key] = principal_task
                logger.info("launch_principal_subtask_registered", extra={
                    "subtask_key": principal_subtask_internal_key,
                    "parent_run_id": parent_run_id_for_key_gen
                })
            else:
                logger.error("launch_principal_subtask_registration_failed", extra={"subtask_key": principal_subtask_internal_key})

            run_context_global['runtime']["current_principal_subtask_id"] = principal_subtask_internal_key
            logger.info("launch_principal_subtask_id_stored", extra={
                "subtask_key": principal_subtask_internal_key,
                "parent_run_id": parent_run_id_for_key_gen
            })

            import functools 
            callback_partial = functools.partial(
                self._principal_flow_done_callback,
                principal_run_id=principal_run_id_from_meta, 
                run_context_ref=run_context_global
            )
            principal_task.add_done_callback(callback_partial)
            
            return {
                "status": f"principal_launched_{iteration_mode}",
                "principal_run_id": principal_run_id_from_meta,
                "iteration_count": principal_sub_context_obj["state"]["current_iteration_count"],
                "message": f"Principal execution flow ({iteration_mode}) has been successfully launched.",
                "completion_event_for_partner": principal_completion_event
            }
        except Exception as e:
            logger.error("launch_principal_flow_failed", extra={"run_id": run_id_for_log, "error": str(e)}, exc_info=True)
            return {"error": f"Failed to launch Principal flow: {str(e)}"}

    async def post_async(self, shared: Dict[str, Any], prep_res: Dict[str, Any], exec_res: Dict[str, Any]):
        partner_private_state = prep_res.get("partner_context_state_ref") or prep_res.get("partner_context_state_ref_on_error")
        run_context_global = prep_res.get("run_context_global") or prep_res.get("run_context_global_ref_on_error")
        
        if not partner_private_state:
            logger.error("launch_principal_post_partner_state_missing")
            if run_context_global and run_context_global.get("team_state") and "error" in exec_res and exec_res.get("status") != "ignored_principal_running":
                run_context_global["team_state"]["is_principal_flow_running"] = False
                logger.info("launch_principal_post_team_state_updated", extra={
                    "run_id": run_context_global['meta'].get('run_id'),
                    "is_running": False,
                    "reason": "launch_failure_partner_state_missing"
                })
            return "default" 

        is_error = "error" in exec_res or exec_res.get("status") == "ignored_principal_running"
        
        # --- Inbox Migration ---
        serializable_content = exec_res.copy()
        completion_event = serializable_content.pop("completion_event_for_partner", None)
        if completion_event:
            shared['runtime_objects']["active_principal_completion_event"] = completion_event
            logger.info("launch_principal_completion_event_stored")

        tool_result_payload = {
            "tool_name": "LaunchPrincipalExecutionTool",
            "tool_call_id": partner_private_state.get('current_tool_call_id'),
            "is_error": is_error,
            "content": serializable_content
        }

        partner_private_state.setdefault('inbox', []).append({
            "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
            "source": "TOOL_RESULT",
            "payload": tool_result_payload,
            "consumption_policy": "consume_on_read",
            "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
        })
        # --- End Inbox Migration ---
            
        if "principal_launch_config_history" not in partner_private_state:
            partner_private_state["principal_launch_config_history"] = []
        
        launch_config_entry = {
            "timestamp": datetime.now().isoformat(),
            "iteration_mode": prep_res.get("iteration_mode"),
            "launch_result": serializable_content
        }
        if prep_res.get("iteration_mode") == "start_fresh":
            launch_config_entry["briefing"] = prep_res.get("initial_briefing")
            launch_config_entry["profiles_count"] = len(prep_res.get("confirmed_profiles_list", []))
        else:
            launch_config_entry["new_directives"] = prep_res.get("new_directives")
            if prep_res.get("confirmed_profiles_list") is not None:
                launch_config_entry["profiles_count_updated"] = len(prep_res.get("confirmed_profiles_list", []))

        partner_private_state["principal_launch_config_history"].append(launch_config_entry)

        partner_private_state["current_action"] = None
        
        if run_context_global and run_context_global.get("team_state") and \
           "error" in exec_res and exec_res.get("status") != "ignored_principal_running":
            run_context_global["team_state"]["is_principal_flow_running"] = False
            logger.info("launch_principal_post_team_state_updated", extra={
                "run_id": run_context_global['meta'].get('run_id'),
                "is_running": False,
                "reason": "launch_failure",
                "error": exec_res.get('error', 'Unknown launch error')
            })
        
        return "default"

    def _principal_flow_done_callback(self, task: asyncio.Task, principal_run_id: str, run_context_ref: Optional[Dict]):
        log_prefix = f"Principal Flow Callback (Principal Run ID: {principal_run_id}, Task Name: {task.get_name()}):"
        final_task_status_for_log = "unknown_completion"
        parent_run_id_for_key = run_context_ref['meta'].get("run_id") if run_context_ref else "UNKNOWN_PARENT_RUN_ID"

        try:
            if task.cancelled():
                result_package = {"status": "CANCELLED", "final_summary": "Principal flow was cancelled.", "error_details": None}
            else:
                result_package = task.result()
            
            final_task_status_for_log = result_package.get("status", "unknown").lower()
            logger.info("launch_principal_flow_finished", extra={"status": final_task_status_for_log, "principal_run_id": principal_run_id})

            if run_context_ref:
                partner_sub_context = run_context_ref['sub_context_refs'].get("_partner_context_ref")
                if partner_sub_context and partner_sub_context.get("state"):
                    partner_private_state_cb = partner_sub_context["state"]
                    
                    # --- Inbox Migration ---
                    inbox_item_payload = {
                        "status": result_package.get("status"),
                        "summary": result_package.get("final_summary"),
                        "error": result_package.get("error_details"),
                        "deliverables": result_package.get("deliverables")
                    }
                    partner_private_state_cb.setdefault("inbox", []).append({
                        "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
                        "source": "PRINCIPAL_COMPLETED",
                        "payload": inbox_item_payload,
                        "consumption_policy": "consume_on_read",
                        "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                    })
                    logger.info("launch_principal_completion_inbox_added", extra={"principal_run_id": principal_run_id})
                    # --- End Inbox Migration ---

                    # Set the event to wake up the Partner agent
                    completion_event = run_context_ref['runtime'].get("principal_completion_event")
                    if completion_event and not completion_event.is_set():
                        completion_event.set()
                        logger.info("launch_principal_completion_event_set", extra={"principal_run_id": principal_run_id})
                else:
                    logger.error("launch_principal_partner_context_missing", extra={"principal_run_id": principal_run_id})

        except Exception as e:
            logger.error("launch_principal_callback_error", extra={"principal_run_id": principal_run_id, "error": str(e)}, exc_info=True)
            final_task_status_for_log = "finished_with_callback_error"
        finally:
            if run_context_ref and run_context_ref.get("team_state"):
                team_state_global_cb = run_context_ref["team_state"]
                sessions = team_state_global_cb.get("principal_execution_sessions", [])
                if sessions and sessions[-1].get("end_time") is None:
                    last_session = sessions[-1]
                    last_session["end_time"] = datetime.now(timezone.utc).isoformat()
                    if task.cancelled():
                        last_session["termination_reason"] = "cancelled"
                    elif task.exception():
                        last_session["termination_reason"] = "error"
                    else:
                        result_package = task.result() if not task.exception() else {}
                        if result_package.get("status") == "COMPLETED_WITH_ERROR":
                            last_session["termination_reason"] = "error"
                        else:
                            last_session["termination_reason"] = "completed_successfully"
                    logger.info("launch_principal_session_ended", extra={
                        "session_id": last_session['session_id'],
                        "termination_reason": last_session['termination_reason']
                    })

            if run_context_ref and run_context_ref.get("team_state"):
                run_context_ref["team_state"]["is_principal_flow_running"] = False
                logger.info("launch_principal_team_state_callback_updated", extra={
                    "principal_run_id": principal_run_id,
                    "parent_run_id": parent_run_id_for_key,
                    "task_outcome": final_task_status_for_log,
                    "is_running": False
                })
            else:
                logger.error("launch_principal_callback_context_missing", extra={"parent_run_id": parent_run_id_for_key})

            if run_context_ref:
                try:
                    from ...events.event_triggers import trigger_view_model_update
                    asyncio.create_task(
                        trigger_view_model_update(run_context_ref, "timeline_view")
                    )
                    logger.info("launch_principal_timeline_view_triggered", extra={"principal_run_id": principal_run_id})
                except Exception as e_trigger:
                    logger.error("launch_principal_timeline_view_failed", extra={"principal_run_id": principal_run_id, "error": str(e_trigger)}, exc_info=True)

            principal_subtask_internal_key_to_clear = run_context_ref['runtime'].get("current_principal_subtask_id") if run_context_ref else None
            event_manager_cb = run_context_ref['runtime'].get("event_manager") if run_context_ref else None
            websocket_obj = event_manager_cb.websocket if event_manager_cb else None

            if websocket_obj and hasattr(websocket_obj.state, 'active_run_tasks') and principal_subtask_internal_key_to_clear:
                if principal_subtask_internal_key_to_clear in websocket_obj.state.active_run_tasks:
                    del websocket_obj.state.active_run_tasks[principal_subtask_internal_key_to_clear]
                    logger.info("launch_principal_subtask_cleared", extra={
                        "subtask_key": principal_subtask_internal_key_to_clear,
                        "parent_run_id": parent_run_id_for_key
                    })
                else:
                    logger.warning("launch_principal_subtask_not_found", extra={
                        "subtask_key": principal_subtask_internal_key_to_clear,
                        "parent_run_id": parent_run_id_for_key
                    })
            elif run_context_ref and not principal_subtask_internal_key_to_clear:
                logger.warning("launch_principal_subtask_id_none", extra={"parent_run_id": parent_run_id_for_key})
            
            if run_context_ref:
                if run_context_ref['runtime'].get("principal_flow_task_handle") is task:
                    run_context_ref['runtime']["principal_flow_task_handle"] = None
                if run_context_ref['runtime'].get("current_principal_subtask_id") == principal_subtask_internal_key_to_clear:
                    run_context_ref['runtime']["current_principal_subtask_id"] = None
                logger.info("launch_principal_runtime_cleared", extra={"parent_run_id": parent_run_id_for_key})
