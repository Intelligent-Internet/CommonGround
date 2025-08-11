import asyncio
import logging
from typing import Dict, Optional, Any, Tuple, List # Added List
import coolname
import uuid
import copy # Needed for deepcopy
from datetime import datetime, timezone # Needed for profile updates timestamp
from pathlib import Path
from agent_core.iic.core.iic_handlers import find_iic_file_by_run_id
from agent_core.iic.parser.parser import parse_iic
from agent_core.state.management import _inject_restored_state
import json

# Add context variable imports
from agent_core.config.logging_config import run_id_var

# fastapi.WebSocket is no longer directly used in handlers
from agent_core.flow import run_principal_async, run_partner_interaction_async
from agent_core.state.management import create_run_context
from api.session import active_runs_store
from agent_core.framework.tool_registry import get_all_toolsets_with_tools
from agent_core.framework.profile_utils import get_active_profile_by_name # For profile updates
from agent_profiles.loader import get_global_active_profile_by_logical_name_copy # For profile updates from global templates
from agent_core.events.event_triggers import trigger_view_model_update # For view model updates
from agent_core.nodes.custom_nodes.stage_planner_node import _apply_work_module_actions # For direct work module management
from agent_core.utils.serialization import get_serializable_run_snapshot # New import

logger = logging.getLogger(__name__)

# Mappings for special base_profile_logical_name values
GENERIC_PROFILE_MAPPING = {
    "*GENERIC_ASSOCIATE*": "Associate_GenericExecutor_EN",
    "*DEFAULT_PRINCIPAL*": "Principal", # Assuming Principal is a suitable generic principal
    # Add more mappings if needed
}
DEFAULT_GENERIC_FALLBACK_PROFILE_NAME = "Associate_GenericExecutor_EN"


async def _apply_profile_updates_in_run_context(run_context: Dict, profile_updates_list: List[Dict]):
    """
    Applies profile updates to the run_context's agent_profiles_store based on specified actions.
    Actions can be CREATE, UPDATE, DISABLE, RENAME.
    """
    if not run_context or not isinstance(profile_updates_list, list):
        logger.warning("invalid_profile_update_arguments", extra={"run_context_provided": run_context is not None, "profile_updates_list_type": type(profile_updates_list).__name__})
        return

    # V4.1: Access config via refs.run.config (or directly if run_context is the global one)
    # Assuming run_context here is the global RunContext.
    profiles_store = run_context['config'].setdefault("agent_profiles_store", {})
    event_manager = run_context['runtime'].get("event_manager") # event_manager is directly in runtime
    run_id = run_context['meta'].get("run_id")

    for update_request in profile_updates_list:
        if not isinstance(update_request, dict):
            logger.warning("profile_update_request_not_dict", extra={"update_request": update_request, "type": type(update_request).__name__})
            continue

        action = update_request.get("action")
        profile_logical_name = update_request.get("profile_logical_name") # Used by all actions for target/new name
        updates_to_apply = update_request.get("updates_to_apply", {})

        if not action or not profile_logical_name:
            logger.warning("profile_update_missing_required_fields", extra={"update_request": update_request, "has_action": bool(action), "has_profile_logical_name": bool(profile_logical_name)})
            continue
        
        logger.info("profile_update_processing_started", extra={"run_id": run_id, "action": action, "profile_logical_name": profile_logical_name})

        base_profile_dict: Optional[Dict] = None
        new_profile_instance: Optional[Dict] = None
        original_profile_for_event: Optional[Dict] = None # For event details

        # --- Action: CREATE ---
        if action == "CREATE":
            base_on_template_key = update_request.get("base_on_template_key")
            base_on_profile_logical_name = update_request.get("base_on_profile_logical_name")

            if base_on_template_key:
                template_logical_name = GENERIC_PROFILE_MAPPING.get(base_on_template_key, DEFAULT_GENERIC_FALLBACK_PROFILE_NAME)
                base_profile_dict = get_global_active_profile_by_logical_name_copy(template_logical_name)
                if not base_profile_dict:
                    logger.error("create_action_template_not_found", extra={"template_logical_name": template_logical_name, "base_on_template_key": base_on_template_key}, exc_info=True)
                    continue
            elif base_on_profile_logical_name:
                base_profile_dict = get_active_profile_by_name(profiles_store, base_on_profile_logical_name)
                if not base_profile_dict: # Try global if not in run_context
                    base_profile_dict = get_global_active_profile_by_logical_name_copy(base_on_profile_logical_name)
                if not base_profile_dict:
                    logger.error("create_action_base_profile_not_found", extra={"base_on_profile_logical_name": base_on_profile_logical_name}, exc_info=True)
                    continue
            else: # Create from scratch (minimal default profile)
                base_profile_dict = {
                    "name": profile_logical_name, "type": "associate", "rev": 0,
                    "is_active": False, "is_deleted": False, "timestamp": datetime.now(timezone.utc).isoformat(),
                    "profile_id": str(uuid.uuid4()), "llm_config_ref": "associate_llm",
                    "input_construction_config": {"tool_access_policy": {}}, "text_definitions": {}
                }
                logger.info("create_action_minimal_default", extra={"profile_logical_name": profile_logical_name})

            # Deactivate any existing active profile with the same new logical name before creating
            for inst_id, prof in list(profiles_store.items()):
                if prof.get("name") == profile_logical_name and prof.get("is_active") is True:
                    profiles_store[inst_id]["is_active"] = False
                    logger.info("create_action_deactivated_existing", extra={"existing_profile_name": prof.get('name'), "instance_id": inst_id, "new_profile_logical_name": profile_logical_name})

            new_profile_instance = copy.deepcopy(base_profile_dict)
            new_profile_instance["profile_id"] = str(uuid.uuid4())
            new_profile_instance["name"] = profile_logical_name
            new_profile_instance["rev"] = 1 # First revision for this logical name (or new variant)
            new_profile_instance["timestamp"] = datetime.now(timezone.utc).isoformat()
            new_profile_instance["is_active"] = True
            new_profile_instance["is_deleted"] = False
            original_profile_for_event = base_profile_dict

        # --- Action: UPDATE ---
        elif action == "UPDATE":
            original_profile_for_event = get_active_profile_by_name(profiles_store, profile_logical_name)
            if not original_profile_for_event:
                logger.error("update_action_profile_not_found", extra={"profile_logical_name": profile_logical_name}, exc_info=True)
                continue
            
            new_profile_instance = copy.deepcopy(original_profile_for_event)
            new_profile_instance["profile_id"] = str(uuid.uuid4())
            new_profile_instance["rev"] = original_profile_for_event.get("rev", 0) + 1
            new_profile_instance["timestamp"] = datetime.now(timezone.utc).isoformat()
            new_profile_instance["is_active"] = True
            new_profile_instance["is_deleted"] = False # Ensure it's not marked deleted

            # Deactivate other active versions of the same logical name
            for inst_id, prof in list(profiles_store.items()):
                if prof.get("name") == profile_logical_name and prof.get("is_active") is True:
                    profiles_store[inst_id]["is_active"] = False
                    logger.info("update_action_deactivated_old_version", extra={"profile_name": prof.get('name'), "instance_id": inst_id, "revision": prof.get('rev')})
        
        # --- Action: DISABLE ---
        elif action == "DISABLE":
            disabled_count = 0
            for inst_id, prof in list(profiles_store.items()):
                if prof.get("name") == profile_logical_name and prof.get("is_active") is True:
                    profiles_store[inst_id]["is_active"] = False
                    disabled_count += 1
                    logger.info("disable_action_profile_disabled", extra={"profile_name": prof.get('name'), "instance_id": inst_id, "revision": prof.get('rev')})
            if disabled_count == 0:
                logger.info("disable_action_no_active_profile", extra={"profile_logical_name": profile_logical_name})
            # For DISABLE, new_profile_instance remains None. Event details will reflect this.
            original_profile_for_event = {"name": profile_logical_name, "action_taken": "DISABLE", "disabled_count": disabled_count}


        # --- Action: RENAME ---
        elif action == "RENAME":
            new_logical_name_for_rename = update_request.get("new_logical_name_for_rename")
            if not new_logical_name_for_rename:
                logger.error("rename_action_missing_new_name", extra={"profile_logical_name": profile_logical_name}, exc_info=True)
                continue
            
            original_profile_for_event = get_active_profile_by_name(profiles_store, profile_logical_name) # This is the "old" profile
            if not original_profile_for_event:
                logger.error("rename_action_original_not_found", extra={"profile_logical_name": profile_logical_name}, exc_info=True)
                continue

            # Deactivate any existing active profile with the new target logical name
            for inst_id, prof in list(profiles_store.items()):
                if prof.get("name") == new_logical_name_for_rename and prof.get("is_active") is True:
                    profiles_store[inst_id]["is_active"] = False
                    logger.info("rename_action_deactivated_target_conflict", extra={"existing_profile_name": prof.get('name'), "instance_id": inst_id, "new_logical_name_for_rename": new_logical_name_for_rename})

            new_profile_instance = copy.deepcopy(original_profile_for_event)
            new_profile_instance["profile_id"] = str(uuid.uuid4())
            new_profile_instance["name"] = new_logical_name_for_rename
            new_profile_instance["rev"] = 1 # Starts as rev 1 for the new logical name
            new_profile_instance["timestamp"] = datetime.now(timezone.utc).isoformat()
            new_profile_instance["is_active"] = True
            new_profile_instance["is_deleted"] = False

            # Deactivate all versions of the old logical name
            for inst_id, prof in list(profiles_store.items()):
                if prof.get("name") == profile_logical_name and prof.get("is_active") is True: # Check original name
                    profiles_store[inst_id]["is_active"] = False
                    logger.info("rename_action_deactivated_old_profile", extra={"profile_name": prof.get('name'), "instance_id": inst_id, "revision": prof.get('rev')})
        
        else: # Unknown action
            logger.warning("unknown_profile_action", extra={"action": action, "profile_logical_name": profile_logical_name})
            continue

        # Apply 'updates_to_apply' if a new profile instance was created (CREATE, UPDATE, RENAME)
        if new_profile_instance and isinstance(updates_to_apply, dict) and updates_to_apply:
            if "tool_access_policy" in updates_to_apply:
                tap_updates = updates_to_apply["tool_access_policy"]
                if isinstance(tap_updates, dict):
                    profile_tap = new_profile_instance.setdefault("input_construction_config", {}).setdefault("tool_access_policy", {})
                    if "allowed_toolsets" in tap_updates and isinstance(tap_updates["allowed_toolsets"], list):
                        profile_tap["allowed_toolsets"] = list(tap_updates["allowed_toolsets"])
                    if "allowed_individual_tools" in tap_updates and isinstance(tap_updates["allowed_individual_tools"], list):
                        profile_tap["allowed_individual_tools"] = list(tap_updates["allowed_individual_tools"])
                else:
                    logger.warning("profile_update_tap_not_dict", extra={"action": action, "profile_name": new_profile_instance['name'], "tap_updates_type": type(tap_updates).__name__})

            if "system_prompt_segments_override" in updates_to_apply:
                sps_override = updates_to_apply["system_prompt_segments_override"]
                if isinstance(sps_override, list):
                    new_profile_instance.setdefault("input_construction_config", {})["system_prompt_segments"] = sps_override
                else:
                    logger.warning("profile_update_sps_not_list", extra={"action": action, "profile_name": new_profile_instance['name'], "sps_override_type": type(sps_override).__name__})

            if "text_definitions_update" in updates_to_apply:
                td_updates = updates_to_apply["text_definitions_update"]
                if isinstance(td_updates, dict):
                    new_profile_instance.setdefault("text_definitions", {}).update(td_updates)
                else:
                    logger.warning("profile_update_td_not_dict", extra={"action": action, "profile_name": new_profile_instance['name'], "td_updates_type": type(td_updates).__name__})
            # (Future: guidance_segments_override, output_handling_config_override, exposable_as_tool_override)

        # Store the new profile instance if one was created
        if new_profile_instance:
            profiles_store[new_profile_instance["profile_id"]] = new_profile_instance
            logger.info("profile_update_successful", extra={"action": action, "run_id": run_id, "profile_name": new_profile_instance['name'], "new_instance_id": new_profile_instance['profile_id'], "revision": new_profile_instance['rev']})
            final_profile_name_for_event = new_profile_instance["name"]
            event_details_specific = {
                "updated_profile_logical_name": new_profile_instance["name"],
                "new_profile_instance_id": new_profile_instance["profile_id"],
                "new_profile_rev": new_profile_instance["rev"],
                "base_profile_logical_name_used": original_profile_for_event.get("name") if original_profile_for_event else None,
                "base_profile_instance_id_used": original_profile_for_event.get("profile_id") if original_profile_for_event else None,
                "base_profile_rev_used": original_profile_for_event.get("rev") if original_profile_for_event else None,
                "action_taken": action
            }
            if action == "RENAME":
                event_details_specific["renamed_from_logical_name"] = profile_logical_name # Original name before rename
        elif action == "DISABLE": # For DISABLE, use the original profile name for event
            final_profile_name_for_event = profile_logical_name
            event_details_specific = original_profile_for_event # This already contains action_taken and disabled_count
        else: # Should not happen if logic is correct
            logger.warning("profile_update_no_instance_created", extra={"action": action, "run_id": run_id})
            continue
            
        # Emit event
        if event_manager:
            if run_context['meta'].get("run_type") == "partner_interaction":
                partner_context_ref = run_context['sub_context_refs'].get("_partner_context_ref")
                if partner_context_ref and isinstance(partner_context_ref.get("state"), dict):
                    partner_state = partner_context_ref["state"]
                    # --- Inbox Migration ---
                    # Remove the old flag-based notification
                    # if "flags" not in partner_state: partner_state["flags"] = {}
                    # partner_state["flags"]["available_profiles_updated"] = True
                    
                    # Add an InboxItem instead
                    partner_state.setdefault("inbox", []).append({
                        "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
                        "source": "PROFILES_UPDATED_NOTIFICATION",
                        "payload": {"details": event_details_specific},
                        "consumption_policy": "consume_on_read",
                        "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                    })
                    logger.info("profile_update_notification_added", extra={"run_id": run_id, "context_type": "partner", "action": action})
                    # --- End Inbox Migration ---
            
            await event_manager.emit_run_config_updated(
                run_id=run_id,
                config_type="agent_profile",
                item_identifier=final_profile_name_for_event,
                details=event_details_specific
            )
            logger.info("profile_update_event_sent", extra={"run_id": run_id, "event_type": "run_config_updated", "profile_name": final_profile_name_for_event, "action": action})


async def handle_start_run_message(ws_state: Dict, data: Dict):
    """
    Handles the 'start_run' message to create a new, pending run instance.
    This function now also handles the logic for resuming a run from a run_id.
    """
    event_manager = ws_state.event_manager
    session_id_for_log = event_manager.session_id

    request_id = data.get("request_id")
    resume_from_run_id = data.get("resume_from_run_id")

    # --- Resume logic ---
    if resume_from_run_id:
        logger.info("resume_request_received", extra={"session_id": session_id_for_log, "resume_from_run_id": resume_from_run_id})
        if not request_id:
            logger.warning("resume_missing_request_id", extra={"session_id": session_id_for_log, "data": data})
            await event_manager.emit_error(run_id=resume_from_run_id, agent_id="System", error_message="Command 'start_run' for resume requires 'request_id'.")
            return
        try:
            iic_path_str = await find_iic_file_by_run_id(resume_from_run_id)
            if not iic_path_str:
                raise FileNotFoundError(f"No .iic file found for run_id {resume_from_run_id}")
            logger.info("resume_iic_file_found", extra={"run_id": resume_from_run_id, "iic_path": iic_path_str})

            # Construct the JSON path from the run_id, not the .iic filename,
            # to accommodate renamed .iic files. The JSON state file always uses the run_id.
            iic_path_obj = Path(iic_path_str)
            json_path = iic_path_obj.parent / f"{resume_from_run_id}.json"
            if not json_path.exists():
                raise FileNotFoundError(f"State file {json_path} not found for run_id {resume_from_run_id}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                restored_state_data = json.load(f)
            logger.info("resume_state_loaded", extra={"run_id": resume_from_run_id, "json_path": str(json_path)})

            server_run_id = resume_from_run_id
            run_id_var.set(server_run_id)  # Set context variable
            run_context = create_run_context(
                server_run_id=server_run_id,
                run_type=restored_state_data.get("meta", {}).get("run_type", "partner_interaction"),
                initial_params=restored_state_data.get("team_state", {}).get("initial_parameters", {}),
                event_manager_for_websocket=event_manager,
                project_id=data.get("project_id", restored_state_data.get("project_id", ""))
            )
            logger.info("resume_run_context_created", extra={"run_id": resume_from_run_id})

            # Inject the original file path into the run_context metadata
            if "meta" not in run_context: run_context["meta"] = {}
            run_context["meta"]["source_iic_path"] = iic_path_str
            logger.info("resume_source_path_stored", extra={"run_id": resume_from_run_id, "source_iic_path": iic_path_str})

            _inject_restored_state(run_context, restored_state_data)
            logger.info("resume_state_injected", extra={"run_id": resume_from_run_id})

            # A resumed run should immediately enter 'AWAITING_INPUT' state, not 'CREATED'.
            run_context['meta']['status'] = 'AWAITING_INPUT'
            logger.info("resume_status_set", extra={"run_id": resume_from_run_id, "status": "AWAITING_INPUT"})

            active_runs_store[server_run_id] = run_context
            
            await event_manager.emit_run_ready(server_run_id, request_id)
            
            # Send turns_sync to provide the authoritative data for rendering the conversation.
            await event_manager.emit_turns_sync(run_context)
            
            logger.info("resume_completed", extra={"run_id": server_run_id})
            
            if run_context['meta']['run_type'] == "partner_interaction":
                partner_ctx = run_context['sub_context_refs']['_partner_context_ref']
                task = asyncio.create_task(run_partner_interaction_async(partner_context=partner_ctx))
                ws_state.active_run_tasks[server_run_id] = task
                logger.info("resume_partner_task_started", extra={"run_id": server_run_id, "run_type": "partner_interaction"})

        except Exception as e:
            logger.error("resume_failed", extra={"run_id": resume_from_run_id, "error_message": str(e)}, exc_info=True)
            await event_manager.emit_error(run_id=resume_from_run_id, agent_id="System", error_message=f"Failed to resume session: {e}")
        return

    # --- Logic for creating a new run ---
    else:
        run_type = data.get("run_type")
        project_id = data.get("project_id", "")

        if not request_id or not run_type:
            logger.warning("new_run_missing_required_fields", extra={"session_id": session_id_for_log, "data": data, "has_request_id": bool(request_id), "has_run_type": bool(run_type)})
            await event_manager.emit_error(run_id=None, agent_id="System", error_message="Command 'start_run' for new run requires 'request_id' and 'run_type'.")
            return

        server_run_id = coolname.generate_slug(3)
        run_id_var.set(server_run_id)  # Set context variable
        logger.info("new_run_request_received", extra={"session_id": session_id_for_log, "request_id": request_id, "run_type": run_type, "server_run_id": server_run_id})

        try:
            initial_filename = data.get("initial_filename")
            run_context = create_run_context(
                server_run_id=server_run_id,
                run_type=run_type,
                initial_params={},
                event_manager_for_websocket=event_manager,
                project_id=project_id,
            )
            if initial_filename:
                run_context['meta']['initial_filename'] = initial_filename
            
            logger.info("new_run_context_created", extra={"run_id": server_run_id, "status": "CREATED"})
            active_runs_store[server_run_id] = run_context

            # --- NEW: Perform initial persistence BEFORE sending run_ready ---
            from agent_core.iic.core.iic_handlers import persist_initial_run_state
            await persist_initial_run_state(run_context)
            # --- END NEW ---

            await event_manager.emit_run_ready(server_run_id, request_id)

        except Exception as e:
            logger.error("new_run_creation_failed", extra={"session_id": session_id_for_log, "run_type": run_type, "server_run_id": server_run_id, "error_message": str(e)}, exc_info=True)
            await event_manager.emit_error(run_id=server_run_id, agent_id="System", error_message=f"Failed to start run type {run_type}: {str(e)}")
            if server_run_id in active_runs_store:
                del active_runs_store[server_run_id]


async def handle_stop_run_message(ws_state: Dict, data: Dict):
    """Handles messages of type 'stop_run'"""
    run_id_to_stop = data.get("run_id")
    active_runs_tasks = ws_state.active_run_tasks # Changed: Using HEAD's way
    event_manager = ws_state.event_manager # Changed: Using HEAD's way
    session_id_for_log = event_manager.session_id

    if not run_id_to_stop:
        logger.warning("stop_run_missing_run_id", extra={"session_id": session_id_for_log})
        await event_manager.emit_error(run_id=None, agent_id="System", error_message="Stop command requires a 'run_id'.")
        return

    task_to_stop = active_runs_tasks.get(run_id_to_stop)

    if task_to_stop and not task_to_stop.done():
        logger.info("stop_run_cancelling_task", extra={"session_id": session_id_for_log, "run_id": run_id_to_stop})
        task_to_stop.cancel()
        try:
            await asyncio.wait_for(task_to_stop, timeout=5.0)
        except asyncio.CancelledError:
            logger.info("stop_run_task_cancelled", extra={"run_id": run_id_to_stop, "session_id": session_id_for_log})
        except asyncio.TimeoutError:
            logger.warning("stop_run_cancellation_timeout", extra={"run_id": run_id_to_stop, "session_id": session_id_for_log})
        except Exception as e:
            logger.error("stop_run_await_error", extra={"run_id": run_id_to_stop, "session_id": session_id_for_log, "error_message": str(e)}, exc_info=True)
        
        # State updates are now handled in the flow's `except CancelledError` block
        # to prevent race conditions.
    else:
        logger.info("stop_run_no_active_task", extra={"session_id": session_id_for_log, "run_id": run_id_to_stop})
        # The 'no_active_flow_to_stop_or_already_done' status is now inferred on the client.
    
    if run_id_to_stop in active_runs_store:
        del active_runs_store[run_id_to_stop]
        logger.info("stop_run_context_removed", extra={"session_id": session_id_for_log, "run_id": run_id_to_stop})


async def handle_request_available_toolsets(ws_state: Dict, data: Dict):
    """Handles WebSocket messages from the client requesting the list of available toolsets."""
    event_manager = ws_state.event_manager # Changed: Using HEAD's way
    session_id_for_log = event_manager.session_id
    logger.debug("request_available_toolsets_received", extra={"session_id": session_id_for_log, "data": data})
    
    scope_filter = data.get("scope") 
    
    try:
        toolsets_info = get_all_toolsets_with_tools(scope_filter=scope_filter)
        
        await event_manager.send_json( 
            run_id=None, 
            message={
                "type": "available_toolsets_response",
                "data": {"toolsets": toolsets_info}
            }
        )
        logger.debug("available_toolsets_response_sent", extra={"session_id": session_id_for_log, "toolsets_count": len(toolsets_info), "scope_filter": scope_filter or "all"})
    except Exception as e:
        logger.error("request_available_toolsets_error", extra={"session_id": session_id_for_log, "error_message": str(e)}, exc_info=True)
        await event_manager.emit_error(
            run_id=None, 
            agent_id="System", 
            error_message=f"Failed to retrieve toolsets: {str(e)}"
        )

# --- Dango's new handlers, adapted for ws_state ---

async def handle_stop_managed_principal_message(ws_state: Dict, data: Dict):
    event_manager = ws_state.event_manager # Changed: Using HEAD's way
    session_id_for_log = event_manager.session_id
    managing_partner_run_id = data.get("managing_partner_run_id")

    logger.info("stop_managed_principal_received", extra={"session_id": session_id_for_log, "managing_partner_run_id": managing_partner_run_id})

    if not managing_partner_run_id:
        logger.warning("stop_managed_principal_missing_run_id", extra={"session_id": session_id_for_log})
        await event_manager.emit_error(run_id=None, agent_id="System", error_message="'stop_managed_principal' requires 'managing_partner_run_id'.")
        return

    run_context = active_runs_store.get(managing_partner_run_id)
    if not run_context or run_context['meta'].get("run_type") != "partner_interaction": # V4.1: Access meta
        logger.warning("stop_managed_principal_no_partner_context", extra={"session_id": session_id_for_log, "managing_partner_run_id": managing_partner_run_id, "run_context_exists": run_context is not None, "run_type": run_context.get('meta', {}).get('run_type') if run_context else None})
        await event_manager.emit_error(run_id=managing_partner_run_id, agent_id="System", error_message=f"No active Partner run found for ID '{managing_partner_run_id}' to stop its Principal.")
        return

    # V4.1: Access runtime for these handles
    principal_task_handle = run_context['runtime'].get("principal_flow_task_handle")
    principal_subtask_id = run_context['runtime'].get("current_principal_subtask_id")

    if principal_task_handle and not principal_task_handle.done():
        logger.info("stop_managed_principal_attempting", extra={"session_id": session_id_for_log, "principal_subtask_id": principal_subtask_id, "managing_partner_run_id": managing_partner_run_id})
        principal_task_handle.cancel()
        try:
            await asyncio.wait_for(principal_task_handle, timeout=5.0)
        except asyncio.CancelledError:
            logger.info("stop_managed_principal_cancelled", extra={"principal_subtask_id": principal_subtask_id, "managing_partner_run_id": managing_partner_run_id})
        except asyncio.TimeoutError:
            logger.warning("stop_managed_principal_timeout", extra={"principal_subtask_id": principal_subtask_id, "managing_partner_run_id": managing_partner_run_id})
        except Exception as e:
            logger.error("stop_managed_principal_await_error", extra={"principal_subtask_id": principal_subtask_id, "managing_partner_run_id": managing_partner_run_id, "error_message": str(e)}, exc_info=True)

        # Changed: Using ws_state.active_run_tasks
        if principal_subtask_id and hasattr(ws_state, 'active_run_tasks') and principal_subtask_id in ws_state.active_run_tasks:
            del ws_state.active_run_tasks[principal_subtask_id]
            logger.info("stop_managed_principal_task_removed", extra={"session_id": session_id_for_log, "principal_subtask_id": principal_subtask_id})
        
        # V4.1: Access runtime for these handles
        if run_context['runtime'].get("principal_flow_task_handle") is principal_task_handle:
             run_context['runtime']["principal_flow_task_handle"] = None
        if run_context['runtime'].get("current_principal_subtask_id") == principal_subtask_id:
             run_context['runtime']["current_principal_subtask_id"] = None
        
        partner_context_ref = run_context['sub_context_refs'].get("_partner_context_ref") # V4.1: Access sub_context_refs
        if partner_context_ref and partner_context_ref.get("state"):
            partner_context_ref["state"]["is_principal_flow_running"] = False
            logger.info("stop_managed_principal_flag_updated", extra={"session_id": session_id_for_log, "managing_partner_run_id": managing_partner_run_id, "is_principal_flow_running": False})
        
        # The 'principal_task_stopped_by_request' status is now inferred on the client from the turn status.
        logger.info("stop_managed_principal_completed", extra={"managing_partner_run_id": managing_partner_run_id})
    
    elif principal_task_handle and principal_task_handle.done():
        logger.info("stop_managed_principal_already_done", extra={"session_id": session_id_for_log, "principal_subtask_id": principal_subtask_id, "managing_partner_run_id": managing_partner_run_id})
        # The 'principal_task_already_done' status is now inferred on the client.
        # V4.1: Access runtime for these handles
        if run_context['runtime'].get("principal_flow_task_handle") is not None or run_context['runtime'].get("current_principal_subtask_id") is not None:
            run_context['runtime']["principal_flow_task_handle"] = None
            run_context['runtime']["current_principal_subtask_id"] = None
            logger.info("stop_managed_principal_cleanup_stale", extra={"session_id": session_id_for_log, "managing_partner_run_id": managing_partner_run_id})
        partner_context_ref = run_context['sub_context_refs'].get("_partner_context_ref") # V4.1: Access sub_context_refs
        if partner_context_ref and partner_context_ref.get("state") and partner_context_ref["state"].get("is_principal_flow_running") is True:
            partner_context_ref["state"]["is_principal_flow_running"] = False
            logger.info("stop_managed_principal_corrected_flag", extra={"session_id": session_id_for_log, "managing_partner_run_id": managing_partner_run_id, "is_principal_flow_running": False})
    else:
        logger.info("stop_managed_principal_no_active_task", extra={"session_id": session_id_for_log, "managing_partner_run_id": managing_partner_run_id, "has_task_handle": principal_task_handle is not None, "principal_subtask_id": principal_subtask_id})
        # The 'no_active_principal_to_stop' status is now inferred on the client.


async def handle_request_run_profiles_message(ws_state: Dict, data: Dict):
    """Handles 'request_run_profiles' messages, returning the Agent Profile information for the specified run."""
    event_manager = ws_state.event_manager # Changed: Using HEAD's way
    session_id_for_log = event_manager.session_id
    
    run_id = data.get("run_id")
    logger.info("request_run_profiles_received", extra={"session_id": session_id_for_log, "run_id": run_id, "data": data})

    if not run_id:
        logger.warning("request_run_profiles_missing_run_id", extra={"session_id": session_id_for_log})
        await event_manager.send_json(
            run_id=None, 
            message={
                "type": "run_profiles_response",
                "run_id": run_id,
                "error": "Command 'request_run_profiles' requires 'run_id' in 'data' field."
            }
        )
        return

    run_context = active_runs_store.get(run_id)

    if not run_context:
        logger.warning("request_run_profiles_context_not_found", extra={"session_id": session_id_for_log, "run_id": run_id})
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "run_profiles_response",
                "run_id": run_id,
                "error": "Run ID not found or invalid."
            }
        )
        return

    try:
        profiles_to_send = run_context['config'].get("agent_profiles_store", {}) # V4.1: Access config
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "run_profiles_response",
                "run_id": run_id,
                "data": {"profiles": profiles_to_send}
            }
        )
        logger.info("run_profiles_response_sent", extra={"session_id": session_id_for_log, "run_id": run_id})
    except Exception as e:
        logger.error("request_run_profiles_error", extra={"session_id": session_id_for_log, "run_id": run_id, "error_message": str(e)}, exc_info=True)
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "run_profiles_response",
                "run_id": run_id,
                "error": f"Failed to retrieve profiles for run {run_id}: {str(e)}"
            }
        )


async def handle_request_run_context_message(ws_state: Dict, data: Dict):
    """Handles 'request_run_context' messages, returning the serialized context information for the specified run."""
    event_manager = ws_state.event_manager # Changed: Using HEAD's way
    session_id_for_log = event_manager.session_id

    run_id = data.get("run_id")
    logger.info("request_run_context_received", extra={"session_id": session_id_for_log, "run_id": run_id, "data": data})

    if not run_id:
        logger.warning("request_run_context_missing_run_id", extra={"session_id": session_id_for_log})
        await event_manager.send_json(
            run_id=None,
            message={
                "type": "run_context_response",
                "run_id": run_id,
                "error": "Command 'request_run_context' requires 'run_id' in 'data' field."
            }
        )
        return

    run_context = active_runs_store.get(run_id)

    if not run_context:
        logger.warning("request_run_context_context_not_found", extra={"session_id": session_id_for_log, "run_id": run_id})
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "run_context_response",
                "run_id": run_id,
                "error": "Run ID not found or invalid."
            }
        )
        return

    try:
        logger.debug("run_context_snapshot_starting", extra={"session_id": session_id_for_log, "run_id": run_id})
        # sanitized_context = sanitize_context_for_serialization(run_context) # Old call
        snapshot_context = get_serializable_run_snapshot(run_context) # New call
        logger.debug("run_context_snapshot_completed", extra={"session_id": session_id_for_log, "run_id": run_id})
        
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "run_context_response",
                "run_id": run_id,
                "data": {"context": snapshot_context} # Use the new snapshot
            }
        )
        logger.info("run_context_response_sent", extra={"session_id": session_id_for_log, "run_id": run_id})
    except Exception as e:
        logger.error("request_run_context_error", extra={"session_id": session_id_for_log, "run_id": run_id, "error_message": str(e)}, exc_info=True)
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "run_context_response",
                "run_id": run_id,
                "error": f"Failed to serialize or retrieve context for run {run_id}: {str(e)}"
            }
        )

async def handle_request_knowledge_base_message(ws_state: Dict, data: Dict):
    """Handles 'request_knowledge_base' messages, returning the knowledge base content for the specified run."""
    event_manager = ws_state.event_manager
    session_id_for_log = event_manager.session_id
    
    run_id = data.get("run_id")
    logger.info("request_knowledge_base_received", extra={"session_id": session_id_for_log, "run_id": run_id, "data": data})

    if not run_id:
        logger.warning("request_knowledge_base_missing_run_id", extra={"session_id": session_id_for_log})
        await event_manager.send_json(
            run_id=None,
            message={
                "type": "knowledge_base_response",
                "run_id": run_id,
                "error": "Command 'request_knowledge_base' requires 'run_id' in 'data' field."
            }
        )
        return

    run_context = active_runs_store.get(run_id)

    if not run_context:
        logger.warning("request_knowledge_base_context_not_found", extra={"session_id": session_id_for_log, "run_id": run_id})
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "knowledge_base_response",
                "run_id": run_id,
                "error": "Run ID not found or invalid."
            }
        )
        return
    
    knowledge_base_instance = run_context['runtime'].get("knowledge_base")
    if not knowledge_base_instance:
        logger.warning("knowledge_base_not_found", extra={"session_id": session_id_for_log, "run_id": run_id})
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "knowledge_base_response",
                "run_id": run_id,
                "error": "KnowledgeBase not found for this run."
            }
        )
        return

    try:
        # Mainly send items_by_id, as others are indexes or internal state
        # Ensure the content is serializable
        
        # Create a serializable version of items_by_id
        serializable_items_by_id = {}
        if hasattr(knowledge_base_instance, 'items_by_id') and isinstance(knowledge_base_instance.items_by_id, dict):
            for item_id, item_obj in knowledge_base_instance.items_by_id.items():
                # Assuming KnowledgeItem objects have a .to_dict() method or directly accessible serializable fields
                if hasattr(item_obj, 'to_dict') and callable(item_obj.to_dict):
                    serializable_items_by_id[item_id] = item_obj.to_dict()
                elif hasattr(item_obj, 'content') and hasattr(item_obj, 'metadata'): # Simplified handling
                    serializable_items_by_id[item_id] = {
                        "item_id": getattr(item_obj, 'item_id', item_id), # Ensure item_id is in the data
                        "item_type": getattr(item_obj, 'item_type', 'Unknown'),
                        "content": getattr(item_obj, 'content', None),
                        "metadata": getattr(item_obj, 'metadata', {}),
                        "source_uri": getattr(item_obj, 'source_uri', None),
                        "created_at": getattr(item_obj, 'created_at', None),
                        "updated_at": getattr(item_obj, 'updated_at', None),
                    }
                else: # Fallback if direct serialization is not straightforward
                    serializable_items_by_id[item_id] = {"error": "Item not directly serializable", "item_type": str(type(item_obj))}

        kb_data_to_send = {
            "run_id": knowledge_base_instance.run_id,
            "items_by_id": serializable_items_by_id,
            # Optionally add other KB metadata if needed
            # "items_by_uri_count": len(knowledge_base_instance.items_by_uri),
            # "items_by_hash_count": len(knowledge_base_instance.items_by_hash),
        }
        
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "knowledge_base_response",
                "run_id": run_id,
                "data": {"knowledge_base": kb_data_to_send}
            }
        )
        logger.info("knowledge_base_response_sent", extra={"session_id": session_id_for_log, "run_id": run_id})
    except Exception as e:
        logger.error("request_knowledge_base_error", extra={"session_id": session_id_for_log, "run_id": run_id, "error_message": str(e)}, exc_info=True)
        await event_manager.send_json(
            run_id=run_id,
            message={
                "type": "knowledge_base_response",
                "run_id": run_id,
                "error": f"Failed to serialize or retrieve KnowledgeBase for run {run_id}: {str(e)}"
            }
        )

async def handle_subscribe_to_view(ws_state: Dict, data: Dict):
    """Handles client requests to subscribe to a view model"""
    event_manager = ws_state.event_manager
    session_id_for_log = event_manager.session_id
    
    run_id = data.get("run_id")
    view_name = data.get("view_name")
    
    logger.info("subscribe_to_view_received", extra={"session_id": session_id_for_log, "run_id": run_id, "view_name": view_name})

    if not run_id or not view_name:
        logger.warning("subscribe_to_view_missing_params", extra={"session_id": session_id_for_log, "has_run_id": bool(run_id), "has_view_name": bool(view_name)})
        await event_manager.emit_error(run_id=run_id, agent_id="System", error_message="subscribe_to_view requires run_id and view_name.")
        return

    run_context = active_runs_store.get(run_id)
    if not run_context:
        logger.warning("subscribe_to_view_context_not_found", extra={"session_id": session_id_for_log, "run_id": run_id})
        await event_manager.emit_error(run_id=run_id, agent_id="System", error_message=f"Run '{run_id}' not found.")
        return

    # Record the subscription relationship (optional, if more complex unsubscribe logic is needed)
    if not hasattr(ws_state, 'subscriptions'):
        ws_state.subscriptions = {}
    
    subscriptions = ws_state.subscriptions
    if run_id not in subscriptions:
        subscriptions[run_id] = set()
    subscriptions[run_id].add(view_name)
    
    # Immediately push the latest view model once
    await trigger_view_model_update(run_context, view_name)


async def handle_unsubscribe_from_view(ws_state: Dict, data: Dict):
    """Handles client requests to unsubscribe from a view model"""
    event_manager = ws_state.event_manager
    session_id_for_log = event_manager.session_id
    
    run_id = data.get("run_id")
    view_name = data.get("view_name")
    
    logger.info("unsubscribe_from_view_received", extra={"session_id": session_id_for_log, "run_id": run_id, "view_name": view_name})

    if run_id and view_name and hasattr(ws_state, 'subscriptions'):
        subscriptions = ws_state.subscriptions
        if run_id in subscriptions and view_name in subscriptions[run_id]:
            subscriptions[run_id].remove(view_name)
            logger.info("unsubscribe_from_view_successful", extra={"session_id": session_id_for_log, "run_id": run_id, "view_name": view_name})
            if not subscriptions[run_id]:
                del subscriptions[run_id]


async def handle_manage_work_modules_request(ws_state: Dict, data: Dict):
    """Handles direct work module management requests from the client"""
    event_manager = ws_state.event_manager
    session_id_for_log = event_manager.session_id

    run_id = data.get("run_id")
    actions = data.get("actions")

    logger.info("manage_work_modules_request_received", extra={"session_id": session_id_for_log, "run_id": run_id})

    if not run_id or not isinstance(actions, list):
        logger.warning("manage_work_modules_missing_params", extra={"session_id": session_id_for_log, "has_run_id": bool(run_id), "actions_is_list": isinstance(actions, list)})
        await event_manager.emit_error(run_id=run_id, agent_id="System", error_message="manage_work_modules_request requires run_id and a list of actions.")
        return

    run_context = active_runs_store.get(run_id)
    if not run_context:
        logger.warning("manage_work_modules_context_not_found", extra={"session_id": session_id_for_log, "run_id": run_id})
        await event_manager.emit_error(run_id=run_id, agent_id="System", error_message=f"Run '{run_id}' not found.")
        return
    
    team_state = run_context['team_state'] # V4.1: team_state is a direct key
    if not team_state: # Should not happen if run_context is valid
        logger.error("manage_work_modules_no_team_state", extra={"session_id": session_id_for_log, "run_id": run_id}, exc_info=True)
        await event_manager.emit_error(run_id=run_id, agent_id="System", error_message=f"Internal error: team_state not found for run '{run_id}'.")
        return

    update_result = await _apply_work_module_actions(team_state, actions)

    if update_result.get("overall_status") != "failure":
        team_state["work_modules"] = update_result.get("final_work_modules", team_state.get("work_modules"))
        logger.info("work_modules_updated", extra={"run_id": run_id, "source": "direct_request"})
        
        # Trigger kanban view update
        await trigger_view_model_update(run_context, "kanban_view")
    else:
        error_message = f"Failed to manage work modules: {update_result.get('status_message')}"
        logger.error("manage_work_modules_failed", extra={"session_id": session_id_for_log, "error_message": error_message}, exc_info=True)
        await event_manager.emit_error(run_id=run_id, agent_id="System", error_message=error_message)


async def handle_send_to_run_message(ws_state: Dict, data: Dict):
    """
    Handles 'send_to_run' messages, routing client messages to the specified active business run.
    Also handles multimodal payloads (files), and is responsible for activating runs in CREATED state.
    """
    event_manager = ws_state.event_manager
    session_id_for_log = event_manager.session_id

    target_run_id = data.get("run_id")
    run_id_var.set(target_run_id)  # Set context variable
    message_payload = data.get("message_payload")
    extra_payload = data.get("extra_payload")
    files_content = (message_payload or {}).get("files", [])

    logger.info(
        "send_to_run_received",
        extra={
            "session_id": session_id_for_log,
            "target_run_id": target_run_id,
            "message_preview": str(message_payload)[:100],
            "has_files": bool(files_content),
        },
    )

    if not target_run_id or message_payload is None:
        err_msg = "'send_to_run' requires 'run_id' and 'message_payload'."
        logger.warning("send_to_run_missing_params", extra={"session_id": session_id_for_log, "data": data, "has_run_id": bool(target_run_id), "has_message_payload": message_payload is not None})
        await event_manager.emit_error(run_id=target_run_id, agent_id="System", error_message=err_msg)
        return

    run_context = active_runs_store.get(target_run_id)
    if not run_context:
        err_msg = f"Target run {target_run_id} not found or not active."
        logger.warning("send_to_run_target_not_found", extra={"session_id": session_id_for_log, "target_run_id": target_run_id})
        await event_manager.emit_error(run_id=target_run_id, agent_id="System", error_message=err_msg)
        return

    run_status = run_context['meta'].get('status')
    run_type = run_context['meta'].get('run_type')
    prompt_content = message_payload.get("prompt")

    try:
        # --- Branch 1: Activate a pending run ---
        if run_status == 'CREATED':
            logger.debug("run_activation_started", extra={"run_id": target_run_id, "run_type": run_type})
            
            # Allow activation if there is either text or multimodal content
            if (prompt_content is None) and (not files_content):
                raise ValueError("First message must contain either 'prompt' text or attachments (files).")
            
            # If no text but attachments exist, use empty string to initialize question
            run_context['team_state']['question'] = prompt_content or ""
            
            task = None
            if run_type == "partner_interaction":
                partner_context = run_context['sub_context_refs']['_partner_context_ref']
                team_state = run_context['team_state']
                partner_state = partner_context['state']

                # 构建payload，仅使用统一的 files 多模态路径
                payload = {"prompt": prompt_content}
                if files_content:
                    payload["files"] = files_content
                
                inbox_item = {
                    "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
                    "source": (
                        "USER_PROMPT_WITH_FILES" if files_content else "USER_PROMPT"
                    ), # Standardized source with multimodal marker when applicable
                    "payload": payload,
                    "consumption_policy": "consume_on_read",
                    "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                }
                partner_state.setdefault("inbox", []).append(inbox_item)
                
                # 2. Start the task
                task = asyncio.create_task(run_partner_interaction_async(partner_context=partner_context))
            else:
                raise ValueError(f"Run type '{run_type}' does not support activation via 'send_to_run'.")

            ws_state.active_run_tasks[target_run_id] = task
            task.add_done_callback(
                lambda t: logger.info("run_task_finished", extra={"run_id": target_run_id, "run_type": run_type, "session_id": session_id_for_log})
                if not t.cancelled() else
                logger.info("run_task_cancelled", extra={"run_id": target_run_id, "run_type": run_type, "session_id": session_id_for_log})
            )
            
            run_context['meta']['status'] = 'AWAITING_INPUT'
            logger.debug("run_activation_completed", extra={"run_id": target_run_id, "status": "AWAITING_INPUT"})

            # 3. Wake up the task
            if run_type == "partner_interaction":
                new_input_event = run_context['sub_context_refs']['_partner_context_ref']['runtime_objects'].get("new_user_input_event")
                if new_input_event:
                    new_input_event.set()
            return  # Critical: Return immediately after handling activation

        # --- Branch 2: Send a message to a running session ---
        elif run_status in ['RUNNING', 'AWAITING_INPUT']:
            # Allow sending if text or attachments exist
            if (prompt_content is None) and (not files_content):
                raise ValueError("Message must contain either 'prompt' text or attachments (files).")

            if run_type == "partner_interaction":
                partner_context = run_context['sub_context_refs']['_partner_context_ref']
                partner_state = partner_context['state']
                team_state = run_context['team_state']

                # --- Core modification: Create an InboxItem with multimodal info (files only) ---
                payload = {"prompt": prompt_content}
                if files_content:
                    payload["files"] = files_content
                
                inbox_item = {
                    "item_id": f"inbox_{uuid.uuid4().hex[:8]}",
                    "source": ("USER_PROMPT_WITH_FILES" if files_content else "USER_PROMPT"),
                    "payload": payload,
                    "consumption_policy": "consume_on_read",
                    "metadata": {"created_at": datetime.now(timezone.utc).isoformat()}
                }
                partner_state.setdefault("inbox", []).append(inbox_item)

                # Wake up the task
                new_input_event = partner_context['runtime_objects'].get("new_user_input_event")
                if new_input_event:
                    new_input_event.set()
                    logger.info("partner_task_notified", extra={"run_id": target_run_id, "notification_method": "inbox"})
                else:
                    logger.error("partner_notification_failed", extra={"run_id": target_run_id, "reason": "new_user_input_event_not_found"}, exc_info=True)
            
        # --- Branch 3: Handle invalid states ---
        else:
            err_msg = f"Cannot send message to run {target_run_id} because its status is '{run_status}'."
            logger.warning("send_to_run_invalid_status", extra={"session_id": session_id_for_log, "run_id": target_run_id, "run_status": run_status})
            await event_manager.emit_error(run_id=target_run_id, agent_id="System", error_message=err_msg)
            return

    except Exception as e:
        logger.error("send_to_run_processing_error", extra={"session_id": session_id_for_log, "target_run_id": target_run_id, "run_type": run_type, "error_message": str(e)}, exc_info=True)
        await event_manager.emit_error(run_id=target_run_id, agent_id="System", error_message=f"Error processing message for run {target_run_id}: {str(e)}")


# --- MESSAGE_HANDLERS registry (Dango's version, with adapted function names) ---
MESSAGE_HANDLERS: Dict[str, callable] = {
    "start_run": handle_start_run_message,
    "stop_run": handle_stop_run_message,
    "request_available_toolsets": handle_request_available_toolsets,
    "send_to_run": handle_send_to_run_message, # Unified handler
    "stop_managed_principal": handle_stop_managed_principal_message, # Added by Dango, adapted
    "request_run_profiles": handle_request_run_profiles_message, # Added by Dango, adapted
    "request_run_context": handle_request_run_context_message, # Added by Dango, adapted
    "request_knowledge_base": handle_request_knowledge_base_message, # New handler
    "subscribe_to_view": handle_subscribe_to_view, # New view subscription handler
    "unsubscribe_from_view": handle_unsubscribe_from_view, # New view unsubscription handler
    "manage_work_modules_request": handle_manage_work_modules_request, # New module management handler
}

# Ensure old handlers (if they were ever in a combined state) are not present
# These were explicitly removed in Dango's MESSAGE_HANDLERS definition
if "partner_user_message" in MESSAGE_HANDLERS:
    del MESSAGE_HANDLERS["partner_user_message"]
if "notify_partner_principal_done" in MESSAGE_HANDLERS:
    del MESSAGE_HANDLERS["notify_partner_principal_done"]
if "mcp_tool_response" in MESSAGE_HANDLERS:
    del MESSAGE_HANDLERS["mcp_tool_response"]
