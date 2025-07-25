import logging
import asyncio
import uuid
from typing import Optional, Any, Dict, List
from fastapi import WebSocket
from starlette.websockets import WebSocketState # Add import
import json
import copy
from datetime import datetime, timezone

from .session import active_runs_store, active_event_managers # Import global stores
logger = logging.getLogger(__name__)

class SessionEventManager:
    """Session Event Manager

    Handles all real-time communication for a single session, including:
    1. LLM streaming output
    2. State synchronization
    3. Error handling
    4. MCP tool calls
    """
    
    def __init__(self, session_id: str):
        """Initializes the event manager

        Args:
            session_id: The session ID
        """
        self.session_id = session_id
        self.is_connected = False
        self.websocket: Optional[WebSocket] = None
        self.on_send: Optional[callable] = None
        # session_id is now the connection credential ID for the WebSocket, mainly used for logging
        logger.debug("event_manager_created", extra={"session_id": session_id})
        
    def attach(self, on_send):
        self.on_send = on_send

    def connect(self, websocket: WebSocket):
        """Marks the WebSocket connection as established

        Args:
            websocket: The WebSocket connection object
        """
        self.websocket = websocket
        self.is_connected = True
        logger.info("websocket_connection_established", extra={"session_id": self.session_id})
        
    async def disconnect(self):
        """Marks the WebSocket connection as disconnected and tries to cancel associated long-running tasks."""
        original_websocket = self.websocket
        self.websocket = None
        self.is_connected = False
        logger.info("websocket_connection_disconnected", extra={"session_id": self.session_id})

        # Removed the old task cancellation logic based on top_level_shared.
        # Task cancellation is now handled by the finally block of the websocket_endpoint in api/server.py,
        # based on websocket.state.active_run_tasks.
        
        # Ensure the original websocket connection object is properly closed (if not handled automatically by FastAPI)
        # Usually FastAPI handles the closing, but call it explicitly to be sure
        if original_websocket:
            try:
                # Check the state to avoid calling close on an already closed connection
                if original_websocket.client_state == WebSocketState.CONNECTED: # WebSocketState needs to be imported from starlette.websockets
                    await original_websocket.close()
                    logger.info("websocket_explicitly_closed", extra={"session_id": self.session_id})
            except Exception as e:
                logger.warning("websocket_close_error", extra={"session_id": self.session_id, "error": str(e)}, exc_info=True)


    async def _send(self, message: Dict):
        """Internal send method, responsible for checking the connection, JSON serialization, and the actual send operation

        Args:
            message: The message dictionary to send
        """
        if not self.is_connected or not self.websocket:
            logger.debug("websocket_not_connected_message_dropped", extra={"session_id": self.session_id, "message_type": message.get('type', 'unknown')})
            return
            
        try:
            # Add session ID
            if "session_id" not in message:
                message["session_id"] = self.session_id
                
            # Manually serialize JSON and send as text
            # Use default=str to handle objects that cannot be directly serialized, converting them to string form
            message_json = json.dumps(message, ensure_ascii=False, default=str)
            if self.is_connected and self.websocket:
                await self.websocket.send_text(message_json)
            if self.on_send:
                await self.on_send(message_json)
            logger.debug("message_sent", extra={"session_id": self.session_id, "message_type": message.get('type', 'unknown')})
        except RuntimeError as e: # Specifically catch this ASGI error
            # Starlette 0.35+ raises WebSocketException for send on closed, which is a subclass of RuntimeError
            # but checking specific message for older versions or other RuntimeErrors.
            if "Unexpected ASGI message 'websocket.send'" in str(e) or \
               "WebSocket is not connected" in str(e) or \
               "Cannot call send" in str(e): # For starlette.websockets.WebSocketException
                logger.warning("websocket_send_failed_connection_closing", extra={"session_id": self.session_id, "message_type": message.get('type', 'unknown'), "error": str(e)})
                self.is_connected = False 
                self.websocket = None     
            else: # Other RuntimeErrors
                logger.error("message_send_failed_runtime_error", extra={"session_id": self.session_id, "message_type": message.get('type', 'unknown'), "error": str(e)}, exc_info=True)
        except Exception as e: # All other exceptions
            logger.error("message_send_failed_general", extra={"session_id": self.session_id, "message_type": message.get('type', 'unknown'), "error": str(e)}, exc_info=True)
            
    async def emit_llm_chunk(self, run_id: str, agent_id: str, parent_agent_id: Optional[str], chunk_type: str, content: str, stream_id: Optional[str] = None, is_first_chunk: bool = False, is_completion_marker: bool = False, llm_id: Optional[str] = None, contextual_data: Optional[Dict] = None):
        """Sends an LLM streaming output chunk
        
        Args:
            run_id: The run ID
            agent_id: The agent ID
            parent_agent_id: The parent agent ID
            chunk_type: The chunk type (content/tool_name/tool_args/FIM/result)
            content: The content
            stream_id: The stream ID, used to identify a complete streaming output sequence
            is_first_chunk: (Deprecated, kept for compatibility)
            is_completion_marker: Whether this is the completion marker (the last chunk of the stream)
            llm_id: The target LLM model ID
            contextual_data: Additional context data, which will be merged into the data field of the event
        """
        message_data = {
            "content": content
        }
        if contextual_data:
            message_data.update(contextual_data)

        message = {
            "type": "llm_chunk",
            "run_id": run_id,
            "agent_id": agent_id,
            "parent_agent_id": parent_agent_id,
            "chunk_type": chunk_type,
            "stream_id": stream_id or str(uuid.uuid4()),
            "is_completion_marker": is_completion_marker,
            "llm_id": llm_id,  # Add LLM ID
            "data": message_data
        }
        await self._send(message)
            
    async def emit_llm_response(self, run_id: str, agent_id: str, parent_agent_id: Optional[str], content: Optional[str], tool_calls: Optional[List[Dict]], reasoning: Optional[str] = None, stream_id: Optional[str] = None, llm_id: Optional[str] = None, contextual_data: Optional[Dict] = None):
        """Sends a complete LLM response

        Args:
            run_id: The run ID
            agent_id: The agent ID
            parent_agent_id: The parent agent ID
            content: The text content
            tool_calls: The list of tool calls
            reasoning: The reasoning process (optional)
            stream_id: The stream ID, used to identify a complete streaming output sequence
            llm_id: The target LLM model ID (optional)
            contextual_data: Additional context data, which will be merged into the data field of the event
        """
        # Create a log summary
        content_summary = content[:50] + "..." if content and len(content) > 50 else content
        tool_calls_summary = f"{len(tool_calls)} tool calls" if tool_calls else "No tool calls"
        has_reasoning = "Yes" if reasoning else "No"
        
        logger.debug("llm_response_generated", extra={"run_id": run_id, "agent_id": agent_id, "content_summary": content_summary, "tool_calls_summary": tool_calls_summary, "has_reasoning": has_reasoning})
        
        message_data = {
            "content": content,
            "tool_calls": tool_calls,
            "reasoning": reasoning
        }
        if contextual_data:
            message_data.update(contextual_data)

        message = {
            "type": "llm_response",
            "run_id": run_id,
            "agent_id": agent_id,
            "parent_agent_id": parent_agent_id,
            "stream_id": stream_id or str(uuid.uuid4()),
            "llm_id": llm_id, # Add LLM ID
            "is_completion_marker": True,  # llm_response is always a completion marker
            "data": message_data
        }
        await self._send(message)
        
    # DEPRECATED: emit_agent_status has been removed
    # Agent status is now tracked through the Turn model in team_state
    # Use turn status ('running', 'completed', 'error') instead
        
    async def emit_resource(self, run_id: str, agent_id: str, resource_type: str, resource_data: Any, contextual_data: Optional[Dict] = None):
        """Sends resource data

        Args:
            run_id: The run ID
            agent_id: The agent ID
            resource_type: The resource type
            resource_data: The resource data
            contextual_data: Additional context data, which will be merged into the data field of the event (resource_data itself is part of data)
        """
        # Create a resource summary
        if isinstance(resource_data, dict):
            resource_summary = f"Fields: {len(resource_data)}"
        elif isinstance(resource_data, list):
            resource_summary = f"Items: {len(resource_data)}"
        elif isinstance(resource_data, str):
            resource_summary = f"Length: {len(resource_data)}"
        else:
            resource_summary = f"Type: {type(resource_data).__name__}"
            
        logger.debug("resource_emitted", extra={"run_id": run_id, "agent_id": agent_id, "resource_type": resource_type, "resource_summary": resource_summary})
        
        # resource_data is the primary content for the 'data' field of a resource event.
        # If contextual_data is provided, it should be merged into this.
        # However, the typical use of 'data' in 'resource' event is the resource_data itself.
        # Let's clarify: if resource_data is a dict, merge. Otherwise, wrap.
        
        final_data_payload = {}
        if isinstance(resource_data, dict):
            final_data_payload.update(resource_data)
        else: # If resource_data is not a dict (e.g. list, string), wrap it.
            final_data_payload["resource_content"] = resource_data

        if contextual_data:
            final_data_payload.update(contextual_data)

        message = {
            "type": "resource",
            "run_id": run_id,
            "agent_id": agent_id,
            "resource_type": resource_type,
            "data": final_data_payload
        }
        await self._send(message)
        
    # DEPRECATED: emit_state/state_sync has been removed
    # State synchronization is now handled through turns_sync and view model updates
    # Use emit_turns_sync() instead for state updates
            
    async def emit_error(self, run_id: Optional[str], agent_id: Optional[str], error_message: str, contextual_data: Optional[Dict] = None):
        """Sends an error message

        Args:
            run_id: The run ID (optional)
            agent_id: The agent ID (optional)
            error_message: The error message
            contextual_data: Additional context data, which will be merged into the data field of the event
        """
        logger.error("error_event_emitted", extra={"run_id": run_id or 'N/A', "agent_id": agent_id or 'System', "error_message": error_message})
        
        message_data = {
            "message": error_message
        }
        if contextual_data:
            message_data.update(contextual_data)

        message = {
            "type": "error",
            "run_id": run_id,
            "agent_id": agent_id or "System",
            "data": message_data
        }
        await self._send(message)

    async def emit_llm_stream_started(self, run_id: str, agent_id: str, parent_agent_id: Optional[str], stream_id: str, llm_id: Optional[str], contextual_data: Optional[Dict] = None):
        """Sends a signal that the LLM stream has started"""
        logger.debug("llm_stream_started", extra={"run_id": run_id, "agent_id": agent_id, "stream_id": stream_id})
        message = {
            "type": "llm_stream_started",
            "run_id": run_id,
            "agent_id": agent_id,
            "parent_agent_id": parent_agent_id,
            "stream_id": stream_id,
            "llm_id": llm_id,
            "data": contextual_data or {}
        }
        await self._send(message)

    async def emit_llm_stream_ended(self, run_id: str, agent_id: str, parent_agent_id: Optional[str], stream_id: str, contextual_data: Optional[Dict] = None):
        """Sends a signal that the LLM stream has ended successfully"""
        logger.debug("llm_stream_ended_successfully", extra={"run_id": run_id, "agent_id": agent_id, "stream_id": stream_id})
        message = {
            "type": "llm_stream_ended",
            "run_id": run_id,
            "agent_id": agent_id,
            "parent_agent_id": parent_agent_id,
            "stream_id": stream_id,
            "data": contextual_data or {}
        }
        await self._send(message)

    async def emit_llm_stream_failed(self, run_id: str, agent_id: str, parent_agent_id: Optional[str], stream_id: str, reason: str, contextual_data: Optional[Dict] = None):
        """Sends a signal that the LLM stream has failed/been deprecated due to an error"""
        logger.warning("llm_stream_failed", extra={"run_id": run_id, "agent_id": agent_id, "stream_id": stream_id, "reason": reason})
        message_data = {
            "reason": reason
        }
        if contextual_data:
            message_data.update(contextual_data)
        
        message = {
            "type": "llm_stream_failed",
            "run_id": run_id,
            "agent_id": agent_id,
            "parent_agent_id": parent_agent_id,
            "stream_id": stream_id,
            "data": message_data
        }
        await self._send(message)

    async def emit_llm_request_params(self, run_id: str, agent_id: str, stream_id: str, llm_id: Optional[str], params: Dict, contextual_data: Optional[Dict] = None):
        """Sends LLM request parameters

        Args:
            run_id: The run ID
            agent_id: The agent ID
            stream_id: The stream ID
            llm_id: The target LLM model ID
            params: The dictionary of parameters sent to the LLM
            contextual_data: Additional context data, which will be merged into the data field of the event
        """
        logger.debug("llm_request_params_emitted", extra={"run_id": run_id, "agent_id": agent_id, "stream_id": stream_id})
        message_data = {
            "params": self._filter_credentials(params)
        }
        if contextual_data:
            message_data.update(contextual_data)

        message = {
            "type": "llm_request_params",
            "run_id": run_id,
            "agent_id": agent_id,
            "stream_id": stream_id,
            "llm_id": llm_id,
            "data": message_data
        }
        await self._send(message)

    def _filter_credentials(self, params: Dict) -> Dict:
        """
        Filters out sensitive credential information from a parameters dictionary.
        """
        if not isinstance(params, dict):
            return params

        filtered_params = params.copy() # Work on a copy
        sensitive_keywords = ["key", "token", "secret", "password", "credential"]

        for key in list(filtered_params.keys()): # Iterate over a list of keys for safe modification
            key_lower = key.lower()
            for keyword in sensitive_keywords:
                if keyword in key_lower:
                    filtered_params[key] = "[REDACTED]"
                    break # Move to the next key once a keyword is found
            
            # Recursively filter if the value is a dictionary
            if isinstance(filtered_params[key], dict):
                 filtered_params[key] = self._filter_credentials(filtered_params[key])
            # elif isinstance(filtered_params[key], list): # Optional: handle lists of dicts
            #    filtered_params[key] = [self._filter_credentials(item) if isinstance(item, dict) else item for item in filtered_params[key]]


        return filtered_params

    async def emit_run_ready(self, run_id: str, request_id: str):
        """(New method) Sends a run_ready event to confirm that the run has been created"""
        logger.debug("run_ready_emitted", extra={"run_id": run_id, "request_id": request_id})
        await self._send({
            "type": "run_ready",
            "data": {
                "request_id": request_id,
                "run_id": run_id,
                "status": "success"
            }
        })

    # DEPRECATED: emit_tool_result has been removed
    # Tool results are now handled through TOOL_RESULT inbox items in AgentNode
    # Tool interactions are tracked in the Turn model's tool_interactions array

    async def emit_run_config_updated(self, run_id: str, config_type: str, item_identifier: Optional[str], details: Dict, contextual_data: Optional[Dict] = None):
        """Sends a runtime configuration update notification

        Args:
            run_id: The ID of the run where the configuration was updated
            config_type: The specific type of configuration that was updated (e.g., "agent_profile")
            item_identifier: A specific identifier for the item being updated (e.g., an agent_id)
            details: A dictionary containing the details of the update
            contextual_data: Additional context data
        """
        logger.info("run_config_updated", extra={"run_id": run_id, "config_type": config_type, "item_identifier": item_identifier or 'N/A'})
        
        message_data = {
            "config_type": config_type,
            "item_identifier": item_identifier,
            "details": details
        }
        if contextual_data:
            message_data.update(contextual_data)

        message = {
            "type": "run_config_updated",
            "run_id": run_id,
            "data": message_data
        }
        await self._send(message)

    async def _hydrate_turn_interactions(self, turns: List[Dict], kb: Any) -> List[Dict]:
        """ 
        Iterates through turns and hydrates the result_payload in tool_interactions.
        """
        if not kb:
            return turns
        
        return await kb.hydrate_turn_list_tool_results(turns)

    async def emit_turns_sync(self, context: Dict[str, Any]):
        """ 
        (Modified) Sends the complete list of turns to synchronize the frontend state, and hydrates before sending.
        """
        if not context:
            logger.warning("emit_turns_sync called with empty context. Aborting.")
            return

        run_id = None
        team_state = None

        # --- START OF FIX ---
        # Intelligently determine the context type and get team_state and run_id
        if "team_state" in context and "meta" in context:
            # This is a RunContext
            team_state = context.get("team_state", {})
            run_id = context.get("meta", {}).get("run_id")
        elif "refs" in context and "run" in context.get("refs", {}):
            # This is a SubContext
            team_state = context.get("refs", {}).get("team", {})
            run_id = context.get("meta", {}).get("run_id")
        # --- END OF FIX ---

        if not team_state or run_id is None:
            logger.error("invalid_context_for_turns_sync", extra={"context_keys": list(context.keys())})
            return

        turns = team_state.get("turns", [])

        # --- NEW HYDRATION STEP ---
        kb = context.get("refs", {}).get("run", {}).get("runtime", {}).get("knowledge_base")
        if kb:
            logger.debug("hydrating_turns_before_sending", extra={"run_id": run_id})
            turns_to_send = await self._hydrate_turn_interactions(turns, kb)
        else:
            logger.warning("knowledge_base_not_found_for_hydration", extra={"run_id": run_id})
            turns_to_send = turns
        # --- End of hydration step ---

        try:
            # Use the hydrated turns_to_send
            await self.send_json(
                run_id=run_id,
                message={"type": "turns_sync", "data": {"turns": turns_to_send}}
            )
            logger.debug("turns_sync_event_sent", extra={"run_id": run_id, "turns_count": len(turns_to_send)})
        except Exception as e:
            logger.error("turns_sync_event_send_failed", extra={"run_id": run_id, "error": str(e)}, exc_info=True)

    async def emit_work_module_updated(self, run_id: str, module_data: Dict, contextual_data: Optional[Dict] = None):
        """Sends a work module update event

        Args:
            run_id: The run ID
            module_data: The complete work module object
            contextual_data: Additional context data
        """
        logger.info("work_module_updated", extra={"run_id": run_id, "module_id": module_data.get('module_id'), "status": module_data.get('status')})
        
        message_data = {
            "module": module_data
        }
        if contextual_data:
            message_data.update(contextual_data)

        message = {
            "type": "work_module_updated",
            "run_id": run_id,
            "data": message_data
        }
        await self._send(message)

    async def send_json(self, run_id: Optional[str], message: Dict, contextual_data: Optional[Dict] = None):
        """Sends a JSON message

        Args:
            run_id: The run ID (optional)
            message: The JSON message
            contextual_data: Additional context data, which will be merged into the data field of the event (if the message itself has a data field)
        """
        message_type = message.get("type", "custom")
        # Add run_id to the message if it doesn't exist
        if "run_id" not in message and run_id is not None:
            message["run_id"] = run_id
        
        if contextual_data and "data" in message and isinstance(message["data"], dict):
            message["data"].update(contextual_data)
        elif contextual_data and "data" not in message:  # If no data field, but contextual_data exists, add it as data
            message["data"] = contextual_data

        logger.debug("custom_json_message_sent", extra={"run_id": run_id or 'N/A', "message_type": message_type})
        await self._send(message)

    async def emit_turn_completed(self, run_id: str, turn_id: str, agent_id: str):
        """(New method) Sends a signal that a turn has been successfully completed, used to trigger background operations like persistence."""
        logger.debug("turn_completed", extra={"run_id": run_id, "agent_id": agent_id, "turn_id": turn_id})
        message = {
            "type": "turn_completed",
            "run_id": run_id,
            "agent_id": agent_id,
            "data": {
                "turn_id": turn_id
            }
        }
        await self._send(message)

async def broadcast_project_structure_update(reason: str, details: Dict[str, Any]):
    """
    Broadcasts a project structure updated event to all active WebSocket sessions.
    This is a system-level event, not bound to a specific run_id.

    Args:
        reason (str): The reason for the update (e.g., "rename_run", "delete_project").
        details (Dict[str, Any]): A dictionary containing the details of the operation.
    """
    logger.info("project_structure_update_broadcast", extra={"reason": reason, "active_sessions": len(active_event_managers)})
    message = {
        "type": "project_structure_updated",
        "data": {
            "reason": reason,
            "details": details,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    }

    # Broadcast to all connected clients by iterating over the global list of event managers
    broadcast_tasks = [
        manager.send_json(run_id=None, message=message)
        for manager in active_event_managers if manager.is_connected
    ]
    if broadcast_tasks:
        await asyncio.gather(*broadcast_tasks)
