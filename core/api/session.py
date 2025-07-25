import uuid
import logging
from typing import Dict, TYPE_CHECKING, List
from datetime import datetime # Ensure datetime is imported

if TYPE_CHECKING:
    from api.events import SessionEventManager # For type hinting only

logger = logging.getLogger(__name__)

# New: Used to store pending WebSocket session credentials
# Key: session_id, Value: creation timestamp
pending_websocket_sessions: Dict[str, datetime] = {}

# New: Global run store for storing active business run contexts
# Key: server_run_id, Value: run_context (Dict)
active_runs_store: Dict[str, Dict] = {}

# New: Global list to store all active event managers
active_event_managers: List['SessionEventManager'] = []


async def create_session() -> dict:
    """Creates a temporary WebSocket connection credential (session_id).

    This function no longer initializes any business state or top_level_shared objects.
    Its sole responsibility is to generate a session_id and record it for subsequent WebSocket connection request validation.

    Returns:
        dict: A dictionary containing the session_id and status.
    """
    try:
        session_id = str(uuid.uuid4())
        # Record this session_id and its creation timestamp
        pending_websocket_sessions[session_id] = datetime.now()
        logger.info("websocket_credential_created", extra={"session_id": session_id})
        
        return {
            "session_id": session_id,
            "status": "success"
        }
        
    except Exception as e:
        logger.error("websocket_credential_creation_failed", extra={"error": str(e)}, exc_info=True)
        # Consider how to throw up or return an error response in FastAPI
        # For this refactoring, it is assumed that direct function calls will be handled by the caller's exception or FastAPI's error handling mechanism
        raise # Or return a dictionary containing error information, depending on the API design

# Old functions related to sessions and session_metadata (get_session, remove_session, cleanup_sessions)
# will be removed or heavily refactored, as business state is now managed by run_id and active_runs_store.
# The lifecycle of SessionEventManager is now bound to the WebSocket connection, not the old session object.

# def get_session(session_id: str) -> Optional[dict]:
#     """(Old logic) Get the top_level_shared object of the session"""
#     # ... old code ...
#     pass # Will be refactored or removed in subsequent steps based on the new run_id mechanism

# def remove_session(session_id: str, immediate: bool = True):
#     """(Old logic) Remove the session"""
#     # ... old code ...
#     pass # Will be refactored or removed in subsequent steps based on the new run_id mechanism

# def cleanup_sessions():
#     """(Old logic) Periodically clean up expired sessions marked for deletion"""
#     # ... old code ...
#     pass

# cleanup_thread = threading.Thread(target=cleanup_sessions, daemon=True)
# cleanup_thread.start() 
