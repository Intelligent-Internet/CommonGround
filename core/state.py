from typing import List, Optional, Literal, Any
from datetime import datetime
from pydantic import BaseModel, ConfigDict


class AgentStateHead(BaseModel):
    """DB row mirror for state.agent_state_head (L0 AgentTurn).

    Used by PMO to stitch contexts and by Worker to perform CAS checks.
    """

    model_config = ConfigDict(extra="ignore")

    project_id: str
    agent_id: str
    status: Literal["idle", "dispatched", "running", "suspended"]
    active_agent_turn_id: Optional[str] = None
    active_channel_id: Optional[str] = None
    turn_epoch: int = 0
    active_recursion_depth: int = 0
    updated_at: Optional[datetime] = None
    parent_step_id: Optional[str] = None
    trace_id: Optional[str] = None
    context_box_id: Optional[str] = None
    profile_box_id: Optional[str] = None
    output_box_id: Optional[str] = None
    expecting_correlation_id: Optional[str] = None
    waiting_tool_count: int = 0
    resume_deadline: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: Any) -> "AgentStateHead":
        """Create from DB row; force box_id fields to string. Supports dict or record with attributes."""

        def _get(key: str, default=None):
            if isinstance(row, dict):
                return row.get(key, default)
            return getattr(row, key, default)

        return cls(
            project_id=_get("project_id"),
            agent_id=_get("agent_id"),
            status=_get("status"),
            active_agent_turn_id=_get("active_agent_turn_id"),
            active_channel_id=_get("active_channel_id"),
            turn_epoch=_get("turn_epoch", 0),
            active_recursion_depth=_get("active_recursion_depth", 0) or 0,
            updated_at=_get("updated_at"),
            parent_step_id=_get("parent_step_id"),
            trace_id=_get("trace_id"),
            context_box_id=str(_get("context_box_id")) if _get("context_box_id") is not None else None,
            profile_box_id=str(_get("profile_box_id")) if _get("profile_box_id") is not None else None,
            output_box_id=str(_get("output_box_id")) if _get("output_box_id") is not None else None,
            expecting_correlation_id=_get("expecting_correlation_id"),
            waiting_tool_count=_get("waiting_tool_count", 0) or 0,
            resume_deadline=_get("resume_deadline"),
        )


class AgentStatePointers(BaseModel):
    """DB row mirror for state.agent_state_pointers (cross-turn pointers).

    These fields are NOT part of L0 CAS gating. They exist to provide
    stable agent-level defaults such as "memory context" and "last output".
    """

    model_config = ConfigDict(extra="ignore")

    project_id: str
    agent_id: str
    memory_context_box_id: Optional[str] = None
    last_output_box_id: Optional[str] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_row(cls, row: Any) -> "AgentStatePointers":
        def _get(key: str, default=None):
            if isinstance(row, dict):
                return row.get(key, default)
            return getattr(row, key, default)

        return cls(
            project_id=_get("project_id"),
            agent_id=_get("agent_id"),
            memory_context_box_id=(
                str(_get("memory_context_box_id"))
                if _get("memory_context_box_id") is not None
                else None
            ),
            last_output_box_id=(
                str(_get("last_output_box_id"))
                if _get("last_output_box_id") is not None
                else None
            ),
            updated_at=_get("updated_at"),
        )
