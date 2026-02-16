from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field, ConfigDict
from .utp_protocol import CardBox

class LLMStreamPayload(BaseModel):
    """Subject: str.agent.{id}.chunk"""

    agent_turn_id: str
    step_id: str
    chunk_type: Literal["content", "reasoning", "tool_call", "start", "end"]
    content: str = ""
    index: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    usage: Optional[Dict[str, Any]] = None
    response_cost: Optional[float] = None


class StepEventPayload(BaseModel):
    """Subject: evt.agent.{id}.step"""

    model_config = ConfigDict(extra="ignore")

    agent_turn_id: str
    step_id: str
    phase: Literal["started", "planning", "executing", "completed", "failed"]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    new_card_ids: List[str] = Field(default_factory=list)


class AgentResultPayload(BaseModel):
    """Subject: evt.agent.{id}.task"""

    agent_turn_id: str
    status: Literal["success", "failed", "canceled", "timeout", "partial"]
    output_box_id: Optional[str] = None
    deliverable_card_id: Optional[str] = None
    tool_result_card_id: Optional[str] = None
    output_box: Optional[CardBox] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    stats: Dict[str, Any] = Field(default_factory=dict)


class AgentStatePayload(BaseModel):
    """Subject: evt.agent.{id}.state"""

    model_config = ConfigDict(extra="ignore")

    agent_id: str
    agent_turn_id: str
    status: Literal["idle", "dispatched", "running", "suspended"]
    turn_epoch: int
    updated_at: str  # ISO timestamp
    output_box_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
