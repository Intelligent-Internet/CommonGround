from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class AgentStateHead(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    agent_id: str
    status: str
    active_agent_turn_id: Optional[str] = None
    active_channel_id: Optional[str] = None
    turn_epoch: int = 0
    active_recursion_depth: int = 0
    updated_at: Optional[datetime] = None
    parent_step_id: Optional[str] = None
    trace_id: Optional[str] = None
    profile_box_id: Optional[str] = None
    context_box_id: Optional[str] = None
    output_box_id: Optional[str] = None
    expecting_correlation_id: Optional[str] = None
    waiting_tool_count: int = 0
    resume_deadline: Optional[datetime] = None


class AgentStepRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    agent_id: str
    step_id: str
    agent_turn_id: str
    channel_id: Optional[str] = None
    parent_step_id: Optional[str] = None
    trace_id: Optional[str] = None
    status: str
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None


class ExecutionEdgeRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    edge_id: str
    project_id: str
    channel_id: Optional[str] = None
    primitive: str
    edge_phase: str
    source_agent_id: Optional[str] = None
    source_agent_turn_id: Optional[str] = None
    source_step_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    target_agent_turn_id: Optional[str] = None
    correlation_id: Optional[str] = None
    enqueue_mode: Optional[str] = None
    recursion_depth: Optional[int] = None
    trace_id: Optional[str] = None
    parent_step_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class IdentityEdgeRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    edge_id: str
    project_id: str
    channel_id: Optional[str] = None
    action: str
    source_agent_id: Optional[str] = None
    target_agent_id: str
    trace_id: Optional[str] = None
    parent_step_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class CardBoxDTO(BaseModel):
    box_id: str
    card_ids: List[str] = Field(default_factory=list)


class CardDTO(BaseModel):
    model_config = ConfigDict(extra="ignore")

    card_id: str
    project_id: str
    type: str
    content: Any
    created_at: datetime
    author_id: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None
    ttl_seconds: Optional[int] = None


class CardBoxHydratedDTO(BaseModel):
    box_id: str
    card_ids: List[str] = Field(default_factory=list)
    hydrated_cards: Optional[List[CardDTO]] = None


class BatchBoxesRequest(BaseModel):
    box_ids: List[str] = Field(default_factory=list)
    hydrate: bool = False
    limit: int = 200


class BatchBoxesResponse(BaseModel):
    boxes: List[CardBoxHydratedDTO] = Field(default_factory=list)
    missing_box_ids: List[str] = Field(default_factory=list)
    missing_card_ids: List[str] = Field(default_factory=list)


class BatchCardsRequest(BaseModel):
    card_ids: List[str] = Field(default_factory=list)


class BatchCardsResponse(BaseModel):
    cards: List[CardDTO] = Field(default_factory=list)
    missing_ids: List[str] = Field(default_factory=list)


class GodStopRequest(BaseModel):
    agent_id: str
    agent_turn_id: str
    turn_epoch: int
    channel_id: str = "public"
    target: str = "worker_generic"
    reason: str = "admin_intervention"


class GodStopResponse(BaseModel):
    status: Literal["ok"] = "ok"


class PMOToolCallRequest(BaseModel):
    caller_agent_id: str
    channel_id: str = "public"
    args: Dict[str, Any] = Field(default_factory=dict)
    after_execution: Literal["suspend", "terminate"] = "suspend"
    timeout_seconds: int = Field(default=180, ge=1, le=600)
    trace_id: Optional[str] = None


class PMOToolCallResponse(BaseModel):
    status: Literal["ok"] = "ok"
    tool_call_id: str
    step_id: str
    tool_result_card_id: Optional[str] = None
    tool_result: Dict[str, Any] = Field(default_factory=dict)


class AgentRoster(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    agent_id: str
    profile_box_id: str
    worker_target: str
    tags: List[str] = Field(default_factory=list)
    display_name: Optional[str] = None
    owner_agent_id: Optional[str] = None
    status: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AgentRosterUpsert(BaseModel):
    agent_id: str
    profile_box_id: Optional[str] = None
    profile_name: Optional[str] = None
    worker_target: Optional[str] = None
    tags: Optional[List[str]] = None
    display_name: Optional[str] = None
    owner_agent_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    init_state: bool = True
    channel_id: Optional[str] = None
