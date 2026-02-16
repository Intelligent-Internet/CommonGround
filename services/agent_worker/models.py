from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from core.cg_context import CGContext
from core.llm import LLMConfig
from core.utp_protocol import Card

@dataclass
class AgentTurnWorkItem:
    project_id: str
    channel_id: str
    agent_turn_id: str
    agent_id: str
    profile_box_id: Optional[str]
    context_box_id: Optional[str]
    output_box_id: Optional[str]
    turn_epoch: Optional[int]
    headers: Dict[str, str]
    parent_step_id: Optional[str] = None
    trace_id: Optional[str] = None
    step_count: int = 0
    resume_data: Optional[Dict[str, Any]] = None
    stop_data: Optional[Dict[str, Any]] = None
    is_continuation: bool = False
    inbox_id: Optional[str] = None
    guard_inbox_id: Optional[str] = None
    enqueued_at: Optional[datetime] = None
    inbox_created_at: Optional[datetime] = None
    inbox_processed_at: Optional[datetime] = None
    timing: Dict[str, Any] = field(default_factory=dict)

    def to_cg_context(
        self,
        *,
        headers: Optional[Dict[str, str]] = None,
        agent_turn_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        step_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
    ) -> CGContext:
        resolved_headers = self.headers if headers is None else headers
        resolved_agent_turn_id = self.agent_turn_id if agent_turn_id is None else agent_turn_id
        resolved_trace_id = self.trace_id if trace_id is None else trace_id
        resolved_parent_step_id = self.parent_step_id if parent_step_id is None else parent_step_id
        return CGContext(
            project_id=self.project_id,
            channel_id=self.channel_id,
            agent_id=self.agent_id,
            agent_turn_id=resolved_agent_turn_id,
            trace_id=resolved_trace_id,
            headers=dict(resolved_headers or {}),
            step_id=step_id,
            parent_step_id=resolved_parent_step_id,
        )

    def next_step(self) -> "AgentTurnWorkItem":
        return AgentTurnWorkItem(
            project_id=self.project_id,
            channel_id=self.channel_id,
            agent_turn_id=self.agent_turn_id,
            agent_id=self.agent_id,
            profile_box_id=self.profile_box_id,
            context_box_id=self.context_box_id,
            output_box_id=self.output_box_id,
            turn_epoch=self.turn_epoch,
            headers=self.headers,
            parent_step_id=self.parent_step_id,
            trace_id=self.trace_id,
            step_count=self.step_count + 1,
            resume_data=None,
            stop_data=None,
            is_continuation=True,
            inbox_id=None,
            guard_inbox_id=self.guard_inbox_id or self.inbox_id,
            enqueued_at=None,
            inbox_created_at=None,
            inbox_processed_at=None,
            timing={},
        )

@dataclass
class ProfileData:
    card_id: str
    name: str
    system_prompt_template: str
    allowed_downstream: List[str]
    # None => allow all tools registered in project; [] => allow none.
    allowed_tools: Optional[List[str]]
    llm_config: LLMConfig
    # Builtin tools exposed directly by the worker (e.g. submit_result).
    allowed_internal_tools: List[str] = field(default_factory=list)
    must_end_with: List[str] = field(default_factory=list)
    description: str = ""
    downstream_desc: str = ""
    display_name: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    worker_target: Optional[str] = None

class _SafeFormatDict(dict):
    """Keeps unknown placeholders intact when formatting system prompt."""

    def __missing__(self, key):
        return "{" + key + "}"

@dataclass
class ActionOutcome:
    cards: List[Card]
    suspend: bool = False
    mark_complete: bool = False
