from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Literal

from core.cg_context import CGContext
from core.llm import LLMConfig
from core.utp_protocol import Card


@dataclass
class AgentTurnWorkItem:
    ctx: CGContext
    profile_box_id: Optional[str]
    context_box_id: Optional[str]
    output_box_id: Optional[str]
    step_count: int = 0
    resume_data: Optional[Dict[str, Any]] = None
    stop_data: Optional[Dict[str, Any]] = None
    is_continuation: bool = False
    inbox_id: Optional[str] = None
    guard_inbox_id: Optional[str] = None
    inbox_retry_count: int = 0
    enqueued_at: Optional[datetime] = None
    inbox_created_at: Optional[datetime] = None
    inbox_processed_at: Optional[datetime] = None
    timing: Dict[str, Any] = field(default_factory=dict)
    inbox_finalize_action: Literal["consume", "defer", "error"] = "consume"
    retry_delay_seconds: float = 0.0
    retry_reason: Optional[str] = None

    def next_step(self) -> "AgentTurnWorkItem":
        return AgentTurnWorkItem(
            ctx=self.ctx.evolve(step_id=None, tool_call_id=None),
            profile_box_id=self.profile_box_id,
            context_box_id=self.context_box_id,
            output_box_id=self.output_box_id,
            step_count=self.step_count + 1,
            resume_data=None,
            stop_data=None,
            is_continuation=True,
            inbox_id=None,
            guard_inbox_id=self.guard_inbox_id or self.inbox_id,
            inbox_retry_count=0,
            enqueued_at=None,
            inbox_created_at=None,
            inbox_processed_at=None,
            timing={},
            inbox_finalize_action="consume",
            retry_delay_seconds=0.0,
            retry_reason=None,
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
