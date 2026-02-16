from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


@dataclass(frozen=True, slots=True)
class BatchCreateRequest:
    project_id: str
    channel_id: str
    source_agent_id: str
    parent_agent_turn_id: str
    parent_turn_epoch: int
    parent_tool_call_id: str
    parent_step_id: str
    parent_after_execution: str
    tool_suffix: str
    task_specs: List[Dict[str, Any]]
    fail_fast: bool
    deadline_seconds: Optional[float]
    lineage_parent_step_id: Optional[str] = None
    structured_output: Optional[str] = None
    trace_id: Optional[str] = None
    parent_traceparent: Optional[str] = None
    recursion_depth: Optional[int] = None


class Orchestrator(Protocol):
    """L1 orchestrator interface (abstract semantics, decoupled from concrete implementations or storage)."""

    async def create_batch(
        self,
        *,
        req: BatchCreateRequest,
        conn: Any,
    ) -> str: ...

    async def dispatch_pending_tasks(self, *, limit: int = 200) -> None: ...

    async def reap_timed_out_batches(self) -> None: ...

    async def handle_child_task_event(
        self,
        *,
        project_id: str,
        agent_turn_id: str,
        status: str,
        output_box_id: Optional[str],
        deliverable_card_id: Optional[str],
        tool_result_card_id: Optional[str],
        error: Optional[str],
    ) -> None: ...
