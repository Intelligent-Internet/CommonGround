from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(frozen=True, slots=True)
class CGContext:
    """Cross-layer identity/trace context for a single turn-scoped operation."""

    project_id: str
    channel_id: Optional[str] = None
    agent_id: Optional[str] = None
    agent_turn_id: Optional[str] = None
    trace_id: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    step_id: Optional[str] = None
    parent_step_id: Optional[str] = None

