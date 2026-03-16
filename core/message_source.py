from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from core.cg_context import CGContext
from core.errors import ProtocolViolationError


@dataclass(frozen=True, slots=True)
class MessageSource:
    agent_id: str
    agent_turn_id: Optional[str] = None
    step_id: Optional[str] = None

    def __post_init__(self) -> None:
        agent_id = str(self.agent_id or "").strip()
        if not agent_id:
            raise ProtocolViolationError("MessageSource: missing required agent_id")
        object.__setattr__(self, "agent_id", agent_id)

        agent_turn_id = str(self.agent_turn_id or "").strip()
        object.__setattr__(self, "agent_turn_id", agent_turn_id or None)

        step_id = str(self.step_id or "").strip()
        object.__setattr__(self, "step_id", step_id or None)

    @classmethod
    def from_ctx(cls, ctx: CGContext) -> "MessageSource":
        return cls(
            agent_id=ctx.agent_id,
            agent_turn_id=ctx.agent_turn_id or None,
            step_id=ctx.step_id or None,
        )

