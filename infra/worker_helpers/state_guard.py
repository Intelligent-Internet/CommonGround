from __future__ import annotations

from dataclasses import dataclass

from core.state import AgentStateHead


@dataclass(frozen=True)
class TurnGuard:
    """L0 guard for a specific agent turn (active_agent_turn_id + turn_epoch)."""

    agent_turn_id: str
    turn_epoch: int

    def matches(self, state: AgentStateHead) -> bool:
        return state.active_agent_turn_id == self.agent_turn_id and state.turn_epoch == self.turn_epoch

    def matches_same_turn_allow_newer_epoch(self, state: AgentStateHead) -> bool:
        return state.active_agent_turn_id == self.agent_turn_id and state.turn_epoch >= self.turn_epoch

    def mismatch_detail(self, state: AgentStateHead) -> str:
        return (
            f"cmd(turn={self.agent_turn_id},epoch={self.turn_epoch}) "
            f"state(turn={state.active_agent_turn_id},epoch={state.turn_epoch})"
        )
