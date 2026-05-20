from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AgentDirectoryFilters:
    agent_id: str | None = None
    role: str | None = None
    capability: str | None = None
    enabled_only: bool | None = None
    accepts_work_only: bool | None = None
    limit: int = 100


@dataclass(frozen=True, slots=True)
class TurnOfferFilters:
    turn_kind: str | None = None
    agent_id: str | None = None
    enabled_only: bool | None = None
    accepts_work_only: bool | None = None
    limit: int = 100


@dataclass(frozen=True, slots=True)
class TurnEntryFilters:
    target_agent_id: str | None = None
    turn_kind: str | None = None
    state: str | None = None
    outcome: str | None = None
    stop_requested_only: bool | None = None
    limit: int = 100
