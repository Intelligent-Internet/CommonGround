from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from CommonGround.contracts import TurnOutcome, TurnState


@dataclass(frozen=True, slots=True)
class ProjectionDiagnostic:
    code: str
    message: str
    subject_id: str | None = None


@dataclass(frozen=True, slots=True)
class ProjectedAgentEntry:
    agent_id: str
    role: str | None
    description: str | None
    enabled: bool
    accepts_work: bool
    capabilities: tuple[str, ...]
    public_metadata: dict[str, object]
    last_seen_at: datetime | None
    grants: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class ProjectedAgentDirectoryPage:
    project_id: str
    items: tuple[ProjectedAgentEntry, ...]
    limit: int
    diagnostics: tuple[ProjectionDiagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectedTurnOfferEntry:
    agent_id: str
    agent_label: str | None
    agent_description: str | None
    turn_kind: str
    purpose: str | None
    calling: dict[str, object]
    input_contract: dict[str, object]
    variants: dict[str, object]
    enabled: bool
    accepts_work: bool
    metadata_source: str


@dataclass(frozen=True, slots=True)
class ProjectedTurnOfferEntryPage:
    project_id: str
    items: tuple[ProjectedTurnOfferEntry, ...]
    limit: int
    diagnostics: tuple[ProjectionDiagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectedTurnEntry:
    turn_id: str
    project_turn_seq: int
    target_agent_id: str
    turn_kind: str
    state: TurnState
    outcome: TurnOutcome | None
    stop_requested: bool
    cause_kind: str
    cause_id: str
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None


@dataclass(frozen=True, slots=True)
class ProjectedTurnEntryPage:
    project_id: str
    items: tuple[ProjectedTurnEntry, ...]
    limit: int
    diagnostics: tuple[ProjectionDiagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectedTurnLineage:
    project_id: str
    turn_id: str
    parent: ProjectedTurnEntry
    direct_children: tuple[ProjectedTurnEntry, ...]
    limit: int
    diagnostics: tuple[ProjectionDiagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectedProjectFeedPage:
    project_id: str
    items: tuple["ProjectedFeedEvent", ...]
    limit: int
    next_after_ledger_seq: int
    diagnostics: tuple[ProjectionDiagnostic, ...] = ()


@dataclass(frozen=True, slots=True)
class ProjectedFeedEvent:
    ledger_seq: int
    event_type: str
    subject_kind: str
    subject_id: str
    actor_kind: str
    actor_id: str
    cause_kind: str | None
    cause_id: str | None
    created_at: datetime
    note: str | None
    annotations: dict[str, object]
    payload_ref: dict[str, str] | None
