from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
import re
from typing import Any, Mapping

from .cardbox_types import CardBoxLike, CardLike


TURN_KIND_CONVERSATION_V1 = "turn.conversation.v1"
TURN_KIND_PROVISION_AGENT_SPAWN_V1 = "turn.provision.agent.spawn.v1"
TURN_KIND_WORK_MEMORY_REPORT_V1 = "turn.work_memory_report.v1"
WORK_MEMORY_REPORT_MANIFEST_KIND_V1 = "agent_work_memory_report_manifest.v1"
WORK_MEMORY_REPORT_SUBMISSION_KIND_V1 = "work_memory_report.atomic.v1"
WORK_MEMORY_REPORT_MANIFEST_ROLE = "work_memory_report_manifest"
WORK_MEMORY_REPORT_RESULT_ROLE = "work_memory_report_result"
AGENT_CREDENTIAL_STATUS_ACTIVE = "active"
AGENT_CREDENTIAL_STATUS_REVOKED = "revoked"
AGENT_CREDENTIAL_STATUSES = (
    AGENT_CREDENTIAL_STATUS_ACTIVE,
    AGENT_CREDENTIAL_STATUS_REVOKED,
)
DISPATCH_ANCHOR_MAX_LENGTH = 128
DISPATCH_ANCHOR_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$")
DISPATCH_ANCHOR_ALLOWED_CHARS = "letters, digits, '.', '_', '-', ':', '@', and '/'"


def normalize_dispatch_anchor(value: str | None, *, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} is required")
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty after trimming")
    if len(normalized) > DISPATCH_ANCHOR_MAX_LENGTH:
        raise ValueError(f"{field_name} must be at most {DISPATCH_ANCHOR_MAX_LENGTH} characters")
    if DISPATCH_ANCHOR_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must start with a letter or digit and contain only {DISPATCH_ANCHOR_ALLOWED_CHARS}")
    return normalized


class TurnState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUSPENDED = "suspended"
    CLOSED = "closed"


class TurnOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    STOPPED = "stopped"


class LedgerScopeKind(StrEnum):
    AGENT = "agent"
    TURN = "turn"


class DispatchAuthorityMode(StrEnum):
    ROOT_REQUEST = "root_request"
    CHILD_DERIVATION = "child_derivation"


@dataclass(frozen=True, slots=True)
class AgentRef:
    project_id: str
    agent_id: str


@dataclass(frozen=True, slots=True)
class AgentCredentialRef:
    project_id: str
    agent_id: str
    credential_id: str

    def agent_ref(self) -> AgentRef:
        return AgentRef(project_id=self.project_id, agent_id=self.agent_id)


@dataclass(frozen=True, slots=True)
class AgentCredentialIssueResult:
    ref: AgentCredentialRef
    token: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class TurnRef:
    project_id: str
    turn_id: str


@dataclass(frozen=True, slots=True)
class ClaimToken:
    project_id: str
    turn_id: str
    agent_id: str
    token: str
    expires_at: datetime

    def turn_ref(self) -> TurnRef:
        return TurnRef(project_id=self.project_id, turn_id=self.turn_id)

    def agent_ref(self) -> AgentRef:
        return AgentRef(project_id=self.project_id, agent_id=self.agent_id)


@dataclass(frozen=True, slots=True)
class DispatchAuthority:
    mode: DispatchAuthorityMode
    request_id: str | None = None
    parent_claim: ClaimToken | None = None


@dataclass(frozen=True, slots=True)
class SemanticRecordRef:
    project_id: str
    record_id: str


@dataclass(frozen=True, slots=True)
class CauseRef:
    kind: str
    id: str


@dataclass(frozen=True, slots=True)
class OperationMeta:
    note: str | None = None
    reason: str | None = None
    annotations: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TraceContext:
    trace_id: str | None = None
    parent_trace_id: str | None = None
    recursion_depth: int = 0


@dataclass(frozen=True, slots=True)
class CardBoxRef:
    project_id: str
    cardbox_id: str


@dataclass(frozen=True, slots=True)
class HydratedCardBox:
    ref: CardBoxRef
    box: CardBoxLike
    cards: tuple[CardLike, ...]

    def payload(self) -> Any:
        if len(self.cards) != 1:
            raise ValueError("HydratedCardBox.payload() requires a single-card box")
        content = self.cards[0].content
        if hasattr(content, "text"):
            return content.text
        if hasattr(content, "data"):
            return content.data
        raise TypeError(f"unsupported card content for payload extraction: {type(content).__name__}")


@dataclass(frozen=True, slots=True)
class SpawnTurnSpec:
    target_agent: AgentRef
    cause: CauseRef
    bootstrap_bundle_ref: CardBoxRef
    spawn_key: str
    turn_kind: str = TURN_KIND_CONVERSATION_V1
    spawn_entrypoint: str = "system"


@dataclass(frozen=True, slots=True)
class SemanticRecordSpec:
    record_role: str
    cardbox_ref: CardBoxRef


@dataclass(frozen=True, slots=True)
class WorkMemoryReportRecordWriteSpec:
    record_role: str
    cardbox_ref: CardBoxRef


@dataclass(frozen=True, slots=True)
class WorkMemoryReportWriteSpec:
    actor: AgentRef
    request_id: str
    manifest_ref: CardBoxRef
    records: tuple[WorkMemoryReportRecordWriteSpec, ...]
    final_payload_ref: CardBoxRef | None = None


@dataclass(frozen=True, slots=True)
class WorkMemoryReportSubmissionResult:
    status: str
    turn: TurnRef
    record_refs: tuple[SemanticRecordRef, ...]
    final_payload: Any | None = None


@dataclass(frozen=True, slots=True)
class LedgerFeedScope:
    project_id: str
    scope_kind: LedgerScopeKind
    scope_id: str


@dataclass(frozen=True, slots=True)
class AgentSnapshot:
    agent: AgentRef
    enabled: bool
    accepts_work: bool
    capacity: int
    capabilities: tuple[str, ...]
    grants: tuple[str, ...]
    public_metadata: Mapping[str, Any]
    created_at: datetime
    updated_at: datetime
    last_seen_at: datetime | None = None
    role: str | None = None
    description: str | None = None
    registered_by_agent_id: str | None = None
    registration_provenance_kind: str | None = None
    registration_provenance_ref: str | None = None
    registration_provenance_payload_hash: str | None = None
    admitted_spec_hash: str | None = None


@dataclass(frozen=True, slots=True)
class TurnSnapshot:
    turn: TurnRef
    target_agent: AgentRef
    turn_kind: str
    cause: CauseRef
    state: TurnState
    outcome: TurnOutcome | None
    stop_requested: bool
    current_claim_agent_id: str | None
    claim_expires_at: datetime | None
    spawn_key: str
    final_record_role: str | None
    final_cardbox_ref: CardBoxRef | None
    final_payload: Any | None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None


@dataclass(frozen=True, slots=True)
class SemanticRecordSnapshot:
    ref: SemanticRecordRef
    turn: TurnRef
    turn_seq: int
    record_role: str
    cardbox_ref: CardBoxRef
    created_at: datetime


@dataclass(frozen=True, slots=True)
class SemanticRecordPage:
    items: tuple[SemanticRecordSnapshot, ...]
    next_after_turn_seq: int


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    ledger_seq: int
    project_id: str
    event_type: str
    subject_kind: str
    subject_id: str
    actor_kind: str
    actor_id: str
    cause: CauseRef | None
    created_at: datetime
    note: str | None = None
    annotations: Mapping[str, Any] = field(default_factory=dict)
    payload_ref: CardBoxRef | None = None


@dataclass(frozen=True, slots=True)
class LedgerFeedPage:
    items: tuple[LedgerEvent, ...]
    next_after_ledger_seq: int


@dataclass(frozen=True, slots=True)
class ReconcileSummary:
    scanned_count: int
    reconciled_count: int


@dataclass(frozen=True, slots=True)
class ClaimRenewal:
    server_time: datetime
    expires_at: datetime
    recommended_interval_seconds: float


@dataclass(frozen=True, slots=True)
class SpawnEnvelopeRecord:
    project_id: str
    spawn_key: str
    turn_kind: str
    requested_by_agent_id: str
    cause_fingerprint: str
    bootstrap_bundle_ref: CardBoxRef
    target_agent_id: str
    turn_id: str
    created_at: datetime
