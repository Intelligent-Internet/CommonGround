from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping, Protocol

from .cardbox_types import CardBoxLike

from .models import (
    AgentRef,
    AgentSnapshot,
    CardBoxRef,
    ClaimToken,
    ClaimRenewal,
    DispatchAuthority,
    HydratedCardBox,
    LedgerEvent,
    LedgerFeedPage,
    LedgerFeedScope,
    OperationMeta,
    ReconcileSummary,
    SemanticRecordRef,
    SemanticRecordSnapshot,
    SemanticRecordSpec,
    SpawnEnvelopeRecord,
    SpawnTurnSpec,
    TurnOutcome,
    TurnRef,
    TurnSnapshot,
    WorkMemoryReportSubmissionResult,
    WorkMemoryReportWriteSpec,
)
from .rows import AgentRow, SpawnEnvelopeRow, TurnRow


class CardBoxPort(Protocol):
    def create_payload_box(
        self,
        project_id: str,
        payload: Any,
        *,
        write_key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> CardBoxRef:
        ...

    def box_exists(self, ref: CardBoxRef) -> bool:
        ...

    def load_box(self, ref: CardBoxRef) -> CardBoxLike | None:
        ...

    def hydrate_box(self, ref: CardBoxRef) -> HydratedCardBox | None:
        ...


class TruthRepositoryPort(Protocol):
    @property
    def claim_timeout(self) -> timedelta:
        ...

    def now(self) -> datetime:
        ...

    def new_record_id(self) -> str:
        ...

    def new_claim_token(self) -> str:
        ...

    def get_agent_row(self, agent: AgentRef) -> AgentRow | None:
        ...

    def require_agent_row(self, agent: AgentRef) -> AgentRow:
        ...

    def register_agent_primitive(
        self,
        *,
        agent: AgentRef,
        role: str | None,
        description: str | None,
        capabilities: tuple[str, ...],
        accepts_work: bool,
        grants: tuple[str, ...],
        enabled: bool,
    ) -> AgentRef:
        ...

    def insert_agent_birth_primitive(
        self,
        *,
        agent: AgentRef,
        role: str,
        description: str | None,
        capabilities: tuple[str, ...],
        accepts_work: bool,
        grants: tuple[str, ...],
        enabled: bool,
        capacity: int,
        public_metadata: Mapping[str, Any],
        registered_by: AgentRef,
        required_grant: str,
        registration_provenance_kind: str,
        registration_provenance_ref: str,
        registration_provenance_payload_hash: str | None,
        admitted_spec_hash: str,
    ) -> AgentRow:
        ...

    def set_agent_accepts_work_primitive(
        self,
        *,
        agent: AgentRef,
        requested_by: AgentRef,
        accepts_work: bool,
        meta: OperationMeta | None,
    ) -> None:
        ...

    def update_agent_presence_primitive(
        self,
        *,
        agent: AgentRef,
        meta: OperationMeta | None,
    ) -> None:
        ...

    def update_agent_public_metadata_primitive(
        self,
        *,
        agent: AgentRef,
        public_metadata: Mapping[str, Any],
        meta: OperationMeta | None,
    ) -> None:
        ...

    def agent_snapshot(self, row: AgentRow) -> AgentSnapshot:
        ...

    def get_turn_row(self, turn: TurnRef) -> TurnRow | None:
        ...

    def require_turn_row(self, turn: TurnRef) -> TurnRow:
        ...

    def require_active_claim(self, claim: ClaimToken) -> TurnRow:
        ...

    def turn_snapshot(self, row: TurnRow) -> TurnSnapshot:
        ...

    def get_semantic_records(self, turn: TurnRef) -> list[SemanticRecordSnapshot]:
        ...

    def fetch_ledger_feed(
        self,
        *,
        project_id: str,
        after_ledger_seq: int,
        scope: LedgerFeedScope | None = None,
        limit: int = 100,
    ) -> LedgerFeedPage:
        ...

    def get_spawn_envelope_row(self, *, project_id: str, spawn_key: str) -> SpawnEnvelopeRow | None:
        ...

    def spawn_envelope_snapshot(self, row: SpawnEnvelopeRow) -> SpawnEnvelopeRecord:
        ...

    def dispatch_primitive(
        self,
        *,
        requested_by: AgentRef,
        authority: DispatchAuthority,
        spec: SpawnTurnSpec,
        meta: OperationMeta | None,
    ) -> TurnRef:
        ...

    def submit_work_memory_report_primitive(
        self,
        *,
        spec: WorkMemoryReportWriteSpec,
        meta: OperationMeta | None,
    ) -> WorkMemoryReportSubmissionResult:
        ...

    def claim_turn_primitive(
        self,
        *,
        agent: AgentRef,
        meta: OperationMeta | None,
    ) -> ClaimToken | None:
        ...

    def renew_claim_primitive(self, claim: ClaimToken) -> ClaimRenewal:
        ...

    def finish_turn_primitive(
        self,
        *,
        claim: ClaimToken,
        outcome: TurnOutcome,
        meta: OperationMeta | None,
    ) -> None:
        ...

    def record_and_finish_primitive(
        self,
        *,
        claim: ClaimToken,
        outcome: TurnOutcome,
        final_record_role: str | None,
        final_cardbox_ref: CardBoxRef | None,
        meta: OperationMeta | None,
    ) -> SemanticRecordRef | None:
        ...

    def append_semantic_record_primitive(
        self,
        *,
        claim: ClaimToken,
        record: SemanticRecordSpec,
        meta: OperationMeta | None,
    ) -> SemanticRecordRef:
        ...

    def suspend_turn_primitive(
        self,
        *,
        claim: ClaimToken,
        meta: OperationMeta | None,
    ) -> None:
        ...

    def resume_turn_primitive(
        self,
        *,
        turn: TurnRef,
        requested_by: AgentRef,
        meta: OperationMeta | None,
    ) -> None:
        ...

    def request_stop_turn_primitive(
        self,
        *,
        turn: TurnRef,
        requested_by: AgentRef,
        meta: OperationMeta | None,
    ) -> None:
        ...

    def reconcile_expired_claim_primitive(
        self,
        *,
        agent: AgentRef | None,
        meta: OperationMeta | None,
    ) -> ReconcileSummary:
        ...


class TruthDebugPort(Protocol):
    def list_turn_rows(self) -> list[TurnRow]:
        ...

    def list_ledger_events(self) -> list[LedgerEvent]:
        ...
