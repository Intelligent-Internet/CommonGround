from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import secrets
from typing import Any, Literal, Mapping
import uuid

from psycopg import errors
from psycopg.types.json import Jsonb as Json

from CommonGround.agent_registration import AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT
from CommonGround.contracts import (
    AgentRef,
    AgentRow,
    AgentSnapshot,
    CardBoxRef,
    ClaimRenewal,
    ClaimToken,
    Clock,
    CauseRef,
    ConflictError,
    DispatchAuthority,
    DispatchAuthorityMode,
    FencingError,
    LedgerEvent,
    LedgerFeedPage,
    LedgerFeedScope,
    LedgerScopeKind,
    NotFoundError,
    OperationMeta,
    ReconcileSummary,
    SemanticRecordRef,
    SemanticRecordSnapshot,
    SemanticRecordSpec,
    SpawnEnvelopeRecord,
    SpawnEnvelopeRow,
    SpawnTurnSpec,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_WORK_MEMORY_REPORT_V1,
    WORK_MEMORY_REPORT_MANIFEST_ROLE,
    WORK_MEMORY_REPORT_RESULT_ROLE,
    WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
    SystemClock,
    TurnOutcome,
    TurnRef,
    TurnRow,
    TurnSnapshot,
    TurnState,
    WorkMemoryReportSubmissionResult,
    WorkMemoryReportWriteSpec,
)
from CommonGround.infra.postgres_pool import ConnectionFactory, SyncPostgresConnectionProvider, postgres_connection


_LEDGER_CALLER_ANNOTATION_KEY = "caller"
_LEDGER_SYSTEM_ANNOTATION_KEY = "system"
_LEDGER_META_ORIGINS = {"caller", "system"}
LedgerMetaOrigin = Literal["caller", "system"]
_RESERVED_LEDGER_ANNOTATION_KEYS = frozenset(
    {
        _LEDGER_CALLER_ANNOTATION_KEY,
        _LEDGER_SYSTEM_ANNOTATION_KEY,
        "accepts_work",
        "admitted_spec_hash",
        "bootstrap",
        "owner",
        "record_role",
        "registered_by_agent_id",
        "registration_provenance_kind",
        "registration_provenance_payload_hash",
        "registration_provenance_ref",
        "request_id",
        "requested_by",
        "spawn_entrypoint",
        "submission_kind",
        "turn_kind",
    }
)


class PostgresTruthRepository:
    def __init__(
        self,
        pg_dsn: str | None,
        *,
        claim_timeout: timedelta,
        clock: Clock | None = None,
        connection_provider: SyncPostgresConnectionProvider | None = None,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        if not pg_dsn and connection_provider is None and connection_factory is None:
            raise ValueError("pg_dsn or connection provider is required")
        self._pg_dsn = pg_dsn
        self._claim_timeout = claim_timeout
        self._clock = clock or SystemClock()
        self._connection_provider = connection_provider
        self._connection_factory = connection_factory

    @property
    def claim_timeout(self) -> timedelta:
        return self._claim_timeout

    def now(self) -> datetime:
        return self._clock.now()

    def new_record_id(self) -> str:
        return f"record_{uuid.uuid4().hex}"

    def new_claim_token(self) -> str:
        return secrets.token_urlsafe(24)

    def get_agent_row(self, agent: AgentRef) -> AgentRow | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select project_id, agent_id, role, description, enabled, accepts_work, capacity, capabilities, grants, public_metadata,
                       created_at, updated_at, last_seen_at,
                       registered_by_agent_id, registration_provenance_kind, registration_provenance_ref,
                       registration_provenance_payload_hash, admitted_spec_hash
                from cg_agents
                where project_id = %s and agent_id = %s
                """,
                (agent.project_id, agent.agent_id),
            )
            row = cur.fetchone()
        return None if row is None else self._agent_row(row)

    def require_agent_row(self, agent: AgentRef) -> AgentRow:
        row = self.get_agent_row(agent)
        if row is None:
            raise NotFoundError(f"agent not found: {agent.agent_id}")
        return row

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
        now = self.now()
        with self._connect() as conn, conn.cursor() as cur:
            self._upsert_agent(
                cur,
                agent=agent,
                role=role,
                description=description,
                capabilities=capabilities,
                accepts_work=accepts_work,
                grants=grants,
                enabled=enabled,
                now=now,
            )
            conn.commit()
        return agent

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
        if agent.project_id != registered_by.project_id:
            raise ConflictError("registered_by project must match target agent project")
        now = self.now()
        public_metadata_dict = _required_json_object(dict(public_metadata), "public_metadata")
        with self._connect() as conn, conn.cursor() as cur:
            registrar_row = self._require_agent_row_locked(cur, registered_by, for_update=True)
            if required_grant not in registrar_row.grants:
                raise ConflictError(f"caller missing grant: {required_grant}")
            inserted = self._insert_agent(
                cur,
                agent=agent,
                role=role,
                description=description,
                capabilities=capabilities,
                accepts_work=accepts_work,
                grants=grants,
                enabled=enabled,
                capacity=capacity,
                public_metadata=public_metadata_dict,
                registered_by_agent_id=registered_by.agent_id,
                registration_provenance_kind=registration_provenance_kind,
                registration_provenance_ref=registration_provenance_ref,
                registration_provenance_payload_hash=registration_provenance_payload_hash,
                admitted_spec_hash=admitted_spec_hash,
                now=now,
            )
            if not inserted:
                raise ConflictError(f"agent already exists: {agent.agent_id}")
            row = self._require_agent_row_locked(cur, agent, for_update=False)
            self._insert_agent_event(
                cur,
                agent_row=row,
                event_type="agent.registered",
                actor_kind="agent",
                actor_id=registered_by.agent_id,
                note=None,
                annotations={
                    "registered_by_agent_id": registered_by.agent_id,
                    "registration_provenance_kind": registration_provenance_kind,
                    "registration_provenance_ref": registration_provenance_ref,
                    "registration_provenance_payload_hash": registration_provenance_payload_hash,
                    "admitted_spec_hash": admitted_spec_hash,
                },
            )
            conn.commit()
        return row

    def set_agent_accepts_work_primitive(
        self,
        *,
        agent: AgentRef,
        requested_by: AgentRef,
        accepts_work: bool,
        meta: OperationMeta | None,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            agent_row = self._require_agent_row_locked(cur, agent, for_update=True)
            if requested_by.project_id != agent.project_id:
                raise ConflictError("requested_by project must match agent project")
            if requested_by.agent_id != agent.agent_id and not self._agent_has_grant(cur, requested_by, AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT):
                raise FencingError("requested_by is not authorized to update agent accepts_work")
            if agent_row.accepts_work == accepts_work:
                conn.commit()
                return
            now = self.now()
            cur.execute(
                """
                update cg_agents
                set accepts_work = %s,
                    updated_at = %s
                where project_id = %s and agent_id = %s
                """,
                (accepts_work, now, agent.project_id, agent.agent_id),
            )
            if cur.rowcount == 0:
                raise NotFoundError(f"agent not found: {agent.agent_id}")
            updated_row = self._require_agent_row_locked(cur, agent, for_update=False)
            self._insert_agent_event(
                cur,
                agent_row=updated_row,
                event_type="agent.resumed" if accepts_work else "agent.drained",
                actor_kind="agent",
                actor_id=requested_by.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta, system={"accepts_work": accepts_work}),
            )
            conn.commit()

    def update_agent_presence_primitive(
        self,
        *,
        agent: AgentRef,
        meta: OperationMeta | None,
    ) -> None:
        del meta
        now = self.now()
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update cg_agents
                set last_seen_at = %s
                where project_id = %s and agent_id = %s
                """,
                (now, agent.project_id, agent.agent_id),
            )
            if cur.rowcount == 0:
                raise NotFoundError(f"agent not found: {agent.agent_id}")
            conn.commit()

    def update_agent_public_metadata_primitive(
        self,
        *,
        agent: AgentRef,
        public_metadata: Mapping[str, Any],
        meta: OperationMeta | None,
    ) -> None:
        del meta
        public_metadata_dict = _required_json_object(dict(public_metadata), "public_metadata")
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update cg_agents
                set public_metadata = %s,
                    updated_at = %s
                where project_id = %s and agent_id = %s
                """,
                (Json(public_metadata_dict), self.now(), agent.project_id, agent.agent_id),
            )
            if cur.rowcount == 0:
                raise NotFoundError(f"agent not found: {agent.agent_id}")
            conn.commit()

    def agent_snapshot(self, row: AgentRow) -> AgentSnapshot:
        return AgentSnapshot(
            agent=AgentRef(project_id=row.project_id, agent_id=row.agent_id),
            description=row.description,
            enabled=row.enabled,
            accepts_work=row.accepts_work,
            capacity=row.capacity,
            capabilities=row.capabilities,
            grants=row.grants,
            public_metadata=row.public_metadata,
            created_at=row.created_at,
            updated_at=row.updated_at,
            last_seen_at=row.last_seen_at,
            role=row.role,
            registered_by_agent_id=row.registered_by_agent_id,
            registration_provenance_kind=row.registration_provenance_kind,
            registration_provenance_ref=row.registration_provenance_ref,
            registration_provenance_payload_hash=row.registration_provenance_payload_hash,
            admitted_spec_hash=row.admitted_spec_hash,
        )

    def get_turn_row(self, turn: TurnRef) -> TurnRow | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                       state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                       claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                       created_at, updated_at, closed_at, last_turn_seq
                from cg_turns
                where project_id = %s and turn_id = %s
                """,
                (turn.project_id, turn.turn_id),
            )
            row = cur.fetchone()
        return None if row is None else self._turn_row(row)

    def require_turn_row(self, turn: TurnRef) -> TurnRow:
        row = self.get_turn_row(turn)
        if row is None:
            raise NotFoundError(f"turn not found: {turn.turn_id}")
        return row

    def require_active_claim(self, claim: ClaimToken) -> TurnRow:
        with self._connect() as conn, conn.cursor() as cur:
            row = self._load_active_claim(cur, claim, for_update=False)
        return row

    def turn_snapshot(self, row: TurnRow) -> TurnSnapshot:
        return TurnSnapshot(
            turn=TurnRef(project_id=row.project_id, turn_id=row.turn_id),
            target_agent=AgentRef(project_id=row.project_id, agent_id=row.target_agent_id),
            turn_kind=row.turn_kind,
            cause=CauseRef(kind=row.cause_kind, id=row.cause_id),
            state=row.state,
            outcome=row.outcome,
            stop_requested=row.stop_requested,
            current_claim_agent_id=row.current_claim_agent_id,
            claim_expires_at=row.claim_expires_at,
            spawn_key=row.spawn_key,
            final_record_role=row.final_record_role,
            final_cardbox_ref=None
            if row.final_cardbox_ref_project_id is None or row.final_cardbox_ref_cardbox_id is None
            else CardBoxRef(project_id=row.final_cardbox_ref_project_id, cardbox_id=row.final_cardbox_ref_cardbox_id),
            final_payload=None,
            created_at=row.created_at,
            updated_at=row.updated_at,
            closed_at=row.closed_at,
        )

    def get_semantic_records(self, turn: TurnRef) -> list[SemanticRecordSnapshot]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select project_id, record_id, turn_id, turn_seq, record_role,
                       cardbox_project_id, cardbox_id, created_at
                from cg_semantic_records
                where project_id = %s and turn_id = %s
                order by turn_seq asc
                """,
                (turn.project_id, turn.turn_id),
            )
            rows = cur.fetchall()
        return [self._semantic_record_snapshot(row) for row in rows]

    def fetch_ledger_feed(
        self,
        *,
        project_id: str,
        after_ledger_seq: int,
        scope: LedgerFeedScope | None = None,
        limit: int = 100,
    ) -> LedgerFeedPage:
        with self._connect() as conn, conn.cursor() as cur:
            if scope is None:
                cur.execute(
                    """
                    select ledger_seq, project_id, event_type, subject_kind, subject_id, actor_kind, actor_id,
                           cause_kind, cause_id, created_at, note, annotations,
                           payload_ref_project_id, payload_ref_cardbox_id
                    from cg_kernel_ledger
                    where project_id = %s and ledger_seq > %s
                    order by ledger_seq asc
                    limit %s
                    """,
                    (project_id, after_ledger_seq, limit),
                )
            else:
                cur.execute(
                    """
                    select l.ledger_seq, l.project_id, l.event_type, l.subject_kind, l.subject_id, l.actor_kind, l.actor_id,
                           l.cause_kind, l.cause_id, l.created_at, l.note, l.annotations,
                           l.payload_ref_project_id, l.payload_ref_cardbox_id
                    from cg_ledger_scope_index s
                    join cg_kernel_ledger l on l.ledger_seq = s.ledger_seq
                    where s.project_id = %s and s.scope_kind = %s and s.scope_id = %s and s.ledger_seq > %s
                    order by s.ledger_seq asc
                    limit %s
                    """,
                    (scope.project_id, scope.scope_kind.value, scope.scope_id, after_ledger_seq, limit),
                )
            rows = cur.fetchall()
        items = tuple(self._ledger_event(row) for row in rows)
        next_after = after_ledger_seq if not items else items[-1].ledger_seq
        return LedgerFeedPage(items=items, next_after_ledger_seq=next_after)

    def get_spawn_envelope_row(self, *, project_id: str, spawn_key: str) -> SpawnEnvelopeRow | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select project_id, spawn_key, turn_kind, requested_by_agent_id, cause_fingerprint,
                       bootstrap_bundle_ref_project_id, bootstrap_bundle_ref_cardbox_id,
                       target_agent_id, turn_id, created_at
                from cg_spawn_envelopes
                where project_id = %s and spawn_key = %s
                """,
                (project_id, spawn_key),
            )
            row = cur.fetchone()
        return None if row is None else self._spawn_envelope_row(row)

    def spawn_envelope_snapshot(self, row: SpawnEnvelopeRow) -> SpawnEnvelopeRecord:
        return SpawnEnvelopeRecord(
            project_id=row.project_id,
            spawn_key=row.spawn_key,
            turn_kind=row.turn_kind,
            requested_by_agent_id=row.requested_by_agent_id,
            cause_fingerprint=row.cause_fingerprint,
            bootstrap_bundle_ref=CardBoxRef(
                project_id=row.bootstrap_bundle_ref_project_id,
                cardbox_id=row.bootstrap_bundle_ref_cardbox_id,
            ),
            target_agent_id=row.target_agent_id,
            turn_id=row.turn_id,
            created_at=row.created_at,
        )

    def dispatch_primitive(
        self,
        *,
        requested_by: AgentRef,
        authority: DispatchAuthority,
        spec: SpawnTurnSpec,
        meta: OperationMeta | None,
    ) -> TurnRef:
        with self._connect() as conn, conn.cursor() as cur:
            if authority.mode == DispatchAuthorityMode.CHILD_DERIVATION:
                if authority.parent_claim is None:
                    raise ConflictError("child_derivation dispatch requires parent_claim")
                parent_row = self._load_active_claim(cur, authority.parent_claim, for_update=True)
                if spec.cause.kind != "turn" or spec.cause.id != authority.parent_claim.turn_id:
                    raise ConflictError("child cause must reference parent claim turn")
                if requested_by.project_id != authority.parent_claim.project_id:
                    raise ConflictError("requested_by project must match parent claim project")
                if requested_by.agent_id != authority.parent_claim.agent_id and not self._agent_has_grant(cur, requested_by, "turn.dispatch.any"):
                    raise FencingError("requested_by is not authorized to dispatch child for this parent claim")
                now = self.now()
                renewed_expires_at = now + self.claim_timeout
                cur.execute(
                    """
                    update cg_turns
                    set claim_expires_at = %s,
                        updated_at = %s
                    where project_id = %s and turn_id = %s
                    """,
                    (renewed_expires_at, now, parent_row.project_id, parent_row.turn_id),
                )
            turn = self._spawn_turn_locked(cur, requested_by=requested_by, spec=spec, meta=meta)
            conn.commit()
            return turn

    def submit_work_memory_report_primitive(
        self,
        *,
        spec: WorkMemoryReportWriteSpec,
        meta: OperationMeta | None,
    ) -> WorkMemoryReportSubmissionResult:
        if not spec.request_id:
            raise ConflictError("work-memory report request_id is required")
        if not spec.records:
            raise ConflictError("work-memory report requires at least one record")
        if spec.manifest_ref.project_id != spec.actor.project_id:
            raise ConflictError("manifest ref project must match actor project")
        if spec.final_payload_ref is not None and spec.final_payload_ref.project_id != spec.actor.project_id:
            raise ConflictError("final payload ref project must match actor project")
        for record in spec.records:
            if not record.record_role:
                raise ConflictError("work-memory report record role is required")
            if record.record_role in {WORK_MEMORY_REPORT_MANIFEST_ROLE, WORK_MEMORY_REPORT_RESULT_ROLE}:
                raise ConflictError(f"work-memory report record role is reserved: {record.record_role}")
            if record.cardbox_ref.project_id != spec.actor.project_id:
                raise ConflictError("record cardbox project must match actor project")

        spawn_key = f"work-memory-report:{spec.actor.agent_id}:{spec.request_id}"
        cause = CauseRef(kind="external_request", id=spec.request_id)
        cause_fp = _cause_fingerprint(cause)
        with self._connect() as conn, conn.cursor() as cur:
            existing = self._load_spawn_envelope(cur, project_id=spec.actor.project_id, spawn_key=spawn_key, for_update=True)
            if existing is not None:
                self._assert_work_memory_report_matches(cur, existing, spec=spec, cause_fp=cause_fp, spawn_key=spawn_key)
                turn_ref = TurnRef(project_id=existing.project_id, turn_id=existing.turn_id)
                record_refs = self._work_memory_report_record_refs_locked(cur, turn=turn_ref)
                conn.commit()
                return WorkMemoryReportSubmissionResult(
                    status="already_submitted",
                    turn=turn_ref,
                    record_refs=tuple(record_refs),
                )

            agent_row = self._require_agent_row_locked(cur, spec.actor, for_update=True)
            if not agent_row.enabled:
                raise ConflictError(f"agent disabled: {spec.actor.agent_id}")

            now = self.now()
            project_turn_seq = self._next_project_turn_seq_locked(cur, project_id=spec.actor.project_id)
            turn_id = self._format_project_turn_id(project_turn_seq)
            turn_ref = TurnRef(project_id=spec.actor.project_id, turn_id=turn_id)
            cur.execute(
                """
                insert into cg_turns (
                  project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                  state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                  claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                  created_at, updated_at, closed_at, last_turn_seq
                ) values (%s, %s, %s, %s, %s, %s, %s, 'closed', %s, false, null, null, null, %s, %s, %s, %s, %s, %s, %s, 0)
                """,
                (
                    spec.actor.project_id,
                    turn_id,
                    project_turn_seq,
                    spec.actor.agent_id,
                    TURN_KIND_WORK_MEMORY_REPORT_V1,
                    cause.kind,
                    cause.id,
                    TurnOutcome.SUCCEEDED.value,
                    spawn_key,
                    WORK_MEMORY_REPORT_RESULT_ROLE if spec.final_payload_ref is not None else None,
                    None if spec.final_payload_ref is None else spec.final_payload_ref.project_id,
                    None if spec.final_payload_ref is None else spec.final_payload_ref.cardbox_id,
                    now,
                    now,
                    now,
                ),
            )
            turn_row = self._reload_turn_row(cur, turn_ref, for_update=False)
            self._insert_turn_event(
                cur,
                turn_row=turn_row,
                event_type="turn.spawned",
                actor_kind="agent",
                actor_id=spec.actor.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(
                    meta,
                    system={
                        "requested_by": spec.actor.agent_id,
                        "turn_kind": TURN_KIND_WORK_MEMORY_REPORT_V1,
                        "spawn_entrypoint": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
                        "submission_kind": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
                        "request_id": spec.request_id,
                    },
                ),
                payload_ref=spec.manifest_ref,
            )
            self._append_semantic_record_locked(
                cur,
                claim=None,
                turn_ref=turn_ref,
                actor_agent_id=spec.actor.agent_id,
                record=SemanticRecordSpec(record_role=WORK_MEMORY_REPORT_MANIFEST_ROLE, cardbox_ref=spec.manifest_ref),
                meta=meta,
                annotations={
                    "submission_kind": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
                    "request_id": spec.request_id,
                    "record_role": WORK_MEMORY_REPORT_MANIFEST_ROLE,
                },
            )
            record_refs: list[SemanticRecordRef] = []
            for record in spec.records:
                ref = self._append_semantic_record_locked(
                    cur,
                    claim=None,
                    turn_ref=turn_ref,
                    actor_agent_id=spec.actor.agent_id,
                    record=SemanticRecordSpec(record_role=record.record_role, cardbox_ref=record.cardbox_ref),
                    meta=meta,
                    annotations={
                        "submission_kind": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
                        "request_id": spec.request_id,
                        "record_role": record.record_role,
                    },
                )
                record_refs.append(ref)
            cur.execute(
                """
                insert into cg_spawn_envelopes (
                  project_id, spawn_key, turn_kind, requested_by_agent_id, cause_fingerprint,
                  bootstrap_bundle_ref_project_id, bootstrap_bundle_ref_cardbox_id, target_agent_id,
                  turn_id, created_at
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    spec.actor.project_id,
                    spawn_key,
                    TURN_KIND_WORK_MEMORY_REPORT_V1,
                    spec.actor.agent_id,
                    cause_fp,
                    spec.manifest_ref.project_id,
                    spec.manifest_ref.cardbox_id,
                    spec.actor.agent_id,
                    turn_id,
                    now,
                ),
            )
            finished_row = self._reload_turn_row(cur, turn_ref, for_update=False)
            self._insert_turn_event(
                cur,
                turn_row=finished_row,
                event_type="turn.finished",
                actor_kind="agent",
                actor_id=spec.actor.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(
                    meta,
                    system={
                        "submission_kind": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
                        "request_id": spec.request_id,
                    },
                ),
                payload_ref=spec.final_payload_ref,
            )
            conn.commit()
        return WorkMemoryReportSubmissionResult(
            status="submitted",
            turn=turn_ref,
            record_refs=tuple(record_refs),
        )

    def claim_turn_primitive(
        self,
        *,
        agent: AgentRef,
        meta: OperationMeta | None,
    ) -> ClaimToken | None:
        now = self.now()
        with self._connect() as conn, conn.cursor() as cur:
            agent_row = self._require_agent_row_locked(cur, agent, for_update=True)
            if not agent_row.enabled or not agent_row.accepts_work:
                return None
            cur.execute(
                """
                select t.project_id, t.turn_id, t.project_turn_seq, t.target_agent_id, t.turn_kind, t.cause_kind, t.cause_id,
                       t.state, t.outcome, t.stop_requested, t.current_claim_agent_id, t.current_claim_token_hash,
                       t.claim_expires_at, t.spawn_key, t.final_record_role, t.final_cardbox_ref_project_id, t.final_cardbox_ref_cardbox_id,
                       t.created_at, t.updated_at, t.closed_at, t.last_turn_seq
                from cg_turns t
                where t.project_id = %s
                  and t.target_agent_id = %s
                  and t.state = 'queued'
                  and not exists (
                    select 1
                    from cg_turns x
                    where x.project_id = t.project_id
                      and x.target_agent_id = t.target_agent_id
                      and x.turn_id <> t.turn_id
                      and x.state = 'running'
                  )
                order by t.project_turn_seq asc
                for update skip locked
                limit 1
                """,
                (agent.project_id, agent.agent_id),
            )
            row = cur.fetchone()
            if row is None:
                conn.commit()
                return None
            turn_row = self._turn_row(row)
            token = self.new_claim_token()
            token_hash = _hash_claim(token)
            expires_at = now + self.claim_timeout
            cur.execute(
                """
                update cg_turns
                set state = 'running',
                    current_claim_agent_id = %s,
                    current_claim_token_hash = %s,
                    claim_expires_at = %s,
                    updated_at = %s
                where project_id = %s and turn_id = %s and state = 'queued'
                """,
                (agent.agent_id, token_hash, expires_at, now, turn_row.project_id, turn_row.turn_id),
            )
            if cur.rowcount != 1:
                raise ConflictError("claim lost race")
            claim = ClaimToken(
                project_id=agent.project_id,
                turn_id=turn_row.turn_id,
                agent_id=agent.agent_id,
                token=token,
                expires_at=expires_at,
            )
            self._insert_turn_event(
                cur,
                turn_row=self._reload_turn_row(cur, claim.turn_ref(), for_update=False),
                event_type="turn.claimed",
                actor_kind="agent",
                actor_id=agent.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta),
            )
            conn.commit()
            return claim

    def renew_claim_primitive(self, claim: ClaimToken) -> ClaimRenewal:
        now = self.now()
        with self._connect() as conn, conn.cursor() as cur:
            self._load_active_claim(cur, claim, for_update=True)
            expires_at = now + self.claim_timeout
            cur.execute(
                """
                update cg_turns
                set claim_expires_at = %s,
                    updated_at = %s
                where project_id = %s and turn_id = %s
                """,
                (expires_at, now, claim.project_id, claim.turn_id),
            )
            conn.commit()
        return ClaimRenewal(
            server_time=now,
            expires_at=expires_at,
            recommended_interval_seconds=max(1.0, self.claim_timeout.total_seconds() / 2.0),
        )

    def finish_turn_primitive(
        self,
        *,
        claim: ClaimToken,
        outcome: TurnOutcome,
        meta: OperationMeta | None,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            self._load_active_claim(cur, claim, for_update=True)
            now = self.now()
            cur.execute(
                """
                update cg_turns
                set state = 'closed',
                    outcome = %s,
                    current_claim_agent_id = null,
                    current_claim_token_hash = null,
                    claim_expires_at = null,
                    final_record_role = null,
                    final_cardbox_ref_project_id = null,
                    final_cardbox_ref_cardbox_id = null,
                    updated_at = %s,
                    closed_at = %s
                where project_id = %s and turn_id = %s
                """,
                (outcome.value, now, now, claim.project_id, claim.turn_id),
            )
            finished_row = self._reload_turn_row(cur, claim.turn_ref(), for_update=False)
            self._insert_turn_event(
                cur,
                turn_row=finished_row,
                event_type="turn.finished",
                actor_kind="agent",
                actor_id=claim.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta),
            )
            conn.commit()

    def record_and_finish_primitive(
        self,
        *,
        claim: ClaimToken,
        outcome: TurnOutcome,
        final_record_role: str | None,
        final_cardbox_ref: CardBoxRef | None,
        meta: OperationMeta | None,
    ) -> SemanticRecordRef | None:
        with self._connect() as conn, conn.cursor() as cur:
            self._load_active_claim(cur, claim, for_update=True)
            ref = None
            if final_record_role is not None and final_cardbox_ref is not None:
                ref = self._append_semantic_record_locked(
                    cur,
                    claim=claim,
                    record=SemanticRecordSpec(record_role=final_record_role, cardbox_ref=final_cardbox_ref),
                    meta=meta,
                )
            now = self.now()
            cur.execute(
                """
                update cg_turns
                set state = 'closed',
                    outcome = %s,
                    current_claim_agent_id = null,
                    current_claim_token_hash = null,
                    claim_expires_at = null,
                    final_record_role = %s,
                    final_cardbox_ref_project_id = %s,
                    final_cardbox_ref_cardbox_id = %s,
                    updated_at = %s,
                    closed_at = %s
                where project_id = %s and turn_id = %s
                """,
                (
                    outcome.value,
                    final_record_role,
                    None if final_cardbox_ref is None else final_cardbox_ref.project_id,
                    None if final_cardbox_ref is None else final_cardbox_ref.cardbox_id,
                    now,
                    now,
                    claim.project_id,
                    claim.turn_id,
                ),
            )
            finished_row = self._reload_turn_row(cur, claim.turn_ref(), for_update=False)
            self._insert_turn_event(
                cur,
                turn_row=finished_row,
                event_type="turn.finished",
                actor_kind="agent",
                actor_id=claim.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta),
            )
            conn.commit()
            return ref

    def append_semantic_record_primitive(
        self,
        *,
        claim: ClaimToken,
        record: SemanticRecordSpec,
        meta: OperationMeta | None,
    ) -> SemanticRecordRef:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                self._load_active_claim(cur, claim, for_update=True)
                ref = self._append_semantic_record_locked(cur, claim=claim, record=record, meta=meta)
                conn.commit()
                return ref
        except errors.UniqueViolation as exc:
            raise ConflictError(f"singleton semantic record already exists for role={record.record_role}") from exc

    def suspend_turn_primitive(
        self,
        *,
        claim: ClaimToken,
        meta: OperationMeta | None,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            self._load_active_claim(cur, claim, for_update=True)
            now = self.now()
            cur.execute(
                """
                update cg_turns
                set state = 'suspended',
                    current_claim_agent_id = null,
                    current_claim_token_hash = null,
                    claim_expires_at = null,
                    updated_at = %s
                where project_id = %s and turn_id = %s
                """,
                (now, claim.project_id, claim.turn_id),
            )
            turn_row = self._reload_turn_row(cur, claim.turn_ref(), for_update=False)
            self._insert_turn_event(
                cur,
                turn_row=turn_row,
                event_type="turn.suspended",
                actor_kind="agent",
                actor_id=claim.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta),
            )
            conn.commit()

    def resume_turn_primitive(
        self,
        *,
        turn: TurnRef,
        requested_by: AgentRef,
        meta: OperationMeta | None,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            turn_row = self._reload_turn_row(cur, turn, for_update=True)
            if turn_row.state != TurnState.SUSPENDED:
                raise ConflictError("only suspended turns can be resumed")
            if requested_by.project_id != turn_row.project_id:
                raise ConflictError("requested_by project must match turn project")
            if requested_by.agent_id != turn_row.target_agent_id and not self._agent_has_grant(cur, requested_by, "turn.resume.any"):
                raise FencingError("requested_by is not authorized to resume this turn")
            now = self.now()
            cur.execute(
                """
                update cg_turns
                set state = 'queued',
                    updated_at = %s
                where project_id = %s and turn_id = %s
                """,
                (now, turn.project_id, turn.turn_id),
            )
            resumed_row = self._reload_turn_row(cur, turn, for_update=False)
            self._insert_turn_event(
                cur,
                turn_row=resumed_row,
                event_type="turn.resumed",
                actor_kind="agent",
                actor_id=requested_by.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta),
            )
            conn.commit()

    def request_stop_turn_primitive(
        self,
        *,
        turn: TurnRef,
        requested_by: AgentRef,
        meta: OperationMeta | None,
    ) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            turn_row = self._reload_turn_row(cur, turn, for_update=True)
            if requested_by.project_id != turn_row.project_id:
                raise ConflictError("requested_by project must match turn project")
            if requested_by.agent_id != turn_row.target_agent_id and not self._agent_has_grant(cur, requested_by, "turn.stop.any"):
                raise FencingError("requested_by is not authorized to stop this turn")
            now = self.now()
            should_terminalize_expired_claim = (
                turn_row.state == TurnState.RUNNING
                and turn_row.claim_expires_at is not None
                and turn_row.claim_expires_at <= now
            )
            if turn_row.stop_requested and not should_terminalize_expired_claim:
                conn.commit()
                return
            if should_terminalize_expired_claim:
                cur.execute(
                    """
                    update cg_turns
                    set state = 'closed',
                        outcome = %s,
                        stop_requested = true,
                        current_claim_agent_id = null,
                        current_claim_token_hash = null,
                        claim_expires_at = null,
                        final_record_role = null,
                        final_cardbox_ref_project_id = null,
                        final_cardbox_ref_cardbox_id = null,
                        updated_at = %s,
                        closed_at = %s
                    where project_id = %s and turn_id = %s
                    """,
                    (TurnOutcome.STOPPED.value, now, now, turn.project_id, turn.turn_id),
                )
            else:
                cur.execute(
                    """
                    update cg_turns
                    set stop_requested = true,
                        updated_at = %s
                    where project_id = %s and turn_id = %s
                    """,
                    (now, turn.project_id, turn.turn_id),
                )
            stopped_row = self._reload_turn_row(cur, turn, for_update=False)
            if not turn_row.stop_requested:
                self._insert_turn_event(
                    cur,
                    turn_row=stopped_row,
                    event_type="turn.stop_requested",
                    actor_kind="agent",
                    actor_id=requested_by.agent_id,
                    note=_ledger_event_note(meta),
                    annotations=_ledger_event_annotations(meta),
                )
            if should_terminalize_expired_claim:
                self._insert_turn_event(
                    cur,
                    turn_row=stopped_row,
                    event_type="turn.finished",
                    actor_kind="agent",
                    actor_id=requested_by.agent_id,
                    note=_ledger_event_note(meta),
                    annotations=_ledger_event_annotations(meta),
                )
            conn.commit()

    def reconcile_expired_claim_primitive(
        self,
        *,
        agent: AgentRef | None,
        meta: OperationMeta | None,
    ) -> ReconcileSummary:
        now = self.now()
        with self._connect() as conn, conn.cursor() as cur:
            params: list[object] = [now]
            agent_clause = ""
            if agent is not None:
                agent_clause = " and target_agent_id = %s and project_id = %s"
                params.extend([agent.agent_id, agent.project_id])
            cur.execute(
                f"""
                select project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                       state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                       claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                       created_at, updated_at, closed_at, last_turn_seq
                from cg_turns
                where state = 'running'
                  and claim_expires_at <= %s
                  {agent_clause}
                for update
                """,
                tuple(params),
            )
            rows = [self._turn_row(row) for row in cur.fetchall()]
            scanned_count = len(rows)
            reconciled = 0
            actor_kind = "system" if agent is None else "agent"
            actor_id = "system" if agent is None else agent.agent_id
            for row in rows:
                cur.execute(
                    """
                    update cg_turns
                    set state = 'queued',
                        current_claim_agent_id = null,
                        current_claim_token_hash = null,
                        claim_expires_at = null,
                        updated_at = %s
                    where project_id = %s and turn_id = %s
                    """,
                    (now, row.project_id, row.turn_id),
                )
                reconciled_row = self._reload_turn_row(cur, TurnRef(row.project_id, row.turn_id), for_update=False)
                self._insert_turn_event(
                    cur,
                    turn_row=reconciled_row,
                    event_type="turn.claim_reconciled",
                    actor_kind=actor_kind,
                    actor_id=actor_id,
                    note=_ledger_event_note(meta, meta_origin="system" if agent is None else "caller"),
                    annotations=_ledger_event_annotations(meta, meta_origin="system" if agent is None else "caller"),
                )
                reconciled += 1
            conn.commit()
        return ReconcileSummary(scanned_count=scanned_count, reconciled_count=reconciled)

    def list_turn_rows(self) -> list[TurnRow]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                       state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                       claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                       created_at, updated_at, closed_at, last_turn_seq
                from cg_turns
                order by project_id asc, project_turn_seq asc
                """
            )
            return [self._turn_row(row) for row in cur.fetchall()]

    def list_ledger_events(self) -> list[LedgerEvent]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select ledger_seq, project_id, event_type, subject_kind, subject_id, actor_kind, actor_id,
                       cause_kind, cause_id, created_at, note, annotations,
                       payload_ref_project_id, payload_ref_cardbox_id
                from cg_kernel_ledger
                order by ledger_seq asc
                """
            )
            return [self._ledger_event(row) for row in cur.fetchall()]

    def _connect(self):
        return postgres_connection(
            self._pg_dsn,
            connection_provider=self._connection_provider,
            connection_factory=self._connection_factory,
        )

    @staticmethod
    def _format_project_turn_id(project_turn_seq: int) -> str:
        return f"T-{project_turn_seq}"

    def _next_project_turn_seq_locked(self, cur, *, project_id: str) -> int:
        cur.execute(
            """
            insert into cg_project_turn_counters (project_id, last_assigned_turn_seq)
            values (%s, 1)
            on conflict (project_id) do update
            set last_assigned_turn_seq = cg_project_turn_counters.last_assigned_turn_seq + 1
            returning last_assigned_turn_seq
            """,
            (project_id,),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row[0])

    def _spawn_turn_locked(self, cur, *, requested_by: AgentRef, spec: SpawnTurnSpec, meta: OperationMeta | None) -> TurnRef:
        existing = self._load_spawn_envelope(cur, project_id=requested_by.project_id, spawn_key=spec.spawn_key, for_update=True)
        cause_fp = _cause_fingerprint(spec.cause)
        if existing is not None:
            self._assert_spawn_matches(existing, requested_by=requested_by, spec=spec, cause_fp=cause_fp)
            return TurnRef(project_id=existing.project_id, turn_id=existing.turn_id)

        self._require_agent_exists_locked(cur, requested_by)
        target_row = self._require_agent_row_locked(cur, spec.target_agent)
        _require_turn_capability(target_row, spec.turn_kind)
        now = self.now()
        project_turn_seq = self._next_project_turn_seq_locked(cur, project_id=requested_by.project_id)
        turn_id = self._format_project_turn_id(project_turn_seq)
        turn_ref = TurnRef(project_id=requested_by.project_id, turn_id=turn_id)
        cur.execute(
            """
            insert into cg_turns (
              project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
              state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
              claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
              created_at, updated_at, closed_at, last_turn_seq
            ) values (%s, %s, %s, %s, %s, %s, %s, 'queued', null, false, null, null, null, %s, null, null, null, %s, %s, null, 0)
            """,
            (
                requested_by.project_id,
                turn_id,
                project_turn_seq,
                spec.target_agent.agent_id,
                spec.turn_kind,
                spec.cause.kind,
                spec.cause.id,
                spec.spawn_key,
                now,
                now,
            ),
        )
        cur.execute(
            """
            insert into cg_spawn_envelopes (
              project_id, spawn_key, turn_kind, requested_by_agent_id, cause_fingerprint,
              bootstrap_bundle_ref_project_id, bootstrap_bundle_ref_cardbox_id, target_agent_id,
              turn_id, created_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                requested_by.project_id,
                spec.spawn_key,
                spec.turn_kind,
                requested_by.agent_id,
                cause_fp,
                spec.bootstrap_bundle_ref.project_id,
                spec.bootstrap_bundle_ref.cardbox_id,
                spec.target_agent.agent_id,
                turn_id,
                now,
            ),
        )
        turn_row = self._reload_turn_row(cur, turn_ref, for_update=False)
        self._insert_turn_event(
            cur,
            turn_row=turn_row,
            event_type="turn.spawned",
            actor_kind="agent",
            actor_id=requested_by.agent_id,
            note=_ledger_event_note(meta),
            annotations=_ledger_event_annotations(
                meta,
                system={
                    "requested_by": requested_by.agent_id,
                    "turn_kind": spec.turn_kind,
                    "spawn_entrypoint": spec.spawn_entrypoint,
                },
            ),
            payload_ref=spec.bootstrap_bundle_ref,
        )
        self._append_semantic_record_locked(
            cur,
            claim=None,
            turn_ref=turn_ref,
            actor_agent_id=requested_by.agent_id,
            record=SemanticRecordSpec(record_role="bootstrap", cardbox_ref=spec.bootstrap_bundle_ref),
            meta=None,
        )
        return turn_ref

    def _append_semantic_record_locked(
        self,
        cur,
        *,
        claim: ClaimToken | None,
        record: SemanticRecordSpec,
        meta: OperationMeta | None,
        turn_ref: TurnRef | None = None,
        actor_agent_id: str | None = None,
        annotations: dict[str, object] | None = None,
    ) -> SemanticRecordRef:
        # Claimless append is an internal primitive for born-closed system writes
        # such as atomic work-memory report submission. Public worker mutation
        # routes must continue to pass a fenced claim.
        if turn_ref is None:
            if claim is None:
                raise ValueError("claim or turn_ref is required")
            turn_ref = claim.turn_ref()
        row = self._reload_turn_row(cur, turn_ref, for_update=True)
        now = self.now()
        record_id = self.new_record_id()
        turn_seq = row.last_turn_seq + 1
        cur.execute(
            """
            insert into cg_semantic_records (
              project_id, record_id, turn_id, turn_seq, record_role,
              cardbox_project_id, cardbox_id, created_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                turn_ref.project_id,
                record_id,
                turn_ref.turn_id,
                turn_seq,
                record.record_role,
                record.cardbox_ref.project_id,
                record.cardbox_ref.cardbox_id,
                now,
            ),
        )
        cur.execute(
            """
            update cg_turns
            set last_turn_seq = %s,
                updated_at = %s
            where project_id = %s and turn_id = %s
            """,
            (turn_seq, now, turn_ref.project_id, turn_ref.turn_id),
        )
        updated_row = self._reload_turn_row(cur, turn_ref, for_update=False)
        if claim is not None:
            self._insert_turn_event(
                cur,
                turn_row=updated_row,
                event_type="turn.progress_appended",
                actor_kind="agent",
                actor_id=claim.agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta, system={"record_role": record.record_role}),
                payload_ref=record.cardbox_ref,
            )
        elif actor_agent_id is not None:
            event_type = "turn.bootstrap_recorded" if record.record_role == "bootstrap" else "turn.progress_appended"
            self._insert_turn_event(
                cur,
                turn_row=updated_row,
                event_type=event_type,
                actor_kind="agent",
                actor_id=actor_agent_id,
                note=_ledger_event_note(meta),
                annotations=_ledger_event_annotations(meta, system=annotations or {"record_role": record.record_role}),
                payload_ref=record.cardbox_ref,
            )
        return SemanticRecordRef(project_id=turn_ref.project_id, record_id=record_id)

    def _load_active_claim(self, cur, claim: ClaimToken, *, for_update: bool) -> TurnRow:
        row = self._reload_turn_row(cur, claim.turn_ref(), for_update=for_update)
        token_hash = _hash_claim(claim.token)
        if row.state != TurnState.RUNNING:
            raise FencingError(f"turn is not running: {claim.turn_id}")
        if row.current_claim_agent_id != claim.agent_id or row.current_claim_token_hash != token_hash:
            raise FencingError(f"claim token mismatch for turn {claim.turn_id}")
        if row.claim_expires_at is None or row.claim_expires_at <= self.now():
            raise FencingError(f"claim expired for turn {claim.turn_id}")
        return row

    def _reload_turn_row(self, cur, turn: TurnRef, *, for_update: bool) -> TurnRow:
        clause = " for update" if for_update else ""
        cur.execute(
            f"""
            select project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                   state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                   claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                   created_at, updated_at, closed_at, last_turn_seq
            from cg_turns
            where project_id = %s and turn_id = %s
            {clause}
            """,
            (turn.project_id, turn.turn_id),
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"turn not found: {turn.turn_id}")
        return self._turn_row(row)

    def _require_agent_exists_locked(self, cur, agent: AgentRef) -> None:
        cur.execute(
            "select 1 from cg_agents where project_id = %s and agent_id = %s",
            (agent.project_id, agent.agent_id),
        )
        if cur.fetchone() is None:
            raise NotFoundError(f"agent not found: {agent.agent_id}")

    def _require_agent_row_locked(self, cur, agent: AgentRef, *, for_update: bool = False) -> AgentRow:
        clause = " for update" if for_update else ""
        cur.execute(
            f"""
            select project_id, agent_id, role, description, enabled, accepts_work, capacity, capabilities, grants, public_metadata,
                   created_at, updated_at, last_seen_at,
                   registered_by_agent_id, registration_provenance_kind, registration_provenance_ref,
                   registration_provenance_payload_hash, admitted_spec_hash
            from cg_agents
            where project_id = %s and agent_id = %s
            {clause}
            """,
            (agent.project_id, agent.agent_id),
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"agent not found: {agent.agent_id}")
        return self._agent_row(row)

    def _agent_has_grant(self, cur, agent: AgentRef, grant: str) -> bool:
        cur.execute(
            "select grants from cg_agents where project_id = %s and agent_id = %s",
            (agent.project_id, agent.agent_id),
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"agent not found: {agent.agent_id}")
        grants = tuple(row[0] or [])
        return grant in grants

    def _upsert_agent(
        self,
        cur,
        *,
        agent: AgentRef,
        role: str | None,
        description: str | None,
        capabilities: tuple[str, ...],
        accepts_work: bool,
        grants: tuple[str, ...],
        enabled: bool,
        now: datetime,
    ) -> None:
        cur.execute(
            """
            insert into cg_agents (
              project_id, agent_id, role, description, enabled, accepts_work, capacity, capabilities, grants, public_metadata,
              created_at, updated_at, last_seen_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, null)
            on conflict (project_id, agent_id) do update
            set role = excluded.role,
                description = cg_agents.description,
                enabled = excluded.enabled,
                accepts_work = excluded.accepts_work,
                capacity = excluded.capacity,
                capabilities = excluded.capabilities,
                grants = excluded.grants,
                updated_at = excluded.updated_at
            """,
            (
                agent.project_id,
                agent.agent_id,
                role,
                description,
                enabled,
                accepts_work,
                1,
                list(capabilities),
                list(grants),
                Json({}),
                now,
                now,
            ),
        )

    def _insert_agent(
        self,
        cur,
        *,
        agent: AgentRef,
        role: str | None,
        description: str | None,
        capabilities: tuple[str, ...],
        accepts_work: bool,
        grants: tuple[str, ...],
        enabled: bool,
        now: datetime,
        capacity: int = 1,
        public_metadata: Mapping[str, Any] | None = None,
        registered_by_agent_id: str | None = None,
        registration_provenance_kind: str | None = None,
        registration_provenance_ref: str | None = None,
        registration_provenance_payload_hash: str | None = None,
        admitted_spec_hash: str | None = None,
    ) -> bool:
        cur.execute(
            """
            insert into cg_agents (
              project_id, agent_id, role, description, enabled, accepts_work, capacity, capabilities, grants, public_metadata,
              registered_by_agent_id, registration_provenance_kind, registration_provenance_ref,
              registration_provenance_payload_hash, admitted_spec_hash,
              created_at, updated_at, last_seen_at
            ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, null)
            on conflict (project_id, agent_id) do nothing
            """,
            (
                agent.project_id,
                agent.agent_id,
                role,
                description,
                enabled,
                accepts_work,
                capacity,
                list(capabilities),
                list(grants),
                Json({} if public_metadata is None else dict(public_metadata)),
                registered_by_agent_id,
                registration_provenance_kind,
                registration_provenance_ref,
                registration_provenance_payload_hash,
                admitted_spec_hash,
                now,
                now,
            ),
        )
        return cur.rowcount == 1

    def _load_spawn_envelope(self, cur, *, project_id: str, spawn_key: str, for_update: bool) -> SpawnEnvelopeRow | None:
        clause = " for update" if for_update else ""
        cur.execute(
            f"""
            select project_id, spawn_key, turn_kind, requested_by_agent_id, cause_fingerprint,
                   bootstrap_bundle_ref_project_id, bootstrap_bundle_ref_cardbox_id,
                   target_agent_id, turn_id, created_at
            from cg_spawn_envelopes
            where project_id = %s and spawn_key = %s
            {clause}
            """,
            (project_id, spawn_key),
        )
        row = cur.fetchone()
        return None if row is None else self._spawn_envelope_row(row)

    def _assert_spawn_matches(
        self,
        row: SpawnEnvelopeRow,
        *,
        requested_by: AgentRef,
        spec: SpawnTurnSpec,
        cause_fp: str,
    ) -> None:
        if row.requested_by_agent_id != requested_by.agent_id:
            raise ConflictError(f"spawn key conflict for {spec.spawn_key}: requested_by differs")
        if row.turn_kind != spec.turn_kind:
            raise ConflictError(f"spawn key conflict for {spec.spawn_key}: turn_kind differs")
        if row.cause_fingerprint != cause_fp:
            raise ConflictError(f"spawn key conflict for {spec.spawn_key}: cause differs")
        if row.bootstrap_bundle_ref_cardbox_id != spec.bootstrap_bundle_ref.cardbox_id:
            raise ConflictError(f"spawn key conflict for {spec.spawn_key}: bootstrap bundle differs")
        if row.target_agent_id != spec.target_agent.agent_id:
            raise ConflictError(f"spawn key conflict for {spec.spawn_key}: target_agent differs")

    def _assert_work_memory_report_matches(
        self,
        cur,
        row: SpawnEnvelopeRow,
        *,
        spec: WorkMemoryReportWriteSpec,
        cause_fp: str,
        spawn_key: str,
    ) -> None:
        if row.requested_by_agent_id != spec.actor.agent_id:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: requested_by differs")
        if row.turn_kind != TURN_KIND_WORK_MEMORY_REPORT_V1:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: turn_kind differs")
        if row.cause_fingerprint != cause_fp:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: cause differs")
        if row.bootstrap_bundle_ref_project_id != spec.manifest_ref.project_id:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: manifest project differs")
        if row.bootstrap_bundle_ref_cardbox_id != spec.manifest_ref.cardbox_id:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: manifest differs")
        if row.target_agent_id != spec.actor.agent_id:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: target_agent differs")
        turn_ref = TurnRef(project_id=row.project_id, turn_id=row.turn_id)
        turn_row = self._reload_turn_row(cur, turn_ref, for_update=True)
        if turn_row.turn_kind != TURN_KIND_WORK_MEMORY_REPORT_V1:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: existing turn_kind differs")
        if turn_row.target_agent_id != spec.actor.agent_id:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: existing target_agent differs")
        if turn_row.state != TurnState.CLOSED or turn_row.outcome != TurnOutcome.SUCCEEDED:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: existing turn is not closed/succeeded")
        if (
            turn_row.current_claim_agent_id is not None
            or turn_row.current_claim_token_hash is not None
            or turn_row.claim_expires_at is not None
        ):
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: existing turn has claim state")
        expected_final_role = WORK_MEMORY_REPORT_RESULT_ROLE if spec.final_payload_ref is not None else None
        if turn_row.final_record_role != expected_final_role:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: final role differs")
        expected_final_project = None if spec.final_payload_ref is None else spec.final_payload_ref.project_id
        expected_final_cardbox = None if spec.final_payload_ref is None else spec.final_payload_ref.cardbox_id
        if turn_row.final_cardbox_ref_project_id != expected_final_project:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: final payload project differs")
        if turn_row.final_cardbox_ref_cardbox_id != expected_final_cardbox:
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: final payload differs")
        self._assert_work_memory_report_records_match(cur, turn=turn_ref, spec=spec, spawn_key=spawn_key)

    def _assert_work_memory_report_records_match(
        self,
        cur,
        *,
        turn: TurnRef,
        spec: WorkMemoryReportWriteSpec,
        spawn_key: str,
    ) -> None:
        cur.execute(
            """
            select record_role, cardbox_project_id, cardbox_id
            from cg_semantic_records
            where project_id = %s
              and turn_id = %s
              and record_role != %s
            order by turn_seq asc
            """,
            (turn.project_id, turn.turn_id, WORK_MEMORY_REPORT_MANIFEST_ROLE),
        )
        rows = cur.fetchall()
        if len(rows) != len(spec.records):
            raise ConflictError(f"work-memory report key conflict for {spawn_key}: record count differs")
        for index, (row, expected) in enumerate(zip(rows, spec.records, strict=True), start=1):
            if row[0] != expected.record_role:
                raise ConflictError(f"work-memory report key conflict for {spawn_key}: record {index} role differs")
            if row[1] != expected.cardbox_ref.project_id:
                raise ConflictError(f"work-memory report key conflict for {spawn_key}: record {index} project differs")
            if row[2] != expected.cardbox_ref.cardbox_id:
                raise ConflictError(f"work-memory report key conflict for {spawn_key}: record {index} payload differs")

    def _work_memory_report_record_refs_locked(self, cur, *, turn: TurnRef) -> list[SemanticRecordRef]:
        cur.execute(
            """
            select project_id, record_id, turn_id, turn_seq, record_role,
                   cardbox_project_id, cardbox_id, created_at
            from cg_semantic_records
            where project_id = %s
              and turn_id = %s
              and record_role != %s
            order by turn_seq asc
            """,
            (turn.project_id, turn.turn_id, WORK_MEMORY_REPORT_MANIFEST_ROLE),
        )
        return [self._semantic_record_snapshot(row).ref for row in cur.fetchall()]

    def _insert_turn_event(
        self,
        cur,
        *,
        turn_row: TurnRow,
        event_type: str,
        actor_kind: str,
        actor_id: str,
        note: str | None,
        annotations: dict[str, object],
        payload_ref: CardBoxRef | None = None,
    ) -> None:
        cur.execute(
            """
            insert into cg_kernel_ledger (
              project_id, event_type, subject_kind, subject_id, actor_kind, actor_id,
              cause_kind, cause_id, created_at, note, annotations,
              payload_ref_project_id, payload_ref_cardbox_id
            ) values (%s, %s, 'turn', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            returning ledger_seq
            """,
            (
                turn_row.project_id,
                event_type,
                turn_row.turn_id,
                actor_kind,
                actor_id,
                turn_row.cause_kind,
                turn_row.cause_id,
                self.now(),
                note,
                Json(annotations),
                None if payload_ref is None else payload_ref.project_id,
                None if payload_ref is None else payload_ref.cardbox_id,
            ),
        )
        ledger_seq = cur.fetchone()[0]
        scope_rows = [
            (turn_row.project_id, LedgerScopeKind.AGENT.value, turn_row.target_agent_id, ledger_seq),
            (turn_row.project_id, LedgerScopeKind.TURN.value, turn_row.turn_id, ledger_seq),
        ]
        if event_type in {"turn.spawned", "turn.finished"} and turn_row.cause_kind == "turn":
            scope_rows.append((turn_row.project_id, LedgerScopeKind.TURN.value, turn_row.cause_id, ledger_seq))
        for scope_row in scope_rows:
            cur.execute(
                """
                insert into cg_ledger_scope_index (project_id, scope_kind, scope_id, ledger_seq)
                values (%s, %s, %s, %s)
                on conflict do nothing
                """,
                scope_row,
            )

    def _insert_agent_event(
        self,
        cur,
        *,
        agent_row: AgentRow,
        event_type: str,
        actor_kind: str,
        actor_id: str,
        note: str | None,
        annotations: dict[str, object],
    ) -> None:
        cur.execute(
            """
            insert into cg_kernel_ledger (
              project_id, event_type, subject_kind, subject_id, actor_kind, actor_id,
              cause_kind, cause_id, created_at, note, annotations,
              payload_ref_project_id, payload_ref_cardbox_id
            ) values (%s, %s, 'agent', %s, %s, %s, null, null, %s, %s, %s, null, null)
            returning ledger_seq
            """,
            (
                agent_row.project_id,
                event_type,
                agent_row.agent_id,
                actor_kind,
                actor_id,
                self.now(),
                note,
                Json(annotations),
            ),
        )
        ledger_seq = cur.fetchone()[0]
        cur.execute(
            """
            insert into cg_ledger_scope_index (project_id, scope_kind, scope_id, ledger_seq)
            values (%s, %s, %s, %s)
            on conflict do nothing
            """,
            (agent_row.project_id, LedgerScopeKind.AGENT.value, agent_row.agent_id, ledger_seq),
        )

    @staticmethod
    def _agent_row(row) -> AgentRow:
        return AgentRow(
            project_id=row[0],
            agent_id=row[1],
            role=row[2],
            description=row[3],
            enabled=row[4],
            accepts_work=row[5],
            capacity=row[6],
            capabilities=tuple(row[7] or ()),
            grants=tuple(row[8] or ()),
            public_metadata=_required_json_object(row[9], "public_metadata"),
            created_at=row[10],
            updated_at=row[11],
            last_seen_at=row[12],
            registered_by_agent_id=row[13],
            registration_provenance_kind=row[14],
            registration_provenance_ref=row[15],
            registration_provenance_payload_hash=row[16],
            admitted_spec_hash=row[17],
        )

    @staticmethod
    def _turn_row(row) -> TurnRow:
        return TurnRow(
            project_id=row[0],
            turn_id=row[1],
            project_turn_seq=row[2],
            target_agent_id=row[3],
            turn_kind=row[4] or TURN_KIND_CONVERSATION_V1,
            cause_kind=row[5],
            cause_id=row[6],
            state=TurnState(row[7]),
            outcome=None if row[8] is None else TurnOutcome(row[8]),
            stop_requested=row[9],
            current_claim_agent_id=row[10],
            current_claim_token_hash=row[11],
            claim_expires_at=row[12],
            spawn_key=row[13],
            final_record_role=row[14],
            final_cardbox_ref_project_id=row[15],
            final_cardbox_ref_cardbox_id=row[16],
            created_at=row[17],
            updated_at=row[18],
            closed_at=row[19],
            last_turn_seq=row[20],
        )

    @staticmethod
    def _semantic_record_snapshot(row) -> SemanticRecordSnapshot:
        return SemanticRecordSnapshot(
            ref=SemanticRecordRef(project_id=row[0], record_id=row[1]),
            turn=TurnRef(project_id=row[0], turn_id=row[2]),
            turn_seq=row[3],
            record_role=row[4],
            cardbox_ref=CardBoxRef(project_id=row[5], cardbox_id=row[6]),
            created_at=row[7],
        )

    @staticmethod
    def _spawn_envelope_row(row) -> SpawnEnvelopeRow:
        return SpawnEnvelopeRow(
            project_id=row[0],
            spawn_key=row[1],
            turn_kind=row[2] or TURN_KIND_CONVERSATION_V1,
            requested_by_agent_id=row[3],
            cause_fingerprint=row[4],
            bootstrap_bundle_ref_project_id=row[5],
            bootstrap_bundle_ref_cardbox_id=row[6],
            target_agent_id=row[7],
            turn_id=row[8],
            created_at=row[9],
        )

    @staticmethod
    def _ledger_event(row) -> LedgerEvent:
        payload_ref = None if row[12] is None else CardBoxRef(project_id=row[12], cardbox_id=row[13])
        cause = None if row[7] is None else CauseRef(kind=row[7], id=row[8])
        return LedgerEvent(
            ledger_seq=row[0],
            project_id=row[1],
            event_type=row[2],
            subject_kind=row[3],
            subject_id=row[4],
            actor_kind=row[5],
            actor_id=row[6],
            cause=cause,
            created_at=row[9],
            note=row[10],
            annotations=dict(row[11] or {}),
            payload_ref=payload_ref,
        )


def _ledger_event_note(meta: OperationMeta | None, *, meta_origin: LedgerMetaOrigin = "caller") -> str | None:
    _validate_ledger_meta_origin(meta_origin)
    if meta is None or meta_origin == "caller":
        return None
    return meta.note


def _ledger_event_annotations(
    meta: OperationMeta | None,
    *,
    system: Mapping[str, object] | None = None,
    meta_origin: LedgerMetaOrigin = "caller",
) -> dict[str, object]:
    _validate_ledger_meta_origin(meta_origin)
    annotations: dict[str, object] = {}
    system_annotations: dict[str, object] = {} if system is None else dict(system)
    if meta is not None and meta_origin == "system":
        if meta.note is not None:
            system_annotations["note"] = meta.note
        if meta.reason is not None:
            system_annotations["reason"] = meta.reason
        system_annotations.update(dict(meta.annotations))
    if system_annotations:
        annotations.update(system_annotations)
        annotations[_LEDGER_SYSTEM_ANNOTATION_KEY] = dict(system_annotations)

    if meta is not None and meta_origin == "caller":
        caller: dict[str, object] = {}
        if meta.note is not None:
            caller["note"] = meta.note
        if meta.reason is not None:
            caller["reason"] = meta.reason
        caller_annotations = dict(meta.annotations)
        if caller_annotations:
            caller["annotations"] = caller_annotations
            reserved_keys = sorted(key for key in caller_annotations if key in _RESERVED_LEDGER_ANNOTATION_KEYS)
            if reserved_keys:
                caller["reserved_annotation_keys"] = reserved_keys
        if caller:
            annotations[_LEDGER_CALLER_ANNOTATION_KEY] = caller
    return annotations


def _validate_ledger_meta_origin(meta_origin: str) -> None:
    if meta_origin not in _LEDGER_META_ORIGINS:
        raise ValueError(f"unsupported ledger meta origin: {meta_origin}")


def _hash_claim(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _cause_fingerprint(cause: CauseRef) -> str:
    return json.dumps({"kind": cause.kind, "id": cause.id}, sort_keys=True)


def _required_json_object(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ConflictError(f"{field_name} must be a JSON object")
    return dict(value)


def _require_turn_capability(row: AgentRow, turn_kind: str) -> None:
    if not turn_kind.startswith("turn."):
        return
    if turn_kind not in row.capabilities:
        raise ConflictError(f"target agent {row.agent_id} missing required capability for turn_kind {turn_kind}")
