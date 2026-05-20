from __future__ import annotations

from CommonGround.contracts import (
    AgentRef,
    CardBoxRef,
    CardBoxPort,
    ClaimToken,
    ClaimRenewal,
    ConflictError,
    DispatchAuthority,
    DispatchAuthorityMode,
    InvariantError,
    NotFoundError,
    OperationMeta,
    ReconcileSummary,
    SemanticRecordRef,
    SpawnTurnSpec,
    TraceContext,
    TurnOutcome,
    TurnRef,
    TurnSnapshot,
    WorkMemoryReportSubmissionResult,
    WorkMemoryReportWriteSpec,
    TruthRepositoryPort,
)


class LifecycleKernel:
    def __init__(self, truth: TruthRepositoryPort, cardbox: CardBoxPort) -> None:
        self._truth = truth
        self._cardbox = cardbox

    def dispatch(
        self,
        *,
        requested_by: AgentRef,
        authority: DispatchAuthority,
        spec: SpawnTurnSpec,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> TurnRef:
        del trace
        self._truth.require_agent_row(requested_by)
        self._truth.require_agent_row(spec.target_agent)
        if spec.bootstrap_bundle_ref.project_id != spec.target_agent.project_id:
            raise ConflictError("bootstrap bundle project must match target agent project")
        if not self._cardbox.box_exists(spec.bootstrap_bundle_ref):
            raise NotFoundError(f"bootstrap bundle not found: {spec.bootstrap_bundle_ref.cardbox_id}")
        if authority.mode == DispatchAuthorityMode.ROOT_REQUEST:
            if not authority.request_id:
                raise ConflictError("root_request dispatch requires request_id")
            if authority.parent_claim is not None:
                raise ConflictError("root_request dispatch cannot include parent_claim")
            if spec.cause.kind != "external_request" or spec.cause.id != authority.request_id:
                raise ConflictError("root_request dispatch cause must match request_id")
            if requested_by.project_id != spec.target_agent.project_id:
                raise ConflictError("requested_by project must match target agent project")
        elif authority.mode == DispatchAuthorityMode.CHILD_DERIVATION:
            if authority.parent_claim is None:
                raise ConflictError("child_derivation dispatch requires parent_claim")
            if authority.request_id is not None:
                raise ConflictError("child_derivation dispatch cannot include request_id")
            if spec.cause.kind != "turn" or spec.cause.id != authority.parent_claim.turn_id:
                raise ConflictError("child_derivation dispatch cause must match parent claim turn")
            if spec.target_agent.project_id != authority.parent_claim.project_id:
                raise ConflictError("child target project must match parent claim project")
        else:
            raise ConflictError(f"unsupported dispatch authority mode: {authority.mode}")
        return self._truth.dispatch_primitive(
            requested_by=requested_by,
            authority=authority,
            spec=spec,
            meta=meta,
        )

    def claim_turn(
        self,
        agent: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> ClaimToken | None:
        del trace
        self._truth.require_agent_row(agent)
        return self._truth.claim_turn_primitive(agent=agent, meta=meta)

    def submit_work_memory_report(
        self,
        spec: WorkMemoryReportWriteSpec,
        *,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> WorkMemoryReportSubmissionResult:
        del trace
        if spec.manifest_ref.project_id != spec.actor.project_id:
            raise ConflictError("manifest ref project must match actor project")
        if not self._cardbox.box_exists(spec.manifest_ref):
            raise NotFoundError(f"manifest box not found: {spec.manifest_ref.cardbox_id}")
        for record in spec.records:
            if record.cardbox_ref.project_id != spec.actor.project_id:
                raise ConflictError("record cardbox project must match actor project")
            if not self._cardbox.box_exists(record.cardbox_ref):
                raise NotFoundError(f"record box not found: {record.cardbox_ref.cardbox_id}")
        if spec.final_payload_ref is not None:
            if spec.final_payload_ref.project_id != spec.actor.project_id:
                raise ConflictError("final payload ref project must match actor project")
            if not self._cardbox.box_exists(spec.final_payload_ref):
                raise NotFoundError(f"final payload box not found: {spec.final_payload_ref.cardbox_id}")
        return self._truth.submit_work_memory_report_primitive(spec=spec, meta=meta)

    def renew_claim(
        self,
        claim: ClaimToken,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> ClaimRenewal:
        del meta, trace
        return self._truth.renew_claim_primitive(claim)

    def suspend_turn(
        self,
        claim: ClaimToken,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        del trace
        self._truth.suspend_turn_primitive(claim=claim, meta=meta)

    def resume_turn(
        self,
        turn: TurnRef,
        *,
        requested_by: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        del trace
        if requested_by.project_id != turn.project_id:
            raise ConflictError("requested_by project must match turn project")
        self._truth.require_agent_row(requested_by)
        self._truth.resume_turn_primitive(turn=turn, requested_by=requested_by, meta=meta)

    def finish_turn(
        self,
        claim: ClaimToken,
        outcome: TurnOutcome,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        del trace
        self._truth.finish_turn_primitive(claim=claim, outcome=outcome, meta=meta)

    def record_and_finish(
        self,
        claim: ClaimToken,
        *,
        outcome: TurnOutcome,
        final_record_role: str | None = None,
        final_cardbox_ref: CardBoxRef | None = None,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> SemanticRecordRef | None:
        del trace
        if final_cardbox_ref is not None and not self._cardbox.box_exists(final_cardbox_ref):
            raise NotFoundError(f"final payload box not found: {final_cardbox_ref.cardbox_id}")
        return self._truth.record_and_finish_primitive(
            claim=claim,
            outcome=outcome,
            final_record_role=final_record_role,
            final_cardbox_ref=final_cardbox_ref,
            meta=meta,
        )

    def request_stop_turn(
        self,
        turn: TurnRef,
        *,
        requested_by: AgentRef,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> None:
        del trace
        if requested_by.project_id != turn.project_id:
            raise ConflictError("requested_by project must match turn project")
        self._truth.require_agent_row(requested_by)
        self._truth.request_stop_turn_primitive(turn=turn, requested_by=requested_by, meta=meta)

    def reconcile_expired_claim(
        self,
        agent: AgentRef | None = None,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> ReconcileSummary:
        del trace
        if agent is not None:
            self._truth.require_agent_row(agent)
        return self._truth.reconcile_expired_claim_primitive(agent=agent, meta=meta)

    def get_turn(self, turn: TurnRef) -> TurnSnapshot | None:
        row = self._truth.get_turn_row(turn)
        if row is None:
            return None
        snapshot = self._truth.turn_snapshot(row)
        if snapshot.final_cardbox_ref is None:
            return snapshot
        hydrated = self._cardbox.hydrate_box(snapshot.final_cardbox_ref)
        if hydrated is None:
            raise InvariantError(f"dangling final cardbox ref: {snapshot.final_cardbox_ref.cardbox_id}")
        final_payload = hydrated.payload()
        return TurnSnapshot(
            turn=snapshot.turn,
            target_agent=snapshot.target_agent,
            turn_kind=snapshot.turn_kind,
            cause=snapshot.cause,
            state=snapshot.state,
            outcome=snapshot.outcome,
            stop_requested=snapshot.stop_requested,
            current_claim_agent_id=snapshot.current_claim_agent_id,
            claim_expires_at=snapshot.claim_expires_at,
            spawn_key=snapshot.spawn_key,
            final_record_role=snapshot.final_record_role,
            final_cardbox_ref=snapshot.final_cardbox_ref,
            final_payload=final_payload,
            created_at=snapshot.created_at,
            updated_at=snapshot.updated_at,
            closed_at=snapshot.closed_at,
        )
