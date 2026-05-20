from __future__ import annotations

from typing import Any, Mapping

from CommonGround.contracts import (
    AgentRef,
    CardBoxPort,
    CauseRef,
    ConflictError,
    DispatchAuthority,
    DispatchAuthorityMode,
    InvariantError,
    LedgerFeedPage,
    LedgerFeedScope,
    LedgerScopeKind,
    NotFoundError,
    OperationMeta,
    SpawnTurnSpec,
    TURN_KIND_CONVERSATION_V1,
    WORK_MEMORY_REPORT_MANIFEST_KIND_V1,
    WORK_MEMORY_REPORT_MANIFEST_ROLE,
    WORK_MEMORY_REPORT_RESULT_ROLE,
    WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
    TraceContext,
    TurnOutcome,
    TurnRef,
    WorkMemoryReportRecordWriteSpec,
    WorkMemoryReportSubmissionResult,
    WorkMemoryReportWriteSpec,
    normalize_dispatch_anchor,
)
from CommonGround.kernel import LedgerKernel, LifecycleKernel, SemanticKernel, TopologyKernel
from CommonGround.payloads import UNSET_PAYLOAD
from CommonGround.sdk.context import KernelOpContext, SemanticContextItem, TurnContext


class KernelSDK:
    def __init__(
        self,
        *,
        topology: TopologyKernel,
        lifecycle: LifecycleKernel,
        semantic: SemanticKernel,
        ledger: LedgerKernel,
        cardbox: CardBoxPort,
    ) -> None:
        self.topology = topology
        self.lifecycle = lifecycle
        self.semantic = semantic
        self.ledger = ledger
        self.cardbox = cardbox

    def dispatch(
        self,
        *,
        requested_by: AgentRef,
        target_agent: AgentRef,
        input_payload: Any,
        authority: DispatchAuthority,
        dispatch_key: str,
        turn_kind: str = TURN_KIND_CONVERSATION_V1,
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> TurnRef:
        dispatch_key = _normalize_dispatch_anchor_or_conflict(dispatch_key, field_name="dispatch_key")
        if authority.mode == DispatchAuthorityMode.ROOT_REQUEST:
            request_id = _normalize_dispatch_anchor_or_conflict(authority.request_id, field_name="request_id")
            authority = DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=request_id)
            cause = CauseRef(kind="external_request", id=request_id)
            write_key = f"dispatch-root:{target_agent.agent_id}:{request_id}:{dispatch_key}"
            spawn_entrypoint = "dispatch.root_request"
            dispatch_meta = meta
        elif authority.mode == DispatchAuthorityMode.CHILD_DERIVATION:
            if authority.parent_claim is None:
                raise ConflictError("child_derivation dispatch requires parent_claim")
            if target_agent.project_id != authority.parent_claim.project_id:
                raise ConflictError("child target project must match parent turn project")
            cause = CauseRef(kind="turn", id=authority.parent_claim.turn_id)
            write_key = f"dispatch:child:{authority.parent_claim.turn_id}:{target_agent.agent_id}:{dispatch_key}"
            spawn_entrypoint = "dispatch.child_derivation"
            dispatch_meta = meta
        else:
            raise ConflictError(f"unsupported dispatch authority mode: {authority.mode}")
        bootstrap_ref = self.cardbox.create_payload_box(
            target_agent.project_id,
            input_payload,
            write_key=write_key,
            metadata={"cg_role": "bootstrap_bundle"},
        )
        return self.lifecycle.dispatch(
            requested_by=requested_by,
            authority=authority,
            spec=SpawnTurnSpec(
                target_agent=target_agent,
                cause=cause,
                bootstrap_bundle_ref=bootstrap_ref,
                spawn_key=dispatch_key,
                turn_kind=turn_kind,
                spawn_entrypoint=spawn_entrypoint,
            ),
            meta=dispatch_meta,
            trace=trace,
        )

    def fetch_context(self, turn: TurnRef, after_turn_seq: int = 0, limit: int = 100) -> TurnContext:
        snapshot = self.lifecycle.get_turn(turn)
        if snapshot is None:
            raise NotFoundError(f"turn not found: {turn.turn_id}")
        page = self.semantic.list_semantic_records(turn, after_turn_seq=after_turn_seq, limit=limit)
        items = []
        for record in page.items:
            content = self.cardbox.hydrate_box(record.cardbox_ref)
            if content is None:
                raise InvariantError(f"dangling cardbox ref: {record.cardbox_ref.cardbox_id}")
            items.append(SemanticContextItem(record=record, content=content))
        return TurnContext(turn=snapshot, semantic_items=tuple(items))

    def record_and_finish(
        self,
        *,
        context: KernelOpContext,
        outcome: TurnOutcome,
        final_payload: Any = UNSET_PAYLOAD,
        record_role: str = "deliverable",
    ) -> None:
        if context.claim is None:
            raise ValueError("record_and_finish requires KernelOpContext.claim")
        ref = None
        if final_payload is not UNSET_PAYLOAD:
            ref = self.cardbox.create_payload_box(
                context.claim.project_id,
                final_payload,
                metadata={"cg_role": record_role},
            )
        self.lifecycle.record_and_finish(
            context.claim,
            outcome=outcome,
            final_record_role=record_role if ref is not None else None,
            final_cardbox_ref=ref,
            meta=context.meta,
            trace=context.trace,
        )

    def fetch_turn_feed(self, turn: TurnRef, after_ledger_seq: int = 0, limit: int = 100) -> LedgerFeedPage:
        return self.ledger.fetch_ledger_feed(
            project_id=turn.project_id,
            after_ledger_seq=after_ledger_seq,
            scope=LedgerFeedScope(
                project_id=turn.project_id,
                scope_kind=LedgerScopeKind.TURN,
                scope_id=turn.turn_id,
            ),
            limit=limit,
        )

    def fetch_agent_feed(
        self,
        agent: AgentRef,
        after_ledger_seq: int = 0,
        limit: int = 100,
    ) -> LedgerFeedPage:
        return self.ledger.fetch_ledger_feed(
            project_id=agent.project_id,
            after_ledger_seq=after_ledger_seq,
            scope=LedgerFeedScope(
                project_id=agent.project_id,
                scope_kind=LedgerScopeKind.AGENT,
                scope_id=agent.agent_id,
            ),
            limit=limit,
        )

    def submit_work_memory_report(
        self,
        *,
        actor: AgentRef,
        manifest: Mapping[str, Any],
        meta: OperationMeta | None = None,
        trace: TraceContext | None = None,
    ) -> WorkMemoryReportSubmissionResult:
        del trace
        normalized = _normalize_work_memory_report_manifest(actor=actor, manifest=manifest)
        request_id = normalized["request_id"]
        actor_snapshot = self.topology.get_agent(actor)
        if actor_snapshot is None:
            raise NotFoundError(f"agent not found: {actor.agent_id}")
        if not actor_snapshot.enabled:
            raise ConflictError(f"agent disabled: {actor.agent_id}")
        base_key = f"work-memory-report:{actor.agent_id}:{request_id}"
        manifest_ref = self.cardbox.create_payload_box(
            actor.project_id,
            normalized,
            write_key=f"{base_key}:manifest",
            metadata={
                "cg_role": WORK_MEMORY_REPORT_MANIFEST_ROLE,
                "submission_kind": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
            },
        )
        record_specs: list[WorkMemoryReportRecordWriteSpec] = []
        for index, record in enumerate(normalized["records"], start=1):
            role = record["role"]
            ref = self.cardbox.create_payload_box(
                actor.project_id,
                {
                    "kind": "work_memory_report_record.v1",
                    "role": role,
                    "payload": record["payload"],
                    "source_refs": record["source_refs"],
                },
                write_key=f"{base_key}:record:{index}",
                metadata={
                    "cg_role": role,
                    "submission_kind": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
                },
            )
            record_specs.append(WorkMemoryReportRecordWriteSpec(record_role=role, cardbox_ref=ref))
        final_payload = normalized.get("final_payload")
        final_ref = None
        if final_payload is not None:
            final_ref = self.cardbox.create_payload_box(
                actor.project_id,
                final_payload,
                write_key=f"{base_key}:final",
                metadata={
                    "cg_role": WORK_MEMORY_REPORT_RESULT_ROLE,
                    "submission_kind": WORK_MEMORY_REPORT_SUBMISSION_KIND_V1,
                },
            )
        result = self.lifecycle.submit_work_memory_report(
            WorkMemoryReportWriteSpec(
                actor=actor,
                request_id=request_id,
                manifest_ref=manifest_ref,
                records=tuple(record_specs),
                final_payload_ref=final_ref,
            ),
            meta=meta,
        )
        return WorkMemoryReportSubmissionResult(
            status=result.status,
            turn=result.turn,
            record_refs=result.record_refs,
            final_payload=final_payload,
        )


def _normalize_dispatch_anchor_or_conflict(value: str | None, *, field_name: str) -> str:
    try:
        return normalize_dispatch_anchor(value, field_name=field_name)
    except ValueError as exc:
        raise ConflictError(str(exc)) from exc


def _normalize_work_memory_report_manifest(*, actor: AgentRef, manifest: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(manifest, Mapping):
        raise ConflictError("work-memory report manifest must be an object")
    allowed_keys = {
        "kind",
        "request_id",
        "summary",
        "records",
        "final_payload",
        "declared_project_id",
        "declared_agent_id",
    }
    forbidden = {
        "claim",
        "claim_token",
        "registration_token",
        "registration_credential",
        "credential",
        "grant",
        "agent_birth_authority",
    }
    extra_keys = set(manifest) - allowed_keys
    if extra_keys:
        key = sorted(extra_keys)[0]
        if key == "meta":
            raise ConflictError("work-memory manifest must not include meta")
        if key in forbidden:
            raise ConflictError(f"work-memory report manifest must not include authority field: {key}")
        raise ConflictError(f"unsupported work-memory report manifest field: {key}")

    kind = manifest.get("kind", WORK_MEMORY_REPORT_MANIFEST_KIND_V1)
    if kind != WORK_MEMORY_REPORT_MANIFEST_KIND_V1:
        raise ConflictError(f"unsupported work-memory report manifest kind: {kind}")
    request_id = manifest.get("request_id")
    if not isinstance(request_id, str) or not request_id.strip():
        raise ConflictError("work-memory report request_id is required")
    request_id = request_id.strip()
    declared_project_id = manifest.get("declared_project_id")
    if declared_project_id is not None and declared_project_id != actor.project_id:
        raise ConflictError("manifest declared_project_id must match trusted actor project")
    declared_agent_id = manifest.get("declared_agent_id")
    if declared_agent_id is not None and declared_agent_id != actor.agent_id:
        raise ConflictError("manifest declared_agent_id must match trusted actor")
    summary = manifest.get("summary")
    if summary is not None and not isinstance(summary, str):
        raise ConflictError("work-memory report summary must be a string when provided")
    raw_records = manifest.get("records")
    if not isinstance(raw_records, list) or not raw_records:
        raise ConflictError("work-memory report records must be a non-empty list")
    records: list[dict[str, Any]] = []
    for index, raw_record in enumerate(raw_records, start=1):
        if not isinstance(raw_record, Mapping):
            raise ConflictError(f"work-memory report record {index} must be an object")
        record_extra = set(raw_record) - {"role", "payload", "source_refs"}
        if record_extra:
            raise ConflictError(f"unsupported work-memory report record field: {sorted(record_extra)[0]}")
        role = raw_record.get("role")
        if not isinstance(role, str) or not role.strip():
            raise ConflictError(f"work-memory report record {index} role is required")
        role = role.strip()
        if role in {WORK_MEMORY_REPORT_MANIFEST_ROLE, WORK_MEMORY_REPORT_RESULT_ROLE}:
            raise ConflictError(f"work-memory report record role is reserved: {role}")
        if "payload" not in raw_record:
            raise ConflictError(f"work-memory report record {index} payload is required")
        source_refs = raw_record.get("source_refs", [])
        if not isinstance(source_refs, list):
            raise ConflictError(f"work-memory report record {index} source_refs must be a list")
        normalized_source_refs: list[dict[str, Any]] = []
        for source_index, source_ref in enumerate(source_refs, start=1):
            if not isinstance(source_ref, Mapping):
                raise ConflictError(
                    f"work-memory report record {index} source_refs item {source_index} must be an object"
                )
            normalized_source_refs.append(dict(source_ref))
        records.append(
            {
                "role": role,
                "payload": raw_record["payload"],
                "source_refs": normalized_source_refs,
            }
        )

    normalized: dict[str, Any] = {
        "kind": WORK_MEMORY_REPORT_MANIFEST_KIND_V1,
        "request_id": request_id,
        "records": records,
    }
    if summary is not None:
        normalized["summary"] = summary
    if "final_payload" in manifest and manifest.get("final_payload") is not None:
        normalized["final_payload"] = manifest["final_payload"]
    if declared_project_id is not None:
        normalized["declared_project_id"] = declared_project_id
    if declared_agent_id is not None:
        normalized["declared_agent_id"] = declared_agent_id
    return normalized
