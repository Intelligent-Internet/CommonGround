from __future__ import annotations

from fastapi import APIRouter, Query, Request

from CommonGround.agent_credentials import AGENT_CREDENTIAL_ISSUE_ANY_GRANT, AGENT_CREDENTIAL_REVOKE_ANY_GRANT
from CommonGround.agent_registration import AgentBirthSpec, AgentRegistrationProvenance
from CommonGround.adapters import ExternalAgentAdapter
from CommonGround.contracts import AgentRef, ConflictError, DispatchAuthority, DispatchAuthorityMode, NotFoundError, TurnOutcome, TurnRef
from CommonGround.payloads import UNSET_PAYLOAD

from .schemas import (
    AppendSemanticRecordRequest,
    ClaimTurnRequest,
    DispatchTurnRequest,
    DrainAgentRequest,
    FinishTurnRequest,
    IssueAgentCredentialRequest,
    ReconcileExpiredRequest,
    RegisterAgentByServiceRequest,
    RenewClaimRequest,
    ResumeAgentRequest,
    ResumeTurnRequest,
    StopTurnRequest,
    SuspendTurnRequest,
    UpdateAgentPresenceRequest,
    UpdateAgentPublicMetadataRequest,
    WorkMemoryReportSubmissionRequest,
)
from .serialization import (
    parse_agent_ref,
    parse_claim_token,
    parse_operation_meta,
    parse_trace_context,
    to_jsonable,
)
from .auth import require_caller_identity
from .read_policy import ReadSurfaceKind, authorize_read
from .write_guard import WriteSurfaceKind


router = APIRouter()


def _deps(request: Request):
    return request.app.state.service_deps


def _write_guard(request: Request):
    return _deps(request).write_guard


def _require_turn(request: Request, *, project_id: str, turn_id: str):
    deps = _deps(request)
    turn = TurnRef(project_id=project_id, turn_id=turn_id)
    snapshot = deps.kernel_app.lifecycle.get_turn(turn)
    if snapshot is None:
        raise NotFoundError(f"turn not found: {turn_id}")
    return turn


def _require_agent_path(agent: AgentRef, *, project_id: str, agent_id: str) -> None:
    if agent.project_id != project_id:
        raise ConflictError("agent project must match path project")
    if agent.agent_id != agent_id:
        raise ConflictError("path agent_id must match request agent")


def _require_agent_exists(request: Request, agent: AgentRef):
    snapshot = _deps(request).kernel_app.topology.get_agent(agent)
    if snapshot is None:
        raise NotFoundError(f"agent not found: {agent.agent_id}")
    return snapshot


def _credential_summary(row) -> dict[str, object]:
    return {
        "credential_id": row.credential_id,
        "project_id": row.project_id,
        "agent_id": row.agent_id,
        "status": row.status,
        "issued_by_agent_id": row.issued_by_agent_id,
        "provenance_kind": row.provenance_kind,
        "provenance_ref": row.provenance_ref,
        "provenance_payload_hash": row.provenance_payload_hash,
        "expires_at": row.expires_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "revoked_at": row.revoked_at,
        "last_used_at": row.last_used_at,
    }


def _caller_has_grant(request: Request, caller: AgentRef, grant: str) -> bool:
    snapshot = _require_agent_exists(request, caller)
    return grant in snapshot.grants


def _require_credential_admin_grant(request: Request, caller: AgentRef, grant: str) -> None:
    if not _caller_has_grant(request, caller, grant):
        raise ConflictError(f"caller missing grant: {grant}")


def _require_caller_project(caller: AgentRef, *, project_id: str) -> None:
    if caller.project_id != project_id:
        raise ConflictError("caller project must match path project")


def _require_turn_ref_path(turn: TurnRef, *, project_id: str, turn_id: str) -> None:
    if turn.project_id != project_id:
        raise ConflictError("turn project must match path project")
    if turn.turn_id != turn_id:
        raise ConflictError("path turn_id must match request turn")


@router.get("/healthz")
def healthz(request: Request):
    deps = _deps(request)
    return {"status": "ok", "service": deps.config.service_name}


@router.get("/readyz")
def readyz(request: Request):
    deps = _deps(request)
    return {
        "status": "ready",
        "service": deps.config.service_name,
        "backend": deps.config.backend_kind,
        "claim_timeout_seconds": deps.config.claim_timeout_seconds,
        "claim_reaper_interval_seconds": deps.config.claim_reaper_interval_seconds,
    }


@router.post("/v3r1/projects/{project_id}/agents:register")
def register_agent_by_service(project_id: str, body: RegisterAgentByServiceRequest, request: Request):
    deps = _deps(request)
    write_request = _write_guard(request).require_registration_service_actor(
        request,
        project_id=project_id,
    )
    assert write_request.actor is not None
    snapshot = deps.management.register_agent_by_service(
        registered_by=write_request.actor,
        spec=AgentBirthSpec(
            agent_id=body.spec.agent_id,
            role=body.spec.role,
            description=body.spec.description,
            enabled=body.spec.enabled,
            accepts_work=body.spec.accepts_work,
            capacity=body.spec.capacity,
            capabilities=body.spec.capabilities,
            grants=body.spec.grants,
            public_metadata=body.spec.public_metadata,
        ),
        provenance=AgentRegistrationProvenance(
            kind=body.provenance.kind,
            external_ref=body.provenance.external_ref,
            payload_hash=body.provenance.payload_hash,
        ),
    )
    return to_jsonable(snapshot)


@router.post("/v3r1/projects/{project_id}/agents/{agent_id}/credentials:issue")
def issue_agent_credential(project_id: str, agent_id: str, body: IssueAgentCredentialRequest, request: Request):
    deps = _deps(request)
    caller = require_caller_identity(request)
    _require_caller_project(caller.agent_ref(), project_id=project_id)
    target = AgentRef(project_id=project_id, agent_id=agent_id)
    _require_agent_exists(request, target)
    _require_credential_admin_grant(request, caller.agent_ref(), AGENT_CREDENTIAL_ISSUE_ANY_GRANT)
    issued = deps.credential_store.issue_agent_credential(
        target,
        issued_by_agent_id=caller.agent_id,
        provenance_kind=body.provenance_kind or "agent_credential_lifecycle_route",
        provenance_ref=body.provenance_ref,
        provenance_payload_hash=body.provenance_payload_hash,
        expires_at=body.expires_at,
    )
    row = deps.credential_store.load_agent_credential_by_id(issued.ref.credential_id)
    assert row is not None
    return to_jsonable({"credential": _credential_summary(row), "token": issued.token})


@router.post("/v3r1/projects/{project_id}/agents/{agent_id}/credentials/{credential_id}:revoke")
def revoke_agent_credential(project_id: str, agent_id: str, credential_id: str, request: Request):
    deps = _deps(request)
    caller = require_caller_identity(request)
    _require_caller_project(caller.agent_ref(), project_id=project_id)
    target = AgentRef(project_id=project_id, agent_id=agent_id)
    _require_agent_exists(request, target)
    row = deps.credential_store.load_agent_credential_by_id(credential_id)
    if row is None:
        raise NotFoundError(f"agent credential not found: {credential_id}")
    if row.project_id != project_id or row.agent_id != agent_id:
        raise ConflictError("credential does not belong to path agent")
    if caller.agent_id != agent_id:
        _require_credential_admin_grant(request, caller.agent_ref(), AGENT_CREDENTIAL_REVOKE_ANY_GRANT)
    revoked = deps.credential_store.revoke_agent_credential(credential_id)
    return to_jsonable({"credential": _credential_summary(revoked)})


@router.get("/v3r1/projects/{project_id}/agents/{agent_id}/credentials")
def list_agent_credentials(project_id: str, agent_id: str, request: Request):
    deps = _deps(request)
    caller = require_caller_identity(request)
    _require_caller_project(caller.agent_ref(), project_id=project_id)
    target = AgentRef(project_id=project_id, agent_id=agent_id)
    _require_agent_exists(request, target)
    if caller.agent_id != agent_id and not (
        _caller_has_grant(request, caller.agent_ref(), AGENT_CREDENTIAL_ISSUE_ANY_GRANT)
        or _caller_has_grant(request, caller.agent_ref(), AGENT_CREDENTIAL_REVOKE_ANY_GRANT)
    ):
        raise ConflictError(f"caller missing grant: {AGENT_CREDENTIAL_ISSUE_ANY_GRANT} or {AGENT_CREDENTIAL_REVOKE_ANY_GRANT}")
    rows = deps.credential_store.list_agent_credentials(target)
    return to_jsonable({"credentials": [_credential_summary(row) for row in rows]})


@router.get("/v3r1/projects/{project_id}/agents/{agent_id}")
def get_agent(project_id: str, agent_id: str, request: Request):
    deps = _deps(request)
    agent = AgentRef(project_id=project_id, agent_id=agent_id)
    authorize_read(
        request,
        project_id=project_id,
        surface_kind=ReadSurfaceKind.TRUTH_SNAPSHOT,
        resource_family="agent_snapshot",
        resource_id=agent_id,
    )
    snapshot = deps.kernel_app.topology.get_agent(agent)
    if snapshot is None:
        raise NotFoundError(f"agent not found: {agent_id}")
    return to_jsonable(snapshot)


@router.post("/v3r1/projects/{project_id}/agents/{agent_id}/work-memory-reports")
def submit_work_memory_report(project_id: str, agent_id: str, body: WorkMemoryReportSubmissionRequest, request: Request):
    deps = _deps(request)
    actor = AgentRef(project_id=project_id, agent_id=agent_id)
    _write_guard(request).require_agent_actor(
        request,
        project_id=project_id,
        agent=actor,
        path_agent_id=agent_id,
        surface_kind=WriteSurfaceKind.WORK_MEMORY_REPORT_SUBMISSION,
        resource_family="work_memory_report",
        operation="submit",
    )
    manifest = body.model_dump(mode="json", exclude_none=True)
    result = deps.kernel_app.sdk.submit_work_memory_report(
        actor=actor,
        manifest=manifest,
        meta=None,
    )
    return to_jsonable(result)


@router.post("/v3r1/projects/{project_id}/management/agents/{agent_id}:drain")
def drain_agent(project_id: str, agent_id: str, body: DrainAgentRequest, request: Request):
    deps = _deps(request)
    agent = parse_agent_ref(body.agent.model_dump())
    requested_by = parse_agent_ref(body.requested_by.model_dump())
    _require_agent_path(agent, project_id=project_id, agent_id=agent_id)
    _write_guard(request).require_requested_by(
        request,
        project_id=project_id,
        requested_by=requested_by,
        surface_kind=WriteSurfaceKind.AGENT_OPERATIONAL_STATE,
        resource_family="agent_operational_state",
        operation="drain",
        resource_id=agent_id,
    )
    deps.management.drain_agent(
        agent=agent,
        requested_by=requested_by,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return {"ok": True}


@router.post("/v3r1/projects/{project_id}/management/agents/{agent_id}:resume")
def resume_agent(project_id: str, agent_id: str, body: ResumeAgentRequest, request: Request):
    deps = _deps(request)
    agent = parse_agent_ref(body.agent.model_dump())
    requested_by = parse_agent_ref(body.requested_by.model_dump())
    _require_agent_path(agent, project_id=project_id, agent_id=agent_id)
    _write_guard(request).require_requested_by(
        request,
        project_id=project_id,
        requested_by=requested_by,
        surface_kind=WriteSurfaceKind.AGENT_OPERATIONAL_STATE,
        resource_family="agent_operational_state",
        operation="resume",
        resource_id=agent_id,
    )
    deps.management.resume_agent(
        agent=agent,
        requested_by=requested_by,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return {"ok": True}


@router.post("/v3r1/projects/{project_id}/management/agents/{agent_id}:heartbeat-presence")
def update_agent_presence(project_id: str, agent_id: str, body: UpdateAgentPresenceRequest, request: Request):
    deps = _deps(request)
    agent = parse_agent_ref(body.agent.model_dump())
    _write_guard(request).require_agent_actor(
        request,
        project_id=project_id,
        agent=agent,
        path_agent_id=agent_id,
        surface_kind=WriteSurfaceKind.AGENT_PRESENCE,
        resource_family="agent_presence",
        operation="heartbeat_presence",
    )
    deps.management.update_agent_presence(
        agent=agent,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return {"ok": True}


@router.put("/v3r1/projects/{project_id}/management/agents/{agent_id}/public-metadata")
def update_agent_public_metadata(project_id: str, agent_id: str, body: UpdateAgentPublicMetadataRequest, request: Request):
    deps = _deps(request)
    agent = parse_agent_ref(body.agent.model_dump())
    _write_guard(request).require_agent_actor(
        request,
        project_id=project_id,
        agent=agent,
        path_agent_id=agent_id,
        surface_kind=WriteSurfaceKind.AGENT_PUBLIC_METADATA,
        resource_family="agent_public_metadata",
        operation="put",
    )
    deps.management.update_agent_public_metadata(
        agent=agent,
        public_metadata=body.public_metadata,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return {"ok": True}


@router.post("/v3r1/projects/{project_id}/management/turns/{turn_id}:stop")
def request_stop(project_id: str, turn_id: str, body: StopTurnRequest, request: Request):
    deps = _deps(request)
    requested_by = parse_agent_ref(body.requested_by.model_dump())
    turn = TurnRef(project_id=body.turn.project_id, turn_id=body.turn.turn_id)
    _require_turn_ref_path(turn, project_id=project_id, turn_id=turn_id)
    _write_guard(request).require_requested_by(
        request,
        project_id=project_id,
        requested_by=requested_by,
        surface_kind=WriteSurfaceKind.TURN_STOP_REQUEST,
        resource_family="turn_stop",
        operation="stop",
        resource_id=turn_id,
    )
    turn = deps.management.request_stop(
        project_id=project_id,
        turn_id=turn_id,
        requested_by=requested_by,
        reason=body.meta.reason if body.meta is not None else "operator_stop",
        note=body.meta.note if body.meta is not None else None,
        annotations={} if body.meta is None else dict(body.meta.annotations),
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return to_jsonable(turn)


@router.post("/v3r1/projects/{project_id}/turns:dispatch")
def dispatch_turn(project_id: str, body: DispatchTurnRequest, request: Request):
    deps = _deps(request)
    target_agent = parse_agent_ref(body.target_agent.model_dump())
    requested_by = parse_agent_ref(body.requested_by.model_dump())
    _write_guard(request).require_requested_by(
        request,
        project_id=project_id,
        requested_by=requested_by,
        surface_kind=WriteSurfaceKind.TURN_BIRTH,
        resource_family="turn",
        operation="dispatch",
    )
    if target_agent.project_id != project_id:
        raise ConflictError("target_agent project must match path project")
    authority_model = body.authority
    if authority_model.mode == DispatchAuthorityMode.ROOT_REQUEST:
        authority = DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id=authority_model.request_id)
        turn = deps.dispatch_ingress.dispatch(
            requested_by=requested_by,
            target_agent=target_agent,
            request_id=authority.request_id,
            input_payload=body.input,
            turn_kind=body.turn_kind,
            dispatch_key=body.dispatch_key,
            meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
            trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
        )
        return to_jsonable(turn)
    authority = DispatchAuthority(
        mode=DispatchAuthorityMode.CHILD_DERIVATION,
        parent_claim=parse_claim_token(authority_model.parent_claim.model_dump()),
    )
    if authority.parent_claim.turn_id != authority_model.parent_claim.turn_id:
        raise ConflictError("authority parent claim must match request body")
    if authority.parent_claim.project_id != project_id:
        raise ConflictError("authority parent claim project must match path project")
    _require_turn(request, project_id=authority.parent_claim.project_id, turn_id=authority.parent_claim.turn_id)
    adapter = ExternalAgentAdapter(agent=requested_by, sdk=deps.kernel_app.sdk)
    turn = adapter.dispatch(
        authority.parent_claim,
        target_agent=target_agent,
        input_payload=body.input,
        turn_kind=body.turn_kind,
        dispatch_key=body.dispatch_key,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return to_jsonable(turn)


@router.post("/v3r1/projects/{project_id}/agents/{agent_id}/claims:claim")
def claim_turn(project_id: str, agent_id: str, body: ClaimTurnRequest, request: Request):
    deps = _deps(request)
    agent = parse_agent_ref(body.agent.model_dump())
    _write_guard(request).require_agent_actor(
        request,
        project_id=project_id,
        agent=agent,
        path_agent_id=agent_id,
        surface_kind=WriteSurfaceKind.CLAIM_ACQUIRE,
        resource_family="claim",
        operation="claim",
    )
    claim = deps.kernel_app.lifecycle.claim_turn(
        agent,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return None if claim is None else to_jsonable(claim)


@router.post("/v3r1/projects/{project_id}/claims:renew")
def renew_claim(project_id: str, body: RenewClaimRequest, request: Request):
    deps = _deps(request)
    claim = parse_claim_token(body.claim.model_dump())
    _write_guard(request).require_claim_owner(
        request,
        project_id=project_id,
        claim=claim,
        surface_kind=WriteSurfaceKind.CLAIM_RENEWAL,
        resource_family="claim",
        operation="renew",
    )
    renewed = deps.kernel_app.lifecycle.renew_claim(
        claim,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return to_jsonable(renewed)


@router.post("/v3r1/projects/{project_id}/claims:reconcile-expired")
def reconcile_expired(project_id: str, body: ReconcileExpiredRequest, request: Request):
    deps = _deps(request)
    if body.agent is None:
        write_request = _write_guard(request).require_caller_default_agent(
            request,
            project_id=project_id,
            surface_kind=WriteSurfaceKind.CLAIM_RECONCILE,
            resource_family="claim",
            operation="reconcile_expired",
        )
        assert write_request.actor is not None
        target_agent = write_request.actor
    else:
        target_agent = parse_agent_ref(body.agent.model_dump())
        _write_guard(request).require_agent_actor(
            request,
            project_id=project_id,
            agent=target_agent,
            surface_kind=WriteSurfaceKind.CLAIM_RECONCILE,
            resource_family="claim",
            operation="reconcile_expired",
        )
    summary = deps.kernel_app.lifecycle.reconcile_expired_claim(
        target_agent,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return to_jsonable(summary)


@router.get("/v3r1/projects/{project_id}/turns/{turn_id}")
def get_turn(project_id: str, turn_id: str, request: Request):
    deps = _deps(request)
    authorize_read(
        request,
        project_id=project_id,
        surface_kind=ReadSurfaceKind.TRUTH_SNAPSHOT,
        resource_family="turn_snapshot",
        resource_id=turn_id,
    )
    turn = _require_turn(request, project_id=project_id, turn_id=turn_id)
    snapshot = deps.kernel_app.lifecycle.get_turn(turn)
    assert snapshot is not None
    return to_jsonable(snapshot)


@router.get("/v3r1/projects/{project_id}/turns/{turn_id}/context")
def fetch_context(project_id: str, turn_id: str, request: Request, after_turn_seq: int = Query(0), limit: int = Query(100)):
    deps = _deps(request)
    authorize_read(
        request,
        project_id=project_id,
        surface_kind=ReadSurfaceKind.TURN_INSPECT,
        resource_family="turn_context",
        resource_id=turn_id,
    )
    turn = _require_turn(request, project_id=project_id, turn_id=turn_id)
    context = deps.kernel_app.sdk.fetch_context(turn, after_turn_seq=after_turn_seq, limit=limit)
    return to_jsonable(context)


@router.get("/v3r1/projects/{project_id}/turns/{turn_id}/feed")
def fetch_turn_feed(project_id: str, turn_id: str, request: Request, after_ledger_seq: int = Query(0), limit: int = Query(100)):
    deps = _deps(request)
    authorize_read(
        request,
        project_id=project_id,
        surface_kind=ReadSurfaceKind.TURN_INSPECT,
        resource_family="turn_feed",
        resource_id=turn_id,
    )
    turn = _require_turn(request, project_id=project_id, turn_id=turn_id)
    page = deps.kernel_app.sdk.fetch_turn_feed(turn, after_ledger_seq=after_ledger_seq, limit=limit)
    return to_jsonable(page)


@router.get("/v3r1/projects/{project_id}/agents/{agent_id}/feed")
def fetch_agent_feed(project_id: str, agent_id: str, request: Request, after_ledger_seq: int = Query(0), limit: int = Query(100)):
    deps = _deps(request)
    agent = AgentRef(project_id=project_id, agent_id=agent_id)
    authorize_read(
        request,
        project_id=project_id,
        surface_kind=ReadSurfaceKind.PROJECTION,
        resource_family="agent_feed",
        resource_id=agent_id,
    )
    page = deps.kernel_app.sdk.fetch_agent_feed(
        agent,
        after_ledger_seq=after_ledger_seq,
        limit=limit,
    )
    return to_jsonable(page)


@router.post("/v3r1/projects/{project_id}/turns/{turn_id}/semantic-records")
def append_record(project_id: str, turn_id: str, body: AppendSemanticRecordRequest, request: Request):
    deps = _deps(request)
    claim = parse_claim_token(body.claim.model_dump())
    _write_guard(request).require_claim_owner(
        request,
        project_id=project_id,
        claim=claim,
        path_turn_id=turn_id,
        surface_kind=WriteSurfaceKind.CLAIM_FENCED_TURN_MUTATION,
        resource_family="semantic_record",
        operation="append",
    )
    _require_turn(request, project_id=project_id, turn_id=turn_id)
    adapter = ExternalAgentAdapter(agent=AgentRef(claim.project_id, claim.agent_id), sdk=deps.kernel_app.sdk)
    ref = adapter.append_record(
        claim,
        body.payload,
        role=body.role,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return to_jsonable(ref)


@router.post("/v3r1/projects/{project_id}/turns/{turn_id}:suspend")
def suspend_turn(project_id: str, turn_id: str, body: SuspendTurnRequest, request: Request):
    deps = _deps(request)
    claim = parse_claim_token(body.claim.model_dump())
    _write_guard(request).require_claim_owner(
        request,
        project_id=project_id,
        claim=claim,
        path_turn_id=turn_id,
        surface_kind=WriteSurfaceKind.CLAIM_FENCED_TURN_MUTATION,
        resource_family="turn_lifecycle",
        operation="suspend",
    )
    _require_turn(request, project_id=project_id, turn_id=turn_id)
    adapter = ExternalAgentAdapter(agent=AgentRef(claim.project_id, claim.agent_id), sdk=deps.kernel_app.sdk)
    adapter.suspend_current(
        claim,
        reason=body.reason,
        note=body.note,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return {"ok": True}


@router.post("/v3r1/projects/{project_id}/turns/{turn_id}:resume")
def resume_turn(project_id: str, turn_id: str, body: ResumeTurnRequest, request: Request):
    deps = _deps(request)
    requested_by = parse_agent_ref(body.requested_by.model_dump())
    _write_guard(request).require_requested_by(
        request,
        project_id=project_id,
        requested_by=requested_by,
        surface_kind=WriteSurfaceKind.TURN_RESUME,
        resource_family="turn_lifecycle",
        operation="resume",
        resource_id=turn_id,
    )
    _require_turn(request, project_id=project_id, turn_id=turn_id)
    adapter = ExternalAgentAdapter(agent=requested_by, sdk=deps.kernel_app.sdk)
    adapter.resume_turn(
        TurnRef(project_id=project_id, turn_id=turn_id),
        note=body.note,
        meta=parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        trace=parse_trace_context(body.trace.model_dump()) if body.trace else None,
    )
    return {"ok": True}


@router.post("/v3r1/projects/{project_id}/turns/{turn_id}:finish")
def finish_turn(project_id: str, turn_id: str, body: FinishTurnRequest, request: Request):
    deps = _deps(request)
    claim = parse_claim_token(body.claim.model_dump())
    _write_guard(request).require_claim_owner(
        request,
        project_id=project_id,
        claim=claim,
        path_turn_id=turn_id,
        surface_kind=WriteSurfaceKind.CLAIM_FENCED_TURN_MUTATION,
        resource_family="turn_lifecycle",
        operation="finish",
    )
    _require_turn(request, project_id=project_id, turn_id=turn_id)
    adapter = ExternalAgentAdapter(agent=AgentRef(claim.project_id, claim.agent_id), sdk=deps.kernel_app.sdk)
    finish_kwargs = {
        "outcome": TurnOutcome(body.outcome),
        "final_record_role": body.final_record_role,
        "meta": parse_operation_meta(body.meta.model_dump()) if body.meta else None,
        "trace": parse_trace_context(body.trace.model_dump()) if body.trace else None,
    }
    finish_kwargs["final_payload"] = body.final_payload if "final_payload" in body.model_fields_set else UNSET_PAYLOAD
    adapter.finish_current(claim, **finish_kwargs)
    return {"ok": True}
