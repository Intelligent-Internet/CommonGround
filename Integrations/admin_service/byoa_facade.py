from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from CommonGround.agent_registration import AgentBirthSpec, agent_birth_spec_hash, canonical_agent_birth_spec
from CommonGround.contracts import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_WORK_MEMORY_REPORT_V1,
)

from .byoa_registration import ByoaRegistrationProcessor
from .byoa_primitives import (
    BYOA_INVITE_CODE_APPROVAL_MODE,
    BYOA_PROFILE_CONVERSATION_WORKER_V1,
    BYOA_PROFILE_KINDS,
    BYOA_PROFILE_WORK_MEMORY_REPORTER_V1,
    ByoaInviteApproval,
)
from .byoa_workflow import (
    BYOA_STATUS_REGISTERED,
    BYOA_STATUS_SUBMITTED,
    DEFAULT_BYOA_PROVENANCE_KIND,
    ByoaRegistrationRequest,
    ByoaWorkflowStore,
    canonical_json_sha256,
)
from .project_bootstrap import ADMIN_SERVICE_AGENT_ID
from .project_setup import admin_service_credential_token_ready, project_status


BYOA_MVP_POLICY_VERSION = "mvp.v1"
BYOA_APPROVAL_RECORD_KIND = "admin_service.byoa_registration.approval.v1"
BYOA_EXTERNAL_AGENT_ROLE = "external.agent.v1"
BYOA_EXTERNAL_CONVERSATION_WORKER_ROLE = "external.conversation_worker.v1"
BYOA_INVITE_ISSUER_ROLE_PROJECT_OWNER = "project_owner"
BYOA_ALLOWED_REQUESTED_ROLES = frozenset((BYOA_EXTERNAL_AGENT_ROLE,))
BYOA_ALLOWED_REQUESTED_CAPABILITIES = frozenset((TURN_KIND_WORK_MEMORY_REPORT_V1,))
BYOA_DIRECT_CG_PROFILE_STATUS_CREDENTIAL_READY = "credential_ready"

_REQUEST_FIELDS = frozenset(
    (
        "request_id",
        "project_id",
        "requested_agent_id",
        "display_name",
        "description",
        "requested_role",
        "requested_capabilities",
        "runtime_kind",
        "profile_kind",
        "invitation_code",
        "invitation",
    )
)
_REQUIRED_REQUEST_FIELDS = frozenset(
    (
        "request_id",
        "project_id",
        "requested_agent_id",
        "display_name",
        "description",
        "requested_role",
        "requested_capabilities",
        "runtime_kind",
    )
)

AuthorizeByoaRequest = Callable[[str, str], bool]
ValidateByoaInvitation = Callable[["ByoaProductRegistrationRequest", str], "ByoaInviteApproval"]


@dataclass(frozen=True, slots=True)
class ByoaInviteConfigEntry:
    invite_id: str
    project_id: str
    issued_by_user_id: str
    issuer_role: str
    allowed_profile_kinds: tuple[str, ...]
    code_sha256: str
    enabled: bool = True
    expires_at: datetime | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ByoaInviteConfigEntry":
        invite_id = _required_string("invite_id", value.get("invite_id") or value.get("id"))
        project_id = _required_string("project_id", value.get("project_id"))
        issued_by_user_id = _required_string("issued_by_user_id", value.get("issued_by_user_id"))
        issuer_role = _required_string("issuer_role", value.get("issuer_role"))
        if issuer_role != BYOA_INVITE_ISSUER_ROLE_PROJECT_OWNER:
            raise ConflictError(f"unsupported BYOA invitation issuer_role: {issuer_role}")
        allowed_profile_kinds = _allowed_profile_kinds(value.get("allowed_profile_kinds"))
        code_sha256 = value.get("code_sha256")
        if code_sha256 is None and value.get("code") is not None:
            code_sha256 = _sha256_string(_required_string("code", value.get("code")))
        code_sha256 = _normalize_code_sha256(_required_string("code_sha256", code_sha256))
        enabled = value.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ConflictError("enabled must be a boolean")
        expires_at = _optional_datetime("expires_at", value.get("expires_at"))
        return cls(
            invite_id=invite_id,
            project_id=project_id,
            issued_by_user_id=issued_by_user_id,
            issuer_role=issuer_role,
            allowed_profile_kinds=allowed_profile_kinds,
            code_sha256=code_sha256,
            enabled=enabled,
            expires_at=expires_at,
        )


class ByoaJsonInviteValidator:
    """Validate BYOA invite codes from startup JSON config without use-count state."""

    def __init__(
        self,
        entries: Iterable[ByoaInviteConfigEntry | Mapping[str, Any]],
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        parsed_entries: list[ByoaInviteConfigEntry] = []
        for entry in entries:
            if isinstance(entry, ByoaInviteConfigEntry):
                parsed_entries.append(entry)
            elif isinstance(entry, Mapping):
                parsed_entries.append(ByoaInviteConfigEntry.from_mapping(entry))
            else:
                raise ConflictError("BYOA invite config entries must be objects")
        self._entries = tuple(parsed_entries)
        self._now = now or (lambda: datetime.now(UTC))

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any] | Iterable[Mapping[str, Any]],
        *,
        now: Callable[[], datetime] | None = None,
    ) -> "ByoaJsonInviteValidator":
        if isinstance(config, Mapping):
            entries = config.get("invitations")
            if entries is None:
                entries = config.get("invites", ())
        else:
            entries = config
        if isinstance(entries, Mapping) or not isinstance(entries, Iterable):
            raise ConflictError("BYOA invite config invitations must be a list")
        return cls(entries, now=now)

    @classmethod
    def from_json_file(
        cls,
        path: str | Path,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> "ByoaJsonInviteValidator":
        with Path(path).open("r", encoding="utf-8") as stream:
            return cls.from_config(json.load(stream), now=now)

    def __call__(self, request: "ByoaProductRegistrationRequest", invitation_code: str) -> ByoaInviteApproval:
        invitation_code = _required_string("invitation_code", invitation_code)
        code_sha256 = _sha256_string(invitation_code)
        now = _ensure_aware_utc(self._now())
        for entry in self._entries:
            if entry.code_sha256 != code_sha256:
                continue
            if not entry.enabled:
                raise ForbiddenError("BYOA invitation code is disabled")
            if entry.project_id != request.project_id:
                raise ForbiddenError("BYOA invitation code is not valid for project")
            if entry.issuer_role != BYOA_INVITE_ISSUER_ROLE_PROJECT_OWNER:
                raise ForbiddenError("BYOA invitation issuer is not a project owner")
            if request.profile_kind not in entry.allowed_profile_kinds:
                raise ForbiddenError("BYOA invitation code is not valid for profile_kind")
            if entry.expires_at is not None and _ensure_aware_utc(entry.expires_at) <= now:
                raise ForbiddenError("BYOA invitation code is expired")
            return ByoaInviteApproval(
                invite_id=entry.invite_id,
                issued_by_user_id=entry.issued_by_user_id,
            )
        raise ForbiddenError("BYOA invitation code is invalid")


@dataclass(frozen=True, slots=True)
class ByoaProductRegistrationRequest:
    request_id: str
    project_id: str
    requested_agent_id: str
    display_name: str
    description: str | None
    requested_role: str
    requested_capabilities: tuple[str, ...]
    runtime_kind: str
    profile_kind: str = BYOA_PROFILE_WORK_MEMORY_REPORTER_V1
    invitation_code: str | None = field(default=None, repr=False, compare=False)
    invitation: ByoaInviteApproval | None = None

    def to_raw_request(self) -> dict[str, Any]:
        raw = {
            "request_id": self.request_id,
            "project_id": self.project_id,
            "requested_agent_id": self.requested_agent_id,
            "display_name": self.display_name,
            "description": self.description,
            "requested_role": self.requested_role,
            "requested_capabilities": list(self.requested_capabilities),
            "runtime_kind": self.runtime_kind,
        }
        if self.profile_kind != BYOA_PROFILE_WORK_MEMORY_REPORTER_V1:
            raw["profile_kind"] = self.profile_kind
        if self.invitation is not None:
            raw["invitation"] = self.invitation.to_raw_request()
        return raw


@dataclass(frozen=True, slots=True)
class ByoaPolicyMapping:
    request: ByoaProductRegistrationRequest
    policy_version: str
    approved_by: str
    agent_birth_spec: AgentBirthSpec
    admitted_spec_hash: str
    approval_record: Mapping[str, Any]
    provenance_kind: str
    provenance_external_ref: str
    provenance_payload_hash: str


@dataclass(frozen=True, slots=True)
class AdminServiceAgentConnectionProfile:
    project_id: str
    agent_id: str
    runtime_kind: str
    profile_kind: str
    profile_ref: str
    credential_id: str
    status: str = BYOA_DIRECT_CG_PROFILE_STATUS_CREDENTIAL_READY


class AdminServiceAgentCredentialSecret:
    """One-time BYOA credential material; callers must explicitly reveal it."""

    __slots__ = ("credential_id", "_token")

    def __init__(self, *, credential_id: str, token: str) -> None:
        self.credential_id = credential_id
        self._token = token

    def reveal_token(self) -> str:
        return self._token

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(credential_id={self.credential_id!r}, token=<redacted>)"


@dataclass(frozen=True, slots=True)
class AdminServiceByoaApprovalResult:
    request_id: str
    project_id: str
    agent_id: str
    status: str
    workflow_row: ByoaRegistrationRequest
    profile: AdminServiceAgentConnectionProfile
    credential_secret: AdminServiceAgentCredentialSecret = field(repr=False)
    policy_version: str
    provenance_kind: str
    provenance_external_ref: str
    provenance_payload_hash: str


class AdminServiceByoaFacade:
    """Reference product-layer BYOA registration facade."""

    def __init__(
        self,
        pg_dsn: str | ByoaWorkflowStore,
        registration_processor: ByoaRegistrationProcessor | None = None,
        *,
        client: Any | None = None,
        base_url: str = "http://127.0.0.1:8000",
        admin_service_token: str | None = None,
        authorize_request: AuthorizeByoaRequest | None = None,
        validate_invitation: ValidateByoaInvitation | None = None,
    ) -> None:
        if authorize_request is None:
            raise ValueError("AdminServiceByoaFacade requires an explicit authorize_request callback")
        self._authorize_request = authorize_request
        self._validate_invitation = validate_invitation

        if isinstance(pg_dsn, str):
            if registration_processor is not None:
                raise ValueError("pass either pg_dsn/client or prebuilt facade components, not both")
            self._pg_dsn = pg_dsn
            self._workflow_store = ByoaWorkflowStore(pg_dsn)
            self._registration_processor = _registration_processor_for_client(
                self._workflow_store,
                client=client,
                base_url=base_url,
                admin_service_token=admin_service_token,
            )
        else:
            self._pg_dsn = getattr(pg_dsn, "_pg_dsn", None)
            if registration_processor is None:
                raise ValueError("prebuilt facade components require workflow and processor")
            self._workflow_store = pg_dsn
            self._registration_processor = registration_processor

    def authorize_project_request(self, requester_user_id: str, project_id: str) -> None:
        requester_user_id = _required_string("requester_user_id", requester_user_id)
        project_id = _required_string("project_id", project_id)
        if not self._authorize_request(requester_user_id, project_id):
            raise ForbiddenError("BYOA registration requester is not authorized for project")

    def ensure_project_ready_for_registration(self, project_id: str) -> None:
        self._ensure_project_ready_for_registration(_required_string("project_id", project_id))

    def submit_registration_request(
        self,
        request: ByoaProductRegistrationRequest | Mapping[str, Any],
        *,
        requester_user_id: str,
        creator_user_id: str | None = None,
        actor_id: str | None = None,
    ) -> ByoaRegistrationRequest:
        product_request = parse_byoa_product_registration_request(request)
        requester_user_id = _required_string("requester_user_id", requester_user_id)
        if creator_user_id is not None:
            creator_user_id = _required_string("creator_user_id", creator_user_id)
        if actor_id is not None:
            actor_id = _required_string("actor_id", actor_id)
        self.authorize_project_request(requester_user_id, product_request.project_id)

        product_request = self._attach_invitation_approval(product_request)
        self._ensure_project_ready_for_registration(product_request.project_id)

        row = self._workflow_store.submit_request(
            product_request.request_id,
            product_request.project_id,
            product_request.requested_agent_id,
            requester_user_id,
            product_request.to_raw_request(),
            creator_user_id=creator_user_id,
            actor_kind="user",
            actor_id=actor_id or requester_user_id,
        )
        if row.status == BYOA_STATUS_SUBMITTED:
            return self._workflow_store.validate_request(
                row.request_id,
                actor_kind="admin_service",
                actor_id=ADMIN_SERVICE_AGENT_ID,
                details={
                    "validator": "admin_service_byoa_facade",
                    "policy_version": BYOA_MVP_POLICY_VERSION,
                },
            )
        return row

    def approve_registration_request(
        self,
        request_id: str,
        *,
        approved_by: str,
        policy_version: str = BYOA_MVP_POLICY_VERSION,
    ) -> AdminServiceByoaApprovalResult:
        request_id = _required_string("request_id", request_id)
        approved_by = _required_string("approved_by", approved_by)
        policy_version = _required_string("policy_version", policy_version)
        row = self._workflow_store.get_request(request_id)
        if row is None:
            raise NotFoundError(f"BYOA registration request not found: {request_id}")

        product_request = parse_byoa_product_registration_request(row.raw_request)
        _ensure_row_matches_request(row, product_request)
        mapping = map_byoa_mvp_policy(
            product_request,
            approved_by=approved_by,
            policy_version=policy_version,
        )
        self._ensure_project_ready_for_registration(product_request.project_id)
        if row.status == BYOA_STATUS_SUBMITTED:
            row = self._workflow_store.validate_request(
                row.request_id,
                actor_kind="admin_service",
                actor_id=ADMIN_SERVICE_AGENT_ID,
                details={
                    "validator": "admin_service_byoa_facade",
                    "policy_version": mapping.policy_version,
                },
            )
        if row.status == BYOA_STATUS_REGISTERED:
            processed = row
        else:
            self._workflow_store.approve_request(
                request_id,
                mapping.agent_birth_spec,
                actor_kind="user",
                actor_id=mapping.approved_by,
                approved_by=mapping.approved_by,
                provenance_kind=mapping.provenance_kind,
                provenance_external_ref=mapping.provenance_external_ref,
                provenance_payload_hash=mapping.provenance_payload_hash,
                details={
                    "policy_version": mapping.policy_version,
                    "approval_record": mapping.approval_record,
                    "approval_record_hash": mapping.provenance_payload_hash,
                },
            )
            processed = self._registration_processor.process_registration(request_id)
        if processed.status != BYOA_STATUS_REGISTERED:
            raise ConflictError(f"BYOA registration did not complete: {processed.status}")
        credential_issue = self._registration_processor.issue_agent_credential_for_registration(
            processed,
            provenance_kind="admin_service.byoa_agent_credential.v1",
            provenance_ref=processed.request_id,
            provenance_payload_hash=processed.provenance_payload_hash,
        )
        return AdminServiceByoaApprovalResult(
            request_id=processed.request_id,
            project_id=processed.project_id,
            agent_id=processed.registered_agent_id or processed.requested_agent_id,
            status=processed.status,
            workflow_row=processed,
            profile=_connection_profile_for_registration(processed, product_request, credential_issue),
            credential_secret=_credential_secret_from_issue(credential_issue),
            policy_version=mapping.policy_version,
            provenance_kind=mapping.provenance_kind,
            provenance_external_ref=mapping.provenance_external_ref,
            provenance_payload_hash=mapping.provenance_payload_hash,
        )

    def redeem_agent_join_invite(
        self,
        *,
        request_id: str,
        project_id: str,
        agent_id: str,
        profile_kind: str,
        runtime_kind: str,
        display_name: str,
        description: str | None,
        invite_id: str,
        issued_by_user_id: str,
    ) -> AdminServiceByoaApprovalResult:
        request_id = _required_string("request_id", request_id)
        project_id = _required_string("project_id", project_id)
        agent_id = _required_string("agent_id", agent_id)
        invite_id = _required_string("invite_id", invite_id)
        issued_by_user_id = _required_string("issued_by_user_id", issued_by_user_id)
        product_request = ByoaProductRegistrationRequest(
            request_id=request_id,
            project_id=project_id,
            requested_agent_id=agent_id,
            display_name=display_name,
            description=description,
            requested_role=BYOA_EXTERNAL_CONVERSATION_WORKER_ROLE,
            requested_capabilities=(TURN_KIND_CONVERSATION_V1,),
            runtime_kind=runtime_kind,
            profile_kind=profile_kind,
            invitation=ByoaInviteApproval(
                invite_id=invite_id,
                issued_by_user_id=issued_by_user_id,
                approval_mode=BYOA_INVITE_CODE_APPROVAL_MODE,
            ),
        )
        product_request = parse_byoa_product_registration_request(product_request)
        if product_request.profile_kind != BYOA_PROFILE_CONVERSATION_WORKER_V1:
            raise ConflictError("Agent join invite currently supports conversation worker profiles only")
        self._ensure_project_ready_for_registration(product_request.project_id)
        row = self._workflow_store.submit_request(
            product_request.request_id,
            product_request.project_id,
            product_request.requested_agent_id,
            requester_user_id=f"agent-join:{invite_id}",
            raw_request=product_request.to_raw_request(),
            creator_user_id=issued_by_user_id,
            actor_kind="agent_join",
            actor_id=invite_id,
        )
        if row.status == BYOA_STATUS_SUBMITTED:
            self._workflow_store.validate_request(
                row.request_id,
                actor_kind="admin_service",
                actor_id=ADMIN_SERVICE_AGENT_ID,
                details={
                    "validator": "admin_service_agent_join_invite",
                    "policy_version": BYOA_MVP_POLICY_VERSION,
                    "invite_id": invite_id,
                },
            )
        return self.approve_registration_request(row.request_id, approved_by=issued_by_user_id)

    def _ensure_project_ready_for_registration(self, project_id: str) -> None:
        if not self._pg_dsn:
            return
        status = project_status(pg_dsn=self._pg_dsn, project_id=project_id)
        if not status.seeded:
            raise NotFoundError(f"project is not seeded: {project_id}")
        if not status.matches_bootstrap_spec:
            raise ConflictError(
                "project admin-service bootstrap conflict: " + ", ".join(status.mismatch_fields)
            )
        if self._registration_processor.has_preconfigured_agent_client:
            # Preconfigured clients own their CG authentication boundary; this guard only
            # validates the CLI/Admin-Service token path.
            return
        if not admin_service_credential_token_ready(
            pg_dsn=self._pg_dsn,
            project_id=project_id,
            token=self._registration_processor.admin_service_token,
        ):
            raise ConflictError("admin-service AgentCredential is required")

    def _attach_invitation_approval(
        self,
        request: ByoaProductRegistrationRequest,
    ) -> ByoaProductRegistrationRequest:
        if request.profile_kind != BYOA_PROFILE_CONVERSATION_WORKER_V1:
            return request
        if request.invitation is not None:
            raise ConflictError("BYOA conversation worker invitation must be supplied as invitation_code")
        if self._validate_invitation is None:
            raise ConflictError("BYOA conversation worker profile requires an invitation validator")
        if request.invitation_code is None:
            raise ForbiddenError("BYOA conversation worker profile requires an invitation_code")
        invitation = self._validate_invitation(request, request.invitation_code)
        if not isinstance(invitation, ByoaInviteApproval):
            raise ConflictError("BYOA invitation validator must return ByoaInviteApproval")
        return ByoaProductRegistrationRequest(
            request_id=request.request_id,
            project_id=request.project_id,
            requested_agent_id=request.requested_agent_id,
            display_name=request.display_name,
            description=request.description,
            requested_role=BYOA_EXTERNAL_CONVERSATION_WORKER_ROLE,
            requested_capabilities=(TURN_KIND_CONVERSATION_V1,),
            runtime_kind=request.runtime_kind,
            profile_kind=request.profile_kind,
            invitation_code=None,
            invitation=invitation,
        )


def parse_byoa_product_registration_request(
    request: ByoaProductRegistrationRequest | Mapping[str, Any],
) -> ByoaProductRegistrationRequest:
    if isinstance(request, ByoaProductRegistrationRequest):
        return _normalize_byoa_product_registration_request(request)
    if not isinstance(request, Mapping):
        raise ConflictError("BYOA registration request must be an object")
    unknown_fields = set(request) - _REQUEST_FIELDS
    if unknown_fields:
        raise ConflictError(
            "unsupported BYOA registration request field(s): " + ", ".join(sorted(str(key) for key in unknown_fields))
        )
    missing_fields = _REQUIRED_REQUEST_FIELDS - set(request)
    if missing_fields:
        raise ConflictError(
            "missing BYOA registration request field(s): " + ", ".join(sorted(missing_fields))
        )
    return ByoaProductRegistrationRequest(
        request_id=_required_string("request_id", request.get("request_id")),
        project_id=_required_string("project_id", request.get("project_id")),
        requested_agent_id=_required_string("requested_agent_id", request.get("requested_agent_id")),
        display_name=_required_string("display_name", request.get("display_name")),
        description=_optional_string("description", request.get("description")),
        requested_role=_required_string("requested_role", request.get("requested_role")),
        requested_capabilities=_string_tuple(request.get("requested_capabilities"), "requested_capabilities"),
        runtime_kind=_required_string("runtime_kind", request.get("runtime_kind")),
        profile_kind=_profile_kind(request.get("profile_kind", BYOA_PROFILE_WORK_MEMORY_REPORTER_V1)),
        invitation_code=_optional_string("invitation_code", request.get("invitation_code")),
        invitation=_invite_approval_from_raw(request.get("invitation")),
    )


def _normalize_byoa_product_registration_request(
    request: ByoaProductRegistrationRequest,
) -> ByoaProductRegistrationRequest:
    invitation = request.invitation
    if invitation is not None and not isinstance(invitation, ByoaInviteApproval):
        raise ConflictError("invitation must be ByoaInviteApproval when provided")
    return ByoaProductRegistrationRequest(
        request_id=_required_string("request_id", request.request_id),
        project_id=_required_string("project_id", request.project_id),
        requested_agent_id=_required_string("requested_agent_id", request.requested_agent_id),
        display_name=_required_string("display_name", request.display_name),
        description=_optional_string("description", request.description),
        requested_role=_required_string("requested_role", request.requested_role),
        requested_capabilities=_string_tuple(request.requested_capabilities, "requested_capabilities"),
        runtime_kind=_required_string("runtime_kind", request.runtime_kind),
        profile_kind=_profile_kind(request.profile_kind),
        invitation_code=_optional_string("invitation_code", request.invitation_code),
        invitation=invitation,
    )


def validate_byoa_mvp_policy(request: ByoaProductRegistrationRequest) -> None:
    if request.profile_kind == BYOA_PROFILE_CONVERSATION_WORKER_V1:
        if request.invitation is None:
            raise ForbiddenError("BYOA conversation worker profile requires a valid invitation")
        return
    if request.requested_role not in BYOA_ALLOWED_REQUESTED_ROLES:
        raise ConflictError(f"unsupported BYOA requested_role: {request.requested_role}")
    unsupported_capabilities = sorted(set(request.requested_capabilities) - BYOA_ALLOWED_REQUESTED_CAPABILITIES)
    if unsupported_capabilities:
        raise ConflictError(
            "unsupported BYOA requested_capability: " + ", ".join(unsupported_capabilities)
        )
    if request.requested_capabilities != (TURN_KIND_WORK_MEMORY_REPORT_V1,):
        raise ConflictError(
            f"BYOA request must include exactly requested_capability: {TURN_KIND_WORK_MEMORY_REPORT_V1}"
        )


def map_byoa_mvp_policy(
    request: ByoaProductRegistrationRequest,
    *,
    approved_by: str,
    policy_version: str = BYOA_MVP_POLICY_VERSION,
) -> ByoaPolicyMapping:
    validate_byoa_mvp_policy(request)
    approved_by = _required_string("approved_by", approved_by)
    policy_version = _required_string("policy_version", policy_version)
    public_metadata = _public_metadata_for_request(request)
    mapped_approved_by = approved_by
    role = request.requested_role
    accepts_work = False
    capabilities: tuple[str, ...] = ()
    if request.profile_kind == BYOA_PROFILE_CONVERSATION_WORKER_V1:
        assert request.invitation is not None
        mapped_approved_by = request.invitation.issued_by_user_id
        role = BYOA_EXTERNAL_CONVERSATION_WORKER_ROLE
        accepts_work = True
        capabilities = (TURN_KIND_CONVERSATION_V1,)
    spec = canonical_agent_birth_spec(
        AgentBirthSpec(
            agent_id=request.requested_agent_id,
            role=role,
            description=request.description,
            enabled=True,
            accepts_work=accepts_work,
            capacity=1,
            capabilities=capabilities,
            grants=(),
            public_metadata=public_metadata,
        )
    )
    admitted_spec_hash = agent_birth_spec_hash(spec)
    approval_record = {
        "kind": BYOA_APPROVAL_RECORD_KIND,
        "request_id": request.request_id,
        "project_id": request.project_id,
        "requested_agent_id": request.requested_agent_id,
        "approved_by": mapped_approved_by,
        "policy_version": policy_version,
        "admitted_spec_hash": admitted_spec_hash,
    }
    if request.profile_kind == BYOA_PROFILE_CONVERSATION_WORKER_V1:
        assert request.invitation is not None
        approval_record.update(
            {
                "approval_mode": request.invitation.approval_mode,
                "invite_id": request.invitation.invite_id,
                "issued_by_user_id": request.invitation.issued_by_user_id,
                "profile_kind": request.profile_kind,
            }
        )
    payload_hash = canonical_json_sha256(approval_record)
    return ByoaPolicyMapping(
        request=request,
        policy_version=policy_version,
        approved_by=mapped_approved_by,
        agent_birth_spec=spec,
        admitted_spec_hash=admitted_spec_hash,
        approval_record=approval_record,
        provenance_kind=DEFAULT_BYOA_PROVENANCE_KIND,
        provenance_external_ref=request.request_id,
        provenance_payload_hash=payload_hash,
    )


def _public_metadata_for_request(request: ByoaProductRegistrationRequest) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "ui": {"label": request.display_name},
        "admin_service": {
            "byoa_request_id": request.request_id,
            "runtime_kind": request.runtime_kind,
        },
    }
    if request.profile_kind == BYOA_PROFILE_CONVERSATION_WORKER_V1:
        assert request.invitation is not None
        metadata["turn_offers"] = [_conversation_turn_offer()]
        metadata["admin_service"].update(
            {
                "profile_kind": request.profile_kind,
            }
        )
    return metadata


def _conversation_turn_offer() -> dict[str, Any]:
    return {
        "turn_kind": TURN_KIND_CONVERSATION_V1,
        "purpose": "Handle a general conversation turn and return the final deliverable.",
        "calling": {
            "operation": "dispatch",
            "authority_modes": [
                {
                    "mode": "root_request",
                    "required_authority": ["trusted_requester_identity"],
                },
                {
                    "mode": "child_derivation",
                    "required_authority": ["active_parent_claim"],
                    "binds_cause_to_current_turn": True,
                },
            ],
        },
        "input_contract": {
            "required_fields": [],
            "example_payload": {"task": "Summarize the latest status."},
        },
        "variants": {},
        "notes": "Conversation-specific stop/resume/finish semantics remain turn-owned semantics.",
    }


def _connection_profile_for_registration(
    row: ByoaRegistrationRequest,
    request: ByoaProductRegistrationRequest,
    credential_issue: Mapping[str, Any],
) -> AdminServiceAgentConnectionProfile:
    agent_id = row.registered_agent_id or row.requested_agent_id
    credential = credential_issue.get("credential")
    if not isinstance(credential, Mapping) or not isinstance(credential.get("credential_id"), str):
        raise ConflictError("BYOA credential issue response missing credential_id")
    return AdminServiceAgentConnectionProfile(
        project_id=row.project_id,
        agent_id=agent_id,
        runtime_kind=request.runtime_kind,
        profile_kind=request.profile_kind,
        profile_ref=f"admin_service/byoa_registration_requests/{row.request_id}/connection-profile",
        credential_id=credential["credential_id"],
    )


def _credential_secret_from_issue(credential_issue: Mapping[str, Any]) -> AdminServiceAgentCredentialSecret:
    credential = credential_issue.get("credential")
    token = credential_issue.get("token")
    if not isinstance(credential, Mapping) or not isinstance(credential.get("credential_id"), str):
        raise ConflictError("BYOA credential issue response missing credential_id")
    if not isinstance(token, str) or not token:
        raise ConflictError("BYOA credential issue response missing token")
    return AdminServiceAgentCredentialSecret(
        credential_id=credential["credential_id"],
        token=token,
    )


def _agent_birth_spec_payload(spec: AgentBirthSpec) -> dict[str, Any]:
    return {
        "agent_id": spec.agent_id,
        "role": spec.role,
        "description": spec.description,
        "enabled": spec.enabled,
        "accepts_work": spec.accepts_work,
        "capacity": spec.capacity,
        "capabilities": list(spec.capabilities),
        "grants": list(spec.grants),
        "public_metadata": dict(spec.public_metadata),
    }


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise ConflictError(f"{field_name} must be an iterable of strings")
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        normalized = _required_string(field_name, item)
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    if not result:
        raise ConflictError(f"{field_name} must not be empty")
    return tuple(result)


def _required_string(field_name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConflictError(f"{field_name} must be non-empty")
    return value.strip()


def _optional_string(field_name: str, value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConflictError(f"{field_name} must be a string when provided")
    value = value.strip()
    return value or None


def _profile_kind(value: Any) -> str:
    profile_kind = _required_string("profile_kind", value)
    if profile_kind not in BYOA_PROFILE_KINDS:
        raise ConflictError(f"unsupported BYOA profile_kind: {profile_kind}")
    return profile_kind


def _allowed_profile_kinds(value: Any) -> tuple[str, ...]:
    if value is None:
        return (BYOA_PROFILE_CONVERSATION_WORKER_V1,)
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise ConflictError("allowed_profile_kinds must be an iterable of profile kinds")
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        profile_kind = _profile_kind(item)
        if profile_kind in seen:
            continue
        seen.add(profile_kind)
        result.append(profile_kind)
    if not result:
        raise ConflictError("allowed_profile_kinds must not be empty")
    return tuple(result)


def _invite_approval_from_raw(value: Any) -> ByoaInviteApproval | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ConflictError("invitation must be an object when provided")
    return ByoaInviteApproval(
        invite_id=_required_string("invitation.invite_id", value.get("invite_id")),
        issued_by_user_id=_required_string("invitation.issued_by_user_id", value.get("issued_by_user_id")),
        approval_mode=_required_string("invitation.approval_mode", value.get("approval_mode")),
    )


def _sha256_string(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_code_sha256(value: str) -> str:
    normalized = value.removeprefix("sha256:").lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise ConflictError("code_sha256 must be a hex SHA-256 digest")
    return normalized


def _optional_datetime(field_name: str, value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_aware_utc(value)
    if not isinstance(value, str):
        raise ConflictError(f"{field_name} must be an ISO datetime string when provided")
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return _ensure_aware_utc(datetime.fromisoformat(normalized))
    except ValueError as exc:
        raise ConflictError(f"{field_name} must be an ISO datetime string when provided") from exc


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _ensure_row_matches_request(
    row: ByoaRegistrationRequest,
    request: ByoaProductRegistrationRequest,
) -> None:
    mismatches = [
        field_name
        for field_name, row_value, request_value in (
            ("request_id", row.request_id, request.request_id),
            ("project_id", row.project_id, request.project_id),
            ("requested_agent_id", row.requested_agent_id, request.requested_agent_id),
        )
        if row_value != request_value
    ]
    if mismatches:
        raise ConflictError("BYOA workflow row does not match raw request: " + ", ".join(mismatches))


def _registration_processor_for_client(
    workflow_store: ByoaWorkflowStore,
    *,
    client: Any | None,
    base_url: str,
    admin_service_token: str | None,
) -> ByoaRegistrationProcessor:
    if _looks_like_configured_agent_client(client):
        return ByoaRegistrationProcessor(
            workflow_store,
            client,
            base_url=base_url,
        )
    return ByoaRegistrationProcessor(
        workflow_store,
        base_url=base_url,
        client=client,
        admin_service_token=admin_service_token,
    )


def _looks_like_configured_agent_client(value: Any) -> bool:
    return value is not None and hasattr(value, "register_agent_by_service") and hasattr(value, "get_agent")


__all__ = [
    "AdminServiceByoaApprovalResult",
    "AdminServiceByoaFacade",
    "AuthorizeByoaRequest",
    "BYOA_ALLOWED_REQUESTED_CAPABILITIES",
    "BYOA_ALLOWED_REQUESTED_ROLES",
    "BYOA_APPROVAL_RECORD_KIND",
    "BYOA_DIRECT_CG_PROFILE_STATUS_CREDENTIAL_READY",
    "BYOA_EXTERNAL_CONVERSATION_WORKER_ROLE",
    "BYOA_EXTERNAL_AGENT_ROLE",
    "BYOA_INVITE_CODE_APPROVAL_MODE",
    "BYOA_MVP_POLICY_VERSION",
    "BYOA_PROFILE_CONVERSATION_WORKER_V1",
    "BYOA_PROFILE_KINDS",
    "BYOA_PROFILE_WORK_MEMORY_REPORTER_V1",
    "AdminServiceAgentConnectionProfile",
    "AdminServiceAgentCredentialSecret",
    "ByoaPolicyMapping",
    "ByoaInviteApproval",
    "ByoaInviteConfigEntry",
    "ByoaJsonInviteValidator",
    "ByoaProductRegistrationRequest",
    "ValidateByoaInvitation",
    "map_byoa_mvp_policy",
    "parse_byoa_product_registration_request",
    "validate_byoa_mvp_policy",
]
