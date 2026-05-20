from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from CommonGround.agent_credentials import AGENT_CREDENTIAL_ISSUE_ANY_GRANT, AGENT_CREDENTIAL_REVOKE_ANY_GRANT
from CommonGround.agent_registration import AGENT_REGISTRATION_BIRTH_GRANT
from CommonGround.app import KernelApp
from CommonGround.contracts import AgentRef, AgentSnapshot, ConflictError


ADMIN_SERVICE_AGENT_ID = "admin-service"
ADMIN_SERVICE_ROLE = "service.admin.v1"
ADMIN_SERVICE_GRANTS = (
    AGENT_CREDENTIAL_ISSUE_ANY_GRANT,
    AGENT_CREDENTIAL_REVOKE_ANY_GRANT,
    AGENT_REGISTRATION_BIRTH_GRANT,
)
ADMIN_SERVICE_PUBLIC_METADATA: Mapping[str, Any] = MappingProxyType(
    {
        "service": MappingProxyType(
            {
                "kind": "admin_service",
                "version": "v1",
            }
        )
    }
)
CREATOR_USER_AUTHORITY_KIND = "product.creator_user.v1"
CREATOR_REF_AUTHORITY_KIND = "product.creator_ref.v1"


@dataclass(frozen=True, slots=True)
class ProjectCreatorAuthority:
    """Immutable product-layer creator authority.

    This value belongs to the Admin Service or external product project store.
    It is deliberately not part of the CommonGround Kernel schema, Agent
    registration provenance, or Agent public_metadata.
    """

    kind: str
    external_ref: str
    display_name: str | None = None


@dataclass(frozen=True, slots=True)
class ProjectCreationBootstrapRequest:
    """Product-layer project creation input for bootstrapping Admin Service.

    The creator_authority field is an immutable product-layer fact. Callers are
    responsible for persisting it in their own project store; this helper only
    seeds the project-scoped Admin Service-Agent into CommonGround.
    """

    project_id: str
    creator_authority: ProjectCreatorAuthority


@dataclass(frozen=True, slots=True)
class AdminServiceProjectBootstrapResult:
    project_id: str
    creator_authority: ProjectCreatorAuthority
    admin_service: AgentSnapshot
    created_admin_service: bool

    @property
    def admin_service_ref(self) -> AgentRef:
        return self.admin_service.agent


class AdminServiceProjectBootstrap:
    """Reference product-layer facade for Admin Service project bootstrap.

    CommonGround currently has no project model or project creation API. This
    facade is intentionally outside Kernel and only uses the existing Agent
    plane to seed the project-scoped Admin Service-Agent needed by later
    product-layer Admin Service flows.
    """

    def __init__(self, kernel_app: KernelApp) -> None:
        self._kernel_app = kernel_app

    def create_project(self, request: ProjectCreationBootstrapRequest) -> AdminServiceProjectBootstrapResult:
        _validate_bootstrap_request(request)
        admin_service_ref = AgentRef(project_id=request.project_id, agent_id=ADMIN_SERVICE_AGENT_ID)
        existing = self._kernel_app.topology.get_agent(admin_service_ref)
        if existing is not None:
            _ensure_existing_admin_service_matches_bootstrap_spec(existing)
            return AdminServiceProjectBootstrapResult(
                project_id=request.project_id,
                creator_authority=request.creator_authority,
                admin_service=existing,
                created_admin_service=False,
            )

        self._kernel_app.topology.register_agent(
            admin_service_ref,
            role=ADMIN_SERVICE_ROLE,
            capabilities=(),
            accepts_work=False,
            grants=ADMIN_SERVICE_GRANTS,
            enabled=True,
        )
        self._kernel_app.topology.update_agent_public_metadata(
            admin_service_ref,
            _admin_service_public_metadata(),
        )
        snapshot = self._kernel_app.topology.get_agent(admin_service_ref)
        if snapshot is None:
            raise RuntimeError("admin-service bootstrap did not create the expected Agent")
        return AdminServiceProjectBootstrapResult(
            project_id=request.project_id,
            creator_authority=request.creator_authority,
            admin_service=snapshot,
            created_admin_service=True,
        )


def create_project(
    kernel_app: KernelApp,
    *,
    project_id: str,
    creator_user_id: str | None = None,
    creator_ref: str | None = None,
) -> AdminServiceProjectBootstrapResult:
    return bootstrap_project_admin_service_agent(
        kernel_app,
        project_id=project_id,
        creator_user_id=creator_user_id,
        creator_ref=creator_ref,
    )


def bootstrap_project_admin_service_agent(
    kernel_app: KernelApp,
    *,
    project_id: str,
    creator_user_id: str | None = None,
    creator_ref: str | None = None,
) -> AdminServiceProjectBootstrapResult:
    request = ProjectCreationBootstrapRequest(
        project_id=project_id,
        creator_authority=_creator_authority_from_args(
            creator_user_id=creator_user_id,
            creator_ref=creator_ref,
        ),
    )
    return bootstrap_admin_service_project(kernel_app, request)


def bootstrap_admin_service_project(
    kernel_app: KernelApp,
    request: ProjectCreationBootstrapRequest,
) -> AdminServiceProjectBootstrapResult:
    return AdminServiceProjectBootstrap(kernel_app).create_project(request)


def _admin_service_public_metadata() -> dict[str, Any]:
    return {"service": {"kind": "admin_service", "version": "v1"}}


def _ensure_existing_admin_service_matches_bootstrap_spec(snapshot: AgentSnapshot) -> None:
    mismatches = admin_service_bootstrap_spec_mismatches(snapshot)
    if mismatches:
        fields = ", ".join(mismatches)
        raise ConflictError(f"existing admin-service does not match bootstrap spec: {fields}")


def admin_service_bootstrap_spec_mismatches(snapshot: AgentSnapshot) -> tuple[str, ...]:
    expected_public_metadata = _admin_service_public_metadata()
    checks = {
        "role": snapshot.role == ADMIN_SERVICE_ROLE,
        "description": snapshot.description is None,
        "enabled": snapshot.enabled is True,
        "accepts_work": snapshot.accepts_work is False,
        "capacity": snapshot.capacity == 1,
        "capabilities": snapshot.capabilities == (),
        "grants": snapshot.grants == ADMIN_SERVICE_GRANTS,
        "public_metadata": dict(snapshot.public_metadata) == expected_public_metadata,
    }
    return tuple(field for field, matches in checks.items() if not matches)


def _creator_authority_from_args(
    *,
    creator_user_id: str | None,
    creator_ref: str | None,
) -> ProjectCreatorAuthority:
    if creator_user_id and creator_ref:
        raise ValueError("pass only one of creator_user_id or creator_ref")
    if creator_user_id:
        return ProjectCreatorAuthority(kind=CREATOR_USER_AUTHORITY_KIND, external_ref=creator_user_id)
    if creator_ref:
        return ProjectCreatorAuthority(kind=CREATOR_REF_AUTHORITY_KIND, external_ref=creator_ref)
    raise ValueError("creator_user_id or creator_ref must be provided")


def _validate_bootstrap_request(request: ProjectCreationBootstrapRequest) -> None:
    if not request.project_id:
        raise ValueError("project_id must be non-empty")
    if not request.creator_authority.kind:
        raise ValueError("creator_authority.kind must be non-empty")
    if not request.creator_authority.external_ref:
        raise ValueError("creator_authority.external_ref must be non-empty")
