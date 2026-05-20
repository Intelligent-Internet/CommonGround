from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from CommonGround.contracts import (
    ConflictError,
    FencingError,
    ForbiddenError,
    InvariantError,
    KernelError,
    NotFoundError,
    TURN_KIND_WORK_MEMORY_REPORT_V1,
    UnauthorizedError,
)

from .byoa_facade import (
    BYOA_EXTERNAL_AGENT_ROLE,
    BYOA_PROFILE_CONVERSATION_WORKER_V1,
    BYOA_PROFILE_WORK_MEMORY_REPORTER_V1,
    AdminServiceByoaFacade,
    ByoaProductRegistrationRequest,
)
from .agent_join_invites import (
    DEFAULT_JOIN_EXPIRES_IN_SECONDS,
    DEFAULT_JOIN_MAX_USES,
    DEFAULT_JOIN_RUNTIME_KIND,
    AgentJoinInviteStore,
)

ResolveProductActor = Callable[[Request], str]


class AdminServiceModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RequestAgentCredentialTokenBody(AdminServiceModel):
    request_id: str
    requested_agent_id: str
    display_name: str
    runtime_kind: str
    description: str | None = None
    requested_role: str = BYOA_EXTERNAL_AGENT_ROLE
    requested_capabilities: tuple[str, ...] = (TURN_KIND_WORK_MEMORY_REPORT_V1,)
    profile_kind: str = BYOA_PROFILE_WORK_MEMORY_REPORTER_V1
    invitation_code: str | None = None
    creator_user_id: str | None = None


class CreateAgentJoinInviteBody(AdminServiceModel):
    agent_id: str
    profile_kind: str = BYOA_PROFILE_CONVERSATION_WORKER_V1
    runtime_kind: str = DEFAULT_JOIN_RUNTIME_KIND
    display_name: str | None = None
    description: str | None = None
    expires_in_seconds: int = DEFAULT_JOIN_EXPIRES_IN_SECONDS
    single_use: bool = True
    max_uses: int | None = None


class RedeemAgentJoinBody(AdminServiceModel):
    join_code: str


def create_agent_credential_token_request_router(
    facade: AdminServiceByoaFacade,
    *,
    resolve_requester_user_id: ResolveProductActor,
    resolve_approved_by: ResolveProductActor | None = None,
    join_invite_store: AgentJoinInviteStore | None = None,
    prefix: str = "/admin/v1",
) -> APIRouter:
    """Create the minimal product-layer token request endpoint.

    This router is intentionally outside CommonGround Service. The embedding
    product/Admin Service owns user auth and supplies actor resolvers.
    """

    router = APIRouter(prefix=prefix)

    @router.post("/projects/{project_id}/agent-credential-tokens:request")
    def request_agent_credential_token(
        project_id: str,
        body: RequestAgentCredentialTokenBody,
        request: Request,
    ):
        requester_user_id = _required_actor("requester_user_id", resolve_requester_user_id(request))
        approved_by = requester_user_id
        if resolve_approved_by is not None:
            approved_by = _required_actor("approved_by", resolve_approved_by(request))

        product_request = ByoaProductRegistrationRequest(
            request_id=body.request_id,
            project_id=project_id,
            requested_agent_id=body.requested_agent_id,
            display_name=body.display_name,
            description=body.description,
            requested_role=body.requested_role,
            requested_capabilities=body.requested_capabilities,
            runtime_kind=body.runtime_kind,
            profile_kind=body.profile_kind,
            invitation_code=body.invitation_code,
        )
        submitted = facade.submit_registration_request(
            product_request,
            requester_user_id=requester_user_id,
            creator_user_id=body.creator_user_id,
            actor_id=requester_user_id,
        )
        approval = facade.approve_registration_request(
            submitted.request_id,
            approved_by=approved_by,
        )
        return {
            "request_id": approval.request_id,
            "project_id": approval.project_id,
            "agent_id": approval.agent_id,
            "status": approval.status,
            "profile": asdict(approval.profile),
            "credential": {
                "credential_id": approval.profile.credential_id,
                "status": "active",
            },
            "agent_credential_token": approval.credential_secret.reveal_token(),
        }

    if join_invite_store is not None:

        @router.post("/projects/{project_id}/agent-join-invites")
        def create_agent_join_invite(
            project_id: str,
            body: CreateAgentJoinInviteBody,
            request: Request,
        ):
            requester_user_id = _required_actor("requester_user_id", resolve_requester_user_id(request))
            facade.authorize_project_request(requester_user_id, project_id)
            facade.ensure_project_ready_for_registration(project_id)
            max_uses = DEFAULT_JOIN_MAX_USES if body.single_use else (body.max_uses or DEFAULT_JOIN_MAX_USES)
            if body.max_uses is not None:
                max_uses = body.max_uses
            invite, join_code = join_invite_store.create_invite(
                project_id=project_id,
                agent_id=body.agent_id,
                issued_by_user_id=requester_user_id,
                profile_kind=body.profile_kind,
                runtime_kind=body.runtime_kind,
                display_name=body.display_name,
                description=body.description,
                expires_in_seconds=body.expires_in_seconds,
                max_uses=max_uses,
            )
            return {
                "invite": invite.to_public_payload(),
                "join_code": join_code,
            }

        def _redeem_agent_join(body: RedeemAgentJoinBody):
            invite, approval = join_invite_store.redeem(
                body.join_code,
                issue=lambda invite: facade.redeem_agent_join_invite(
                    request_id=invite.registration_request_id,
                    project_id=invite.project_id,
                    agent_id=invite.agent_id,
                    profile_kind=invite.profile_kind,
                    runtime_kind=invite.runtime_kind,
                    display_name=invite.display_name,
                    description=invite.description,
                    invite_id=invite.invite_id,
                    issued_by_user_id=invite.issued_by_user_id,
                ),
            )
            return {
                "request_id": approval.request_id,
                "project_id": approval.project_id,
                "agent_id": approval.agent_id,
                "status": approval.status,
                "profile": asdict(approval.profile),
                "credential": {
                    "credential_id": approval.profile.credential_id,
                    "status": "active",
                },
                "invite": invite.to_public_payload(),
                "agent_credential_token": approval.credential_secret.reveal_token(),
            }

        @router.post("/agent-joins:redeem")
        def redeem_agent_join(body: RedeemAgentJoinBody):
            return _redeem_agent_join(body)

        @router.post("/byoa/join:redeem")
        def redeem_byoa_join_alias(body: RedeemAgentJoinBody):
            return _redeem_agent_join(body)

    return router


def create_agent_credential_token_request_app(
    facade: AdminServiceByoaFacade,
    *,
    resolve_requester_user_id: ResolveProductActor,
    resolve_approved_by: ResolveProductActor | None = None,
    join_invite_store: AgentJoinInviteStore | None = None,
    prefix: str = "/admin/v1",
) -> FastAPI:
    app = FastAPI(title="CommonGround Admin Service Admission API", version="0.1.0")
    install_admin_service_exception_handlers(app)
    app.include_router(
        create_agent_credential_token_request_router(
            facade,
            resolve_requester_user_id=resolve_requester_user_id,
            resolve_approved_by=resolve_approved_by,
            join_invite_store=join_invite_store,
            prefix=prefix,
        )
    )
    return app


def install_admin_service_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(NotFoundError)
    async def _not_found(_, exc: NotFoundError):
        return _error_response(404, exc)

    @app.exception_handler(UnauthorizedError)
    async def _unauthorized(_, exc: UnauthorizedError):
        return _error_response(401, exc)

    @app.exception_handler(ForbiddenError)
    async def _forbidden(_, exc: ForbiddenError):
        return _error_response(403, exc)

    @app.exception_handler(ConflictError)
    async def _conflict(_, exc: ConflictError):
        return _error_response(409, exc)

    @app.exception_handler(FencingError)
    async def _fencing(_, exc: FencingError):
        return _error_response(409, exc)

    @app.exception_handler(InvariantError)
    async def _invariant(_, exc: InvariantError):
        return _error_response(422, exc)

    @app.exception_handler(ValueError)
    async def _value_error(_, exc: ValueError):
        message = str(exc)
        status = 409 if "admin_service_token" in message else 422
        return _error_response(status, exc)


def _error_response(status_code: int, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": exc.__class__.__name__,
            "code": _stable_error_code(exc),
            "message": str(exc),
        },
    )


def _stable_error_code(exc: Exception) -> str:
    message = str(exc)
    lowered = message.lower()
    if "project is not seeded" in lowered:
        return "project_not_seeded"
    if "admin-service bootstrap" in lowered or "bootstrap spec" in lowered:
        return "project_bootstrap_conflict"
    if "admin-service agentcredential" in lowered or "admin_service_token" in lowered:
        return "admin_service_credential_required"
    if "invitation validator" in lowered:
        return "invitation_validator_required"
    if "requires an invitation_code" in lowered or "must be supplied as invitation_code" in lowered:
        return "invitation_code_required"
    if "invitation code" in lowered or "valid invitation" in lowered:
        return "invitation_code_invalid"
    if "agent join code" in lowered:
        if "expired" in lowered:
            return "join_code_expired"
        if "disabled" in lowered:
            return "join_code_disabled"
        if "already been redeemed" in lowered:
            return "join_code_used"
        return "join_code_invalid"
    if isinstance(exc, UnauthorizedError):
        return "unauthorized"
    if isinstance(exc, ForbiddenError):
        return "forbidden"
    if isinstance(exc, NotFoundError):
        return "not_found"
    if isinstance(exc, ConflictError):
        return "conflict"
    if isinstance(exc, InvariantError):
        return "invalid_input"
    return "invalid_input"


def _required_actor(field_name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise UnauthorizedError(f"{field_name} is required")
    return value.strip()


__all__ = [
    "CreateAgentJoinInviteBody",
    "RequestAgentCredentialTokenBody",
    "RedeemAgentJoinBody",
    "ResolveProductActor",
    "create_agent_credential_token_request_app",
    "create_agent_credential_token_request_router",
    "install_admin_service_exception_handlers",
]
