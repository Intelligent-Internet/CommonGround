from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from fastapi import Request

from CommonGround.agent_credentials import parse_agent_credential_token, verify_agent_credential_secret
from CommonGround.contracts import (
    AGENT_CREDENTIAL_STATUS_ACTIVE,
    AgentRef,
    ConflictError,
    ForbiddenError,
    NotFoundError,
    UnauthorizedError,
)


@dataclass(frozen=True, slots=True)
class AuthenticatedCaller:
    project_id: str
    agent_id: str
    credential_id: str

    def agent_ref(self) -> AgentRef:
        return AgentRef(project_id=self.project_id, agent_id=self.agent_id)


def authenticate_agent_request(request: Request) -> AuthenticatedCaller:
    deps = request.app.state.service_deps
    config = deps.config
    project_id = request.headers.get(config.agent_project_header)
    agent_id = request.headers.get(config.agent_id_header)
    if not project_id or not agent_id:
        raise UnauthorizedError("claimed agent identity headers are required")

    authorization = request.headers.get("Authorization")
    if not authorization:
        raise UnauthorizedError("agent credential authorization is required")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise UnauthorizedError("agent credential authorization must use Bearer token")
    try:
        parsed = parse_agent_credential_token(token)
    except ValueError as exc:
        raise UnauthorizedError("invalid agent credential token format") from exc

    credential = deps.credential_store.load_agent_credential_by_id(parsed.credential_id)
    if credential is None:
        raise UnauthorizedError("agent credential not found")
    if not verify_agent_credential_secret(parsed.secret, credential.secret_hash):
        raise UnauthorizedError("invalid agent credential secret")
    if credential.status != AGENT_CREDENTIAL_STATUS_ACTIVE:
        raise ForbiddenError(f"agent credential status is not active: {credential.status}")
    if credential.expires_at is not None and credential.expires_at <= datetime.now(UTC):
        raise UnauthorizedError("agent credential expired")
    if credential.project_id != project_id or credential.agent_id != agent_id:
        raise ForbiddenError("agent credential identity does not match claimed identity")

    agent = deps.kernel_app.topology.get_agent(AgentRef(project_id=project_id, agent_id=agent_id))
    if agent is None:
        raise ForbiddenError("authenticated agent does not exist")
    if not agent.enabled:
        raise ForbiddenError("authenticated agent is disabled")

    try:
        deps.credential_store.mark_agent_credential_used(credential.credential_id)
    except (ConflictError, NotFoundError) as exc:
        raise UnauthorizedError("agent credential is no longer active") from exc
    return AuthenticatedCaller(
        project_id=project_id,
        agent_id=agent_id,
        credential_id=credential.credential_id,
    )


CallerIdentity = AuthenticatedCaller


def require_caller_identity(request: Request) -> AuthenticatedCaller:
    return authenticate_agent_request(request)


def require_caller_match(request: Request, *, expected: AgentRef) -> AuthenticatedCaller:
    caller = require_caller_identity(request)
    if caller.project_id != expected.project_id or caller.agent_id != expected.agent_id:
        raise ForbiddenError("authenticated caller identity does not match request body")
    return caller
