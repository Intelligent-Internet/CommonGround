from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from fastapi import Request

from CommonGround.contracts import AgentRef, ClaimToken, ConflictError

from .auth import CallerIdentity, require_caller_identity, require_caller_match


class WriteSurfaceKind(StrEnum):
    TURN_BIRTH = "turn_birth_write"
    CLAIM_ACQUIRE = "claim_acquire_write"
    CLAIM_RENEWAL = "claim_renewal_write"
    CLAIM_RECONCILE = "claim_reconcile_write"
    CLAIM_FENCED_TURN_MUTATION = "claim_fenced_turn_mutation"
    TURN_RESUME = "turn_resume_write"
    TURN_STOP_REQUEST = "turn_stop_request_write"
    AGENT_OPERATIONAL_STATE = "agent_operational_state_write"
    AGENT_PUBLIC_METADATA = "agent_public_metadata_write"
    AGENT_PRESENCE = "agent_presence_write"
    AGENT_REGISTRATION_BIRTH = "agent_registration_birth_write"
    WORK_MEMORY_REPORT_SUBMISSION = "work_memory_report_submission_write"


class WriteAuthorityBindingKind(StrEnum):
    AUTHENTICATED_CALLER_MATCHES_REQUESTED_BY = "authenticated_caller_matches_requested_by"
    AUTHENTICATED_CALLER_MATCHES_AGENT = "authenticated_caller_matches_agent"
    AUTHENTICATED_CALLER_MATCHES_CLAIM_OWNER = "authenticated_caller_matches_claim_owner"
    AUTHENTICATED_CALLER_DEFAULTS_TARGET_AGENT = "authenticated_caller_defaults_target_agent"


@dataclass(frozen=True, slots=True)
class WriteAuthorizationRequest:
    project_id: str
    surface_kind: WriteSurfaceKind
    resource_family: str
    operation: str
    binding_kind: WriteAuthorityBindingKind
    caller: CallerIdentity | None
    resource_id: str | None = None
    actor: AgentRef | None = None
    proof_project_id: str | None = None
    proof_turn_id: str | None = None


class ServiceWriteGuard:
    def require_requested_by(
        self,
        request: Request,
        *,
        project_id: str,
        requested_by: AgentRef,
        surface_kind: WriteSurfaceKind,
        resource_family: str,
        operation: str,
        resource_id: str | None = None,
    ) -> WriteAuthorizationRequest:
        _require_project_match(project_id, requested_by.project_id, "requested_by project must match path project")
        caller = require_caller_match(request, expected=requested_by)
        write_request = WriteAuthorizationRequest(
            project_id=project_id,
            surface_kind=surface_kind,
            resource_family=resource_family,
            operation=operation,
            binding_kind=WriteAuthorityBindingKind.AUTHENTICATED_CALLER_MATCHES_REQUESTED_BY,
            caller=caller,
            resource_id=resource_id,
            actor=requested_by,
            proof_project_id=requested_by.project_id,
        )
        self.authorize(write_request)
        return write_request

    def require_agent_actor(
        self,
        request: Request,
        *,
        project_id: str,
        agent: AgentRef,
        surface_kind: WriteSurfaceKind,
        resource_family: str,
        operation: str,
        path_agent_id: str | None = None,
    ) -> WriteAuthorizationRequest:
        _require_project_match(project_id, agent.project_id, "agent project must match path project")
        if path_agent_id is not None and path_agent_id != agent.agent_id:
            raise ConflictError("path agent_id must match request agent")
        caller = require_caller_match(request, expected=agent)
        write_request = WriteAuthorizationRequest(
            project_id=project_id,
            surface_kind=surface_kind,
            resource_family=resource_family,
            operation=operation,
            binding_kind=WriteAuthorityBindingKind.AUTHENTICATED_CALLER_MATCHES_AGENT,
            caller=caller,
            resource_id=path_agent_id or agent.agent_id,
            actor=agent,
            proof_project_id=agent.project_id,
        )
        self.authorize(write_request)
        return write_request

    def require_claim_owner(
        self,
        request: Request,
        *,
        project_id: str,
        claim: ClaimToken,
        surface_kind: WriteSurfaceKind,
        resource_family: str,
        operation: str,
        path_turn_id: str | None = None,
    ) -> WriteAuthorizationRequest:
        _require_project_match(project_id, claim.project_id, "claim project must match path project")
        if path_turn_id is not None and path_turn_id != claim.turn_id:
            raise ConflictError("path turn_id must match claim.turn_id")
        caller = require_caller_match(request, expected=claim.agent_ref())
        write_request = WriteAuthorizationRequest(
            project_id=project_id,
            surface_kind=surface_kind,
            resource_family=resource_family,
            operation=operation,
            binding_kind=WriteAuthorityBindingKind.AUTHENTICATED_CALLER_MATCHES_CLAIM_OWNER,
            caller=caller,
            resource_id=path_turn_id or claim.turn_id,
            actor=claim.agent_ref(),
            proof_project_id=claim.project_id,
            proof_turn_id=claim.turn_id,
        )
        self.authorize(write_request)
        return write_request

    def require_caller_default_agent(
        self,
        request: Request,
        *,
        project_id: str,
        surface_kind: WriteSurfaceKind,
        resource_family: str,
        operation: str,
    ) -> WriteAuthorizationRequest:
        caller = require_caller_identity(request)
        _require_project_match(project_id, caller.project_id, "caller project must match path project")
        actor = caller.agent_ref()
        write_request = WriteAuthorizationRequest(
            project_id=project_id,
            surface_kind=surface_kind,
            resource_family=resource_family,
            operation=operation,
            binding_kind=WriteAuthorityBindingKind.AUTHENTICATED_CALLER_DEFAULTS_TARGET_AGENT,
            caller=caller,
            resource_id=actor.agent_id,
            actor=actor,
            proof_project_id=actor.project_id,
        )
        self.authorize(write_request)
        return write_request

    def require_registration_service_actor(
        self,
        request: Request,
        *,
        project_id: str,
    ) -> WriteAuthorizationRequest:
        return self.require_caller_default_agent(
            request,
            project_id=project_id,
            surface_kind=WriteSurfaceKind.AGENT_REGISTRATION_BIRTH,
            resource_family="agent_registration",
            operation="birth",
        )

    def authorize(self, request: WriteAuthorizationRequest) -> None:
        return None


def _require_project_match(path_project_id: str, proof_project_id: str, message: str) -> None:
    if path_project_id != proof_project_id:
        raise ConflictError(message)
