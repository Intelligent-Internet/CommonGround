from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from CommonGround.agent_registration import AgentBirthSpec, AgentRegistrationProvenance
from CommonGround.agent_client import FinishTurnAction, HttpAgentClient, SuspendTurnAction
from CommonGround.contracts import (
    AgentRef,
    ClaimToken,
    ConflictError,
    NotFoundError,
    TURN_KIND_PROVISION_AGENT_SPAWN_V1,
    TurnOutcome,
)
from CommonGround.sdk import TurnContext
from CommonGround.provision_launch import (
    PROVISION_LAUNCH_STARTED_ROLE,
    ProvisionLaunchStarted,
    parse_provision_launch_started,
    provision_launch_started_payload,
)

from .context_mapping import extract_bootstrap_payload

from .registration_spec import AgentRegistrationSpec, REGISTRATION_SPEC_ENV, encode_registration_spec
from Integrations.nanobot.provision_role_policy import resolve_role_policy
from Integrations.nanobot.provision_lifecycle import (
    DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
    PROVISION_LIFECYCLE_METADATA_KEY,
    build_ephemeral_lifecycle_metadata,
)


@dataclass(frozen=True, slots=True)
class ProvisionLaunchRequest:
    project_id: str
    assigned_agent_id: str
    role: str
    description: str | None
    capabilities: tuple[str, ...]
    grants: tuple[str, ...]
    accepts_work: bool
    repo_root: str | None
    config_path: str | None
    workspace: str | None
    substrate: str
    provision_turn_id: str
    lifecycle_source_turn_id: str
    registration_mode: str
    env: dict[str, str]
    work_order_task_id: str | None = None
    lifecycle_source_turn_id_explicit: bool = False


@dataclass(frozen=True, slots=True)
class ProvisionLaunchResult:
    started: bool
    handle: str | None = None
    note: str | None = None


class OpsSubstrate(Protocol):
    def start_leaf_worker(self, request: ProvisionLaunchRequest) -> ProvisionLaunchResult:
        ...


class ProvisionAgentSpawnHandler:
    def __init__(
        self,
        *,
        substrate: OpsSubstrate,
        base_url: str,
        repo_root: str | None = None,
        config_path: str | None = None,
        workspace_root: str | None = None,
        substrate_kind: str = "process",
        lifecycle_ttl_seconds: int = DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
    ) -> None:
        self._substrate = substrate
        self._base_url = base_url
        self._repo_root = repo_root
        self._config_path = config_path
        self._workspace_root = workspace_root
        self._substrate_kind = substrate_kind
        self._lifecycle_ttl_seconds = lifecycle_ttl_seconds

    def handle_turn(self, context: TurnContext, client: HttpAgentClient, claim: ClaimToken):
        if context.turn.turn_kind != TURN_KIND_PROVISION_AGENT_SPAWN_V1:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "error": "unexpected_turn_kind",
                    "expected": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                    "actual": context.turn.turn_kind,
                },
            )

        try:
            launch_request = _parse_launch_request(
                extract_bootstrap_payload(context),
                project_id=claim.project_id,
                provision_turn_id=claim.turn_id,
                base_url=self._base_url,
                repo_root=self._repo_root,
                config_path=self._config_path,
                workspace_root=self._workspace_root,
                substrate=self._substrate_kind,
            )
            _validate_lifecycle_source_turn(context, launch_request)
        except InvalidLifecycleSourceTurn as exc:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload={"error": "invalid_lifecycle_source_turn", "message": str(exc)})
        except (ValueError, NotFoundError) as exc:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload={"error": "invalid_bootstrap", "message": str(exc)})
        provisioner = client.get_agent(claim.agent_ref())
        if provisioner is None:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={"error": "provisioner_not_registered", "message": f"agent not found: {claim.project_id}/{claim.agent_id}"},
            )

        new_agent_ref = AgentRef(project_id=claim.project_id, agent_id=launch_request.assigned_agent_id)
        try:
            prior_launch = _has_launch_started(context, launch_request=launch_request) or _has_progress_phase(context, "launch_result")
        except ValueError as exc:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload={"error": "invalid_bootstrap", "message": str(exc)})
        existing_agent = client.get_agent(new_agent_ref)
        if existing_agent is not None and not prior_launch:
            if _agent_was_born_for_provision_turn(existing_agent, claim):
                return _finish_observed_registration(
                    claim=claim,
                    client=client,
                    launch_request=launch_request,
                    new_agent=existing_agent,
                    handle=None,
                )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": False,
                    "registration_mode": launch_request.registration_mode,
                    "error": "agent_already_registered",
                    "existing_agent_ref": {
                        "project_id": existing_agent.agent.project_id,
                        "agent_id": existing_agent.agent.agent_id,
                    },
                    "assigned_agent_id": launch_request.assigned_agent_id,
                },
            )
        if existing_agent is not None:
            return _finish_observed_registration(claim=claim, client=client, launch_request=launch_request, new_agent=existing_agent, handle=None)
        if prior_launch:
            return SuspendTurnAction(reason="await_agent_registration", note=f"awaiting {launch_request.assigned_agent_id} registration")
        try:
            registered_agent = client.register_agent_by_service(
                project_id=claim.project_id,
                spec=AgentBirthSpec(
                    agent_id=launch_request.assigned_agent_id,
                    role=launch_request.role,
                    description=launch_request.description,
                    capabilities=launch_request.capabilities,
                    grants=launch_request.grants,
                    accepts_work=launch_request.accepts_work,
                    public_metadata={
                        PROVISION_LIFECYCLE_METADATA_KEY: build_ephemeral_lifecycle_metadata(
                            owner_agent=claim.agent_ref(),
                            source_turn_id=launch_request.lifecycle_source_turn_id,
                            ttl_seconds=self._lifecycle_ttl_seconds,
                        )
                    },
                ),
                provenance=AgentRegistrationProvenance(
                    kind="nanobot.provision_turn.v1",
                    external_ref=claim.turn_id,
                ),
            )
        except Exception as exc:
            return FinishTurnAction(outcome=TurnOutcome.FAILED, final_payload={"error": "registration_birth_failed", "message": str(exc)})
        scope = _registration_scope(launch_request=launch_request, new_agent=registered_agent)
        if not scope["valid"]:
            client.append_record(
                claim,
                {
                    "phase": "registration_scope_mismatch",
                    "registration_mode": launch_request.registration_mode,
                    "provision_turn_id": claim.turn_id,
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    **scope,
                },
                role="progress",
            )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "provision_turn_id": claim.turn_id,
                    "error": "registration_scope_mismatch",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    **scope,
                },
            )
        try:
            issued_credential = client.issue_agent_credential(
                new_agent_ref,
                provenance_kind="nanobot.provision_turn.v1",
                provenance_ref=claim.turn_id,
            )
        except Exception as exc:
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "error": "credential_issue_failed",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    "message": str(exc),
                },
            )
        credential_id = _issued_credential_id(issued_credential)
        credential_token = issued_credential.get("token")
        if not isinstance(credential_token, str) or not credential_token:
            cleanup = {}
            if credential_id is not None:
                cleanup = _revoke_issued_credential(
                    client,
                    claim=claim,
                    agent=new_agent_ref,
                    credential_id=credential_id,
                    reason="missing_plaintext_token",
                )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "error": "credential_issue_failed",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    "message": "issued credential response did not include a plaintext token",
                    **cleanup,
                },
            )
        launch_request.env["CG_AGENT_CREDENTIAL_TOKEN"] = credential_token
        try:
            client.append_record(
                claim,
                {
                    "phase": "launch_plan",
                    "role": launch_request.role,
                    "assigned_agent_id": launch_request.assigned_agent_id,
                    "capabilities": list(launch_request.capabilities),
                    "grants": list(launch_request.grants),
                    "accepts_work": launch_request.accepts_work,
                    "description": launch_request.description,
                    **({"task_id": launch_request.work_order_task_id} if launch_request.work_order_task_id is not None else {}),
                    "ops": {
                        "substrate": launch_request.substrate,
                        "workspace": launch_request.workspace,
                    },
                },
                role="progress",
            )
            client.append_record(
                claim,
                {
                    "phase": "registration_birth",
                    "registration_mode": launch_request.registration_mode,
                    "provision_turn_id": claim.turn_id,
                    **({"task_id": launch_request.work_order_task_id} if launch_request.work_order_task_id is not None else {}),
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                },
                role="progress",
            )
        except Exception as exc:
            cleanup = {}
            if credential_id is not None:
                cleanup = _revoke_issued_credential(
                    client,
                    claim=claim,
                    agent=new_agent_ref,
                    credential_id=credential_id,
                    reason="prelaunch_progress_record_failed",
                )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "error": "prelaunch_progress_record_failed",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    "message": str(exc),
                    **cleanup,
                },
            )
        try:
            client.append_record(
                claim,
                provision_launch_started_payload(
                    assigned_agent_id=launch_request.assigned_agent_id,
                    role=launch_request.role,
                ),
                role=PROVISION_LAUNCH_STARTED_ROLE,
            )
        except Exception as exc:
            cleanup = {}
            if credential_id is not None:
                cleanup = _revoke_issued_credential(
                    client,
                    claim=claim,
                    agent=new_agent_ref,
                    credential_id=credential_id,
                    reason="launch_started_record_failed",
                )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "error": "launch_started_record_failed",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    "message": str(exc),
                    **cleanup,
                },
            )

        try:
            result = self._substrate.start_leaf_worker(launch_request)
        except Exception as exc:
            cleanup = {}
            if credential_id is not None:
                cleanup = _revoke_issued_credential(
                    client,
                    claim=claim,
                    agent=new_agent_ref,
                    credential_id=credential_id,
                    reason="launch_start_failed",
                )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "error": "launch_start_failed",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    "message": str(exc),
                    **cleanup,
                },
            )
        try:
            client.append_record(
                claim,
                {
                    "phase": "launch_result",
                    "started": result.started,
                    **({"task_id": launch_request.work_order_task_id} if launch_request.work_order_task_id is not None else {}),
                    "ops_debug": {"substrate": launch_request.substrate, "handle": result.handle},
                    "note": result.note,
                },
                role="progress",
            )
        except Exception as exc:
            if result.started:
                raise
            cleanup = {}
            if credential_id is not None:
                cleanup = _revoke_issued_credential(
                    client,
                    claim=claim,
                    agent=new_agent_ref,
                    credential_id=credential_id,
                    reason="launch_result_record_failed",
                )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "error": "launch_result_record_failed",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    "message": str(exc),
                    **cleanup,
                },
            )

        if not result.started:
            cleanup = {}
            if credential_id is not None:
                cleanup = _revoke_issued_credential(
                    client,
                    claim=claim,
                    agent=new_agent_ref,
                    credential_id=credential_id,
                    reason="launch_failed",
                )
            return FinishTurnAction(
                outcome=TurnOutcome.FAILED,
                final_payload={
                    "registered": True,
                    "registration_mode": launch_request.registration_mode,
                    "error": "launch_failed",
                    "new_agent_ref": {
                        "project_id": registered_agent.agent.project_id,
                        "agent_id": registered_agent.agent.agent_id,
                    },
                    "ops_debug": {"substrate": launch_request.substrate, "handle": result.handle},
                    "note": result.note,
                    **cleanup,
                },
            )
        new_agent = client.get_agent(new_agent_ref) or registered_agent

        return _finish_observed_registration(claim=claim, client=client, launch_request=launch_request, new_agent=new_agent, handle=result.handle)


def _parse_launch_request(
    payload: Any,
    *,
    project_id: str,
    provision_turn_id: str,
    base_url: str,
    repo_root: str | None,
    config_path: str | None,
    workspace_root: str | None,
    substrate: str,
) -> ProvisionLaunchRequest:
    if not isinstance(payload, dict):
        raise ValueError("bootstrap payload must be an object")
    _reject_unsupported_spawn_fields(payload)
    agent_payload = payload.get("agent")
    if not isinstance(agent_payload, dict):
        raise ValueError("agent is required")
    role = _required_str(agent_payload, "role")
    role_policy = resolve_role_policy(role)
    capabilities = role_policy.allowed_capabilities
    grants = role_policy.allowed_grants
    accepts_work = role_policy.allowed_accepts_work
    description = role_policy.description or None
    registration_mode = "service_authorized_birth"
    assigned_agent_id = _assigned_agent_id(provision_turn_id)
    lifecycle_source_turn_id = _lifecycle_source_turn_id(payload, default=provision_turn_id)
    lifecycle_source_turn_id_explicit = _has_explicit_lifecycle_source_turn_id(payload)
    work_order_task_id = _work_order_task_id(payload)
    workspace = None if workspace_root is None else str(Path(workspace_root) / assigned_agent_id)

    env = {
        "CG_BASE_URL": base_url,
        "CG_PROJECT_ID": project_id,
        "CG_AGENT_ID": assigned_agent_id,
        "CG_PROVISION_TURN_ID": provision_turn_id,
        REGISTRATION_SPEC_ENV: encode_registration_spec(
            AgentRegistrationSpec(
                role=role,
                capabilities=capabilities,
                grants=grants,
                description=description,
                accepts_work=accepts_work,
            )
        ),
    }
    if repo_root is not None:
        env["NANOBOT_REPO_ROOT"] = repo_root
    if config_path is not None:
        env["NANOBOT_CONFIG_PATH"] = config_path
    if workspace is not None:
        env["NANOBOT_WORKSPACE"] = workspace

    return ProvisionLaunchRequest(
        project_id=project_id,
        assigned_agent_id=assigned_agent_id,
        role=role,
        description=description,
        capabilities=capabilities,
        grants=grants,
        accepts_work=accepts_work,
        repo_root=repo_root,
        config_path=config_path,
        workspace=workspace,
        substrate=substrate,
        provision_turn_id=provision_turn_id,
        lifecycle_source_turn_id=lifecycle_source_turn_id,
        work_order_task_id=work_order_task_id,
        lifecycle_source_turn_id_explicit=lifecycle_source_turn_id_explicit,
        registration_mode=registration_mode,
        env=env,
    )


class InvalidLifecycleSourceTurn(ValueError):
    pass


def _validate_lifecycle_source_turn(context: TurnContext, launch_request: ProvisionLaunchRequest) -> None:
    cause = getattr(getattr(context, "turn", None), "cause", None)
    if getattr(cause, "kind", None) != "turn":
        if (
            launch_request.lifecycle_source_turn_id_explicit
            and launch_request.lifecycle_source_turn_id != launch_request.provision_turn_id
        ):
            raise InvalidLifecycleSourceTurn(
                "explicit lifecycle.source_turn_id is only valid for parent-caused provision turns "
                f"or when it matches the provision turn id {launch_request.provision_turn_id!r}; "
                f"got {launch_request.lifecycle_source_turn_id!r}"
            )
        return
    source_turn_id = getattr(cause, "id", None)
    if not isinstance(source_turn_id, str) or not source_turn_id:
        raise InvalidLifecycleSourceTurn("provision turn cause does not include a valid parent turn id")
    if launch_request.lifecycle_source_turn_id != source_turn_id:
        raise InvalidLifecycleSourceTurn(
            "lifecycle.source_turn_id must match provision turn parent/cause turn "
            f"{source_turn_id!r}; got {launch_request.lifecycle_source_turn_id!r}"
        )


def _lifecycle_source_turn_id(payload: dict[str, Any], *, default: str) -> str:
    lifecycle = payload.get("lifecycle")
    if lifecycle is None:
        return default
    if not isinstance(lifecycle, dict):
        raise ValueError("lifecycle must be an object when provided")
    source_turn_id = lifecycle.get("source_turn_id")
    if source_turn_id is None:
        return default
    if not isinstance(source_turn_id, str) or not source_turn_id:
        raise ValueError("lifecycle.source_turn_id must be a non-empty string")
    return source_turn_id


def _has_explicit_lifecycle_source_turn_id(payload: dict[str, Any]) -> bool:
    lifecycle = payload.get("lifecycle")
    return isinstance(lifecycle, dict) and lifecycle.get("source_turn_id") is not None


def _work_order_task_id(payload: dict[str, Any]) -> str | None:
    work_order = payload.get("work_order")
    if work_order is None:
        return None
    if not isinstance(work_order, dict):
        raise ValueError("work_order must be an object when provided")
    task_id = work_order.get("task_id")
    if task_id is None:
        return None
    if not isinstance(task_id, str) or not task_id:
        raise ValueError("work_order.task_id must be a non-empty string when provided")
    return task_id


def _assigned_agent_id(provision_turn_id: str) -> str:
    return f"nanobot_leaf_{provision_turn_id}"


def _reject_unsupported_spawn_fields(payload: dict[str, Any]) -> None:
    unsupported_keys = (
        "requested_agent_id",
        "registration_profile_id",
        "requested_capabilities",
        "requested_grants",
        "requested_accepts_work",
        "accepts_work",
        "nanobot",
        "ops",
        "role",
        "agent_role",
    )
    for key in unsupported_keys:
        if key in payload:
            raise ValueError(f"unsupported bootstrap field is not allowed: {key}")


def _finish_observed_registration(
    *,
    claim: ClaimToken,
    client: HttpAgentClient,
    launch_request: ProvisionLaunchRequest,
    new_agent,
    handle: str | None,
) -> FinishTurnAction:
    scope = _registration_scope(launch_request=launch_request, new_agent=new_agent)
    if not scope["valid"]:
        client.append_record(
            claim,
            {
                "phase": "registration_scope_mismatch",
                "registration_mode": launch_request.registration_mode,
                "provision_turn_id": claim.turn_id,
                "new_agent_ref": {
                    "project_id": new_agent.agent.project_id,
                    "agent_id": new_agent.agent.agent_id,
                },
                **scope,
            },
            role="progress",
        )
        return FinishTurnAction(
            outcome=TurnOutcome.FAILED,
            final_payload={
                "registered": True,
                "registration_mode": launch_request.registration_mode,
                "provision_turn_id": claim.turn_id,
                "error": "registration_scope_mismatch",
                "new_agent_ref": {
                    "project_id": new_agent.agent.project_id,
                    "agent_id": new_agent.agent.agent_id,
                },
                **scope,
            },
        )

    registration_provenance = {
        "registration_mode": launch_request.registration_mode,
        "provision_turn_id": claim.turn_id,
    }
    client.append_record(
        claim,
        {
            "phase": "registration_observed",
            "registration_mode": launch_request.registration_mode,
            "provision_turn_id": claim.turn_id,
            "registration_provenance": registration_provenance,
            "new_agent_ref": {
                "project_id": new_agent.agent.project_id,
                "agent_id": new_agent.agent.agent_id,
            },
            **scope,
        },
        role="progress",
    )
    return FinishTurnAction(
        outcome=TurnOutcome.SUCCEEDED,
        final_payload={
            "new_agent_ref": {
                "project_id": new_agent.agent.project_id,
                "agent_id": new_agent.agent.agent_id,
            },
            "registered": True,
            "registration_mode": launch_request.registration_mode,
            "provision_turn_id": claim.turn_id,
            **({"task_id": launch_request.work_order_task_id} if launch_request.work_order_task_id is not None else {}),
            "registration_provenance": registration_provenance,
            **scope,
            "ops_debug": {
                "substrate": launch_request.substrate,
                "handle": handle,
                "workspace": launch_request.workspace,
            },
        },
    )


def _issued_credential_id(issued_credential: dict[str, Any]) -> str | None:
    credential_id = issued_credential.get("credential_id")
    if isinstance(credential_id, str) and credential_id:
        return credential_id
    credential = issued_credential.get("credential")
    if isinstance(credential, dict):
        credential_id = credential.get("credential_id")
        if isinstance(credential_id, str) and credential_id:
            return credential_id
    return None


def _revoke_issued_credential(
    client: HttpAgentClient,
    *,
    claim: ClaimToken,
    agent: AgentRef,
    credential_id: str,
    reason: str,
) -> dict[str, Any]:
    cleanup: dict[str, Any] = {
        "credential_cleanup": {
            "attempted": True,
            "credential_id": credential_id,
            "reason": reason,
        }
    }
    try:
        cleanup["credential_cleanup"]["result"] = client.revoke_agent_credential(agent, credential_id)
    except Exception as exc:
        cleanup["credential_cleanup_error"] = str(exc)
    try:
        client.append_record(
            claim,
            {
                "phase": "credential_cleanup",
                "agent_ref": {"project_id": agent.project_id, "agent_id": agent.agent_id},
                **cleanup,
            },
            role="progress",
        )
    except Exception as exc:
        if "credential_cleanup_error" not in cleanup:
            cleanup["credential_cleanup_error"] = f"cleanup progress record failed: {exc}"
    return cleanup


def _registration_scope(*, launch_request: ProvisionLaunchRequest, new_agent) -> dict[str, Any]:
    requested_capabilities = tuple(launch_request.capabilities)
    observed_capabilities = tuple(new_agent.capabilities)
    requested_grants = tuple(launch_request.grants)
    observed_grants = tuple(new_agent.grants)
    requested_accepts_work = launch_request.accepts_work
    observed_accepts_work = new_agent.accepts_work
    missing_capabilities = sorted(set(requested_capabilities) - set(observed_capabilities))
    missing_grants = sorted(set(requested_grants) - set(observed_grants))
    extra_grants = sorted(set(observed_grants) - set(requested_grants))
    accepts_work_mismatch = requested_accepts_work != observed_accepts_work
    return {
        "valid": not missing_capabilities and not missing_grants and not extra_grants and not accepts_work_mismatch,
        "requested_capabilities": list(requested_capabilities),
        "observed_capabilities": list(observed_capabilities),
        "capabilities": list(observed_capabilities),
        "requested_grants": list(requested_grants),
        "observed_grants": list(observed_grants),
        "grants": list(observed_grants),
        "requested_accepts_work": requested_accepts_work,
        "observed_accepts_work": observed_accepts_work,
        "missing_capabilities": missing_capabilities,
        "missing_grants": missing_grants,
        "extra_grants": extra_grants,
        "accepts_work_mismatch": accepts_work_mismatch,
    }


def _agent_was_born_for_provision_turn(new_agent, claim: ClaimToken) -> bool:
    return (
        getattr(new_agent, "registration_provenance_kind", None) == "nanobot.provision_turn.v1"
        and getattr(new_agent, "registration_provenance_ref", None) == claim.turn_id
    )


def _has_progress_phase(context: TurnContext, phase: str) -> bool:
    for item in context.semantic_items:
        if item.record.record_role != "progress":
            continue
        payload = item.content.payload()
        if isinstance(payload, dict) and payload.get("phase") == phase:
            return True
    return False


def _has_launch_started(context: TurnContext, *, launch_request: ProvisionLaunchRequest) -> bool:
    for item in context.semantic_items:
        if item.record.record_role != PROVISION_LAUNCH_STARTED_ROLE:
            continue
        started = _parse_launch_started(item.content.payload())
        if started.assigned_agent_id != launch_request.assigned_agent_id or started.role != launch_request.role:
            raise ValueError("provision launch started record does not match resolved launch request")
        return True
    return False


def _parse_launch_started(payload: Any) -> ProvisionLaunchStarted:
    try:
        return parse_provision_launch_started(payload)
    except ConflictError as exc:
        raise ValueError(str(exc)) from exc


def _required_str(data: dict[str, Any], key: str) -> str:
    value = _optional_str(data, key)
    if value is None:
        raise ValueError(f"{key} is required")
    return value


def _optional_str(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value
