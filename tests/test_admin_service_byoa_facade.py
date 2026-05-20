from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from CommonGround.agent_client import HttpAgentClient, agent_auth_headers
from CommonGround.agent_credentials import parse_agent_credential_token
from CommonGround.contracts import (
    AgentRef,
    ConflictError,
    DispatchAuthority,
    DispatchAuthorityMode,
    ForbiddenError,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_WORK_MEMORY_REPORT_V1,
    TurnOutcome,
    TurnState,
)
from CommonGround.service.projection.filters import TurnOfferFilters
from CommonGround.service.projection.offers import list_turn_offer_entries
from CommonGround.service.projection.postgres_source import PostgresProjectionSource
from Integrations.admin_service import (
    ADMIN_SERVICE_AGENT_ID,
    BYOA_INVITE_CODE_APPROVAL_MODE,
    BYOA_PROFILE_CONVERSATION_WORKER_V1,
    BYOA_PROFILE_WORK_MEMORY_REPORTER_V1,
    BYOA_STATUS_APPROVED,
    BYOA_STATUS_REGISTERED,
    BYOA_STATUS_VALIDATED,
    AdminServiceByoaFacade,
    ByoaJsonInviteValidator,
    ByoaWorkflowStore,
    bootstrap_project_admin_service_agent,
    canonical_json_sha256,
)
from tests.auth_support import agent_token


PROJECT_ID = "admin-byoa-facade"
REQUEST_ID = "byoa-facade-req-001"
REQUESTER_ID = "requester-user-001"
APPROVER_ID = "approver-user-001"
INVITE_ISSUER_ID = "invite-issuer-user-001"
INVITE_ID = "invite-conversation-worker-001"
INVITATION_CODE = "invite-code-001"
AGENT_ID = "byoa-reporting-agent"
CONVERSATION_AGENT_ID = "byoa-conversation-worker"
RUNTIME_KIND = "test.external.runtime.v1"


@pytest.fixture(autouse=True)
def fresh_admin_service_tables(test_pg_dsn: str):
    _drop_admin_service_tables(test_pg_dsn)
    yield
    _drop_admin_service_tables(test_pg_dsn)


def test_facade_requires_explicit_authorizer(test_pg_dsn: str) -> None:
    with pytest.raises(ValueError, match="explicit authorize_request"):
        AdminServiceByoaFacade(test_pg_dsn, client=FailingPostClient())


def test_submit_rejects_unknown_grants_field_before_workflow_or_cg(test_pg_dsn: str) -> None:
    cg = FailingPostClient()
    facade = AdminServiceByoaFacade(test_pg_dsn, client=cg, authorize_request=_allow_project)

    with pytest.raises(ConflictError, match="unsupported BYOA registration request field.*grants"):
        facade.submit_registration_request(_byoa_request(grants=["agent.registration.birth"]), requester_user_id=REQUESTER_ID)

    assert _request_count(test_pg_dsn) == 0
    assert cg.calls == []


@pytest.mark.parametrize(
    ("request_patch", "message"),
    (
        ({"requested_role": "service.admin.v1"}, "unsupported BYOA requested_role"),
        ({"requested_capabilities": [TURN_KIND_CONVERSATION_V1]}, "unsupported BYOA requested_capability"),
    ),
)
def test_approve_rejects_non_allowlisted_role_or_capability_before_cg_registration(
    test_pg_dsn: str,
    kernel_app,
    request_patch: dict[str, Any],
    message: str,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_service_token = agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID))
    cg = FailingPostClient()
    facade = AdminServiceByoaFacade(test_pg_dsn, client=cg, admin_service_token=admin_service_token, authorize_request=_allow_project)
    submitted = facade.submit_registration_request(_byoa_request(**request_patch), requester_user_id=REQUESTER_ID)

    assert submitted.status == BYOA_STATUS_VALIDATED
    with pytest.raises(ConflictError, match=message):
        facade.approve_registration_request(REQUEST_ID, approved_by=APPROVER_ID)

    assert cg.calls == []
    row = ByoaWorkflowStore(test_pg_dsn).get_request(REQUEST_ID)
    assert row is not None
    assert row.registered_agent_id is None


def test_submit_rejects_unauthorized_before_workflow_cg_or_agent_creation(
    test_pg_dsn: str,
    kernel_app,
) -> None:
    cg = FailingPostClient()
    authorize_calls: list[tuple[str, str]] = []

    def deny_request(requester_user_id: str, project_id: str) -> bool:
        authorize_calls.append((requester_user_id, project_id))
        return False

    facade = AdminServiceByoaFacade(test_pg_dsn, client=cg, authorize_request=deny_request)

    with pytest.raises(ForbiddenError, match="not authorized"):
        facade.submit_registration_request(_byoa_request(), requester_user_id=REQUESTER_ID)

    assert authorize_calls == [(REQUESTER_ID, PROJECT_ID)]
    assert _request_count(test_pg_dsn) == 0
    assert cg.calls == []
    assert kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)) is None


def test_happy_path_registers_with_processor_returns_direct_connection_profile(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    approval, workflow = _approve_happy_path(test_pg_dsn, test_client, kernel_app)

    assert approval.status == BYOA_STATUS_REGISTERED
    assert approval.workflow_row.status == BYOA_STATUS_REGISTERED
    assert approval.profile.project_id == PROJECT_ID
    assert approval.profile.agent_id == AGENT_ID
    assert approval.profile.runtime_kind == RUNTIME_KIND
    assert approval.profile.profile_kind == BYOA_PROFILE_WORK_MEMORY_REPORTER_V1
    assert approval.profile.status == "credential_ready"
    assert approval.profile.profile_ref == (
        f"admin_service/byoa_registration_requests/{REQUEST_ID}/connection-profile"
    )
    assert approval.profile.credential_id
    token = approval.credential_secret.reveal_token()
    assert approval.credential_secret.credential_id == approval.profile.credential_id
    assert token.startswith("cgac_")
    assert parse_agent_credential_token(token).credential_id == approval.profile.credential_id

    row = workflow.get_request(REQUEST_ID)
    assert row is not None
    assert row.status == BYOA_STATUS_REGISTERED
    assert row.provenance_kind == "admin_service.byoa_registration.v1"
    assert row.provenance_external_ref == REQUEST_ID
    assert row.provenance_payload_hash == approval.provenance_payload_hash

    events = workflow.list_events(REQUEST_ID)
    approval_events = [event for event in events if event.event_type == BYOA_STATUS_APPROVED]
    assert len(approval_events) == 1
    approval_record = approval_events[0].details["approval_record"]
    assert set(approval_record) == {
        "kind",
        "request_id",
        "project_id",
        "requested_agent_id",
        "approved_by",
        "policy_version",
        "admitted_spec_hash",
    }
    assert approval_record["request_id"] == REQUEST_ID
    assert approval_record["project_id"] == PROJECT_ID
    assert approval_record["requested_agent_id"] == AGENT_ID
    assert approval_record["approved_by"] == APPROVER_ID
    assert approval_events[0].details["approval_record_hash"] == canonical_json_sha256(approval_record)
    assert row.provenance_payload_hash == canonical_json_sha256(approval_record)

    snapshot = kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID))
    assert snapshot is not None
    assert snapshot.role == "external.agent.v1"
    assert snapshot.description == "Reports runtime-local work memory."
    assert snapshot.accepts_work is False
    assert snapshot.capacity == 1
    assert snapshot.capabilities == ()
    assert snapshot.grants == ()
    assert snapshot.public_metadata == {
        "ui": {"label": "BYOA Reporter"},
        "admin_service": {
            "byoa_request_id": REQUEST_ID,
            "runtime_kind": RUNTIME_KIND,
        },
    }
    assert snapshot.registered_by_agent_id == ADMIN_SERVICE_AGENT_ID
    assert snapshot.registration_provenance_kind == "admin_service.byoa_registration.v1"
    assert snapshot.registration_provenance_ref == REQUEST_ID
    assert snapshot.registration_provenance_payload_hash == approval.provenance_payload_hash
    assert snapshot.admitted_spec_hash == row.admitted_spec_hash


def test_conversation_worker_invite_profile_registers_claims_finishes_and_projects_offer(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    facade = AdminServiceByoaFacade(
        test_pg_dsn,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
        authorize_request=_allow_project,
        validate_invitation=_invite_validator(),
    )

    submitted = facade.submit_registration_request(
        _conversation_worker_request(),
        requester_user_id=REQUESTER_ID,
        creator_user_id="creator-user-001",
    )
    assert submitted.status == BYOA_STATUS_VALIDATED
    assert INVITATION_CODE not in repr(submitted.raw_request)

    approval = facade.approve_registration_request(REQUEST_ID, approved_by=REQUESTER_ID)

    assert approval.status == BYOA_STATUS_REGISTERED
    assert approval.profile.profile_kind == BYOA_PROFILE_CONVERSATION_WORKER_V1
    byoa_ref = AgentRef(project_id=PROJECT_ID, agent_id=CONVERSATION_AGENT_ID)
    token = approval.credential_secret.reveal_token()
    authed = test_client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/{CONVERSATION_AGENT_ID}",
        headers=agent_auth_headers(byoa_ref, token),
    )
    assert authed.status_code == 200

    row = ByoaWorkflowStore(test_pg_dsn).get_request(REQUEST_ID)
    assert row is not None
    assert row.status == BYOA_STATUS_REGISTERED
    assert INVITATION_CODE not in repr(row.raw_request)
    assert row.raw_request["requested_role"] == "external.conversation_worker.v1"
    assert row.raw_request["requested_capabilities"] == [TURN_KIND_CONVERSATION_V1]
    assert TURN_KIND_WORK_MEMORY_REPORT_V1 not in repr(row.raw_request)

    events = ByoaWorkflowStore(test_pg_dsn).list_events(REQUEST_ID)
    approval_record = [event for event in events if event.event_type == BYOA_STATUS_APPROVED][0].details["approval_record"]
    assert approval_record["approval_mode"] == BYOA_INVITE_CODE_APPROVAL_MODE
    assert approval_record["invite_id"] == INVITE_ID
    assert approval_record["issued_by_user_id"] == INVITE_ISSUER_ID
    assert approval_record["profile_kind"] == BYOA_PROFILE_CONVERSATION_WORKER_V1
    assert approval_record["approved_by"] == INVITE_ISSUER_ID
    assert INVITATION_CODE not in repr(approval_record)

    snapshot = kernel_app.topology.get_agent(byoa_ref)
    assert snapshot is not None
    assert snapshot.role == "external.conversation_worker.v1"
    assert snapshot.accepts_work is True
    assert snapshot.capacity == 1
    assert snapshot.capabilities == (TURN_KIND_CONVERSATION_V1,)
    assert snapshot.grants == ()
    assert snapshot.public_metadata["admin_service"] == {
        "byoa_request_id": REQUEST_ID,
        "runtime_kind": RUNTIME_KIND,
        "profile_kind": BYOA_PROFILE_CONVERSATION_WORKER_V1,
    }
    assert INVITATION_CODE not in repr(snapshot.public_metadata)
    assert INVITE_ID not in repr(snapshot.public_metadata)

    offers = list_turn_offer_entries(
        PostgresProjectionSource(test_pg_dsn),
        project_id=PROJECT_ID,
        filters=TurnOfferFilters(limit=100),
    )
    offer = next(item for item in offers.items if item.agent_id == CONVERSATION_AGENT_ID)
    assert offer.turn_kind == TURN_KIND_CONVERSATION_V1
    assert offer.calling["operation"] == "dispatch"

    admin_ref = AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)
    admin_client = HttpAgentClient(
        client=test_client,
        headers=agent_auth_headers(admin_ref, agent_token(admin_ref)),
    )
    worker_client = HttpAgentClient(
        client=test_client,
        headers=agent_auth_headers(byoa_ref, token),
    )
    turn = admin_client.dispatch(
        requested_by=admin_ref,
        target_agent=byoa_ref,
        input_payload={"task": "handle byoa conversation"},
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="byoa-conversation-root"),
        dispatch_key="byoa-conversation-root",
        turn_kind=TURN_KIND_CONVERSATION_V1,
    )
    claimed = worker_client.claim_turn(byoa_ref)
    assert claimed is not None
    assert claimed.claim.turn_ref() == turn
    assert claimed.context.semantic_items[0].content.payload() == {"task": "handle byoa conversation"}
    worker_client.finish_turn(
        claimed.claim,
        outcome=TurnOutcome.SUCCEEDED,
        final_payload={"result": "done"},
    )
    turn_snapshot = worker_client.get_turn(turn)
    assert turn_snapshot.state == TurnState.CLOSED
    assert turn_snapshot.outcome == TurnOutcome.SUCCEEDED
    assert turn_snapshot.final_payload == {"result": "done"}


@pytest.mark.parametrize(
    ("request_patch", "message"),
    (
        ({"invitation_code": "bad-code"}, "invalid"),
        ({"project_id": "wrong-project"}, "not valid for project"),
    ),
)
def test_conversation_worker_rejects_invalid_invite_before_workflow_or_cg(
    test_pg_dsn: str,
    request_patch: dict[str, Any],
    message: str,
) -> None:
    cg = FailingPostClient()
    facade = AdminServiceByoaFacade(
        test_pg_dsn,
        client=cg,
        authorize_request=lambda requester_user_id, project_id: requester_user_id == REQUESTER_ID,
        validate_invitation=_invite_validator(),
    )

    with pytest.raises(ForbiddenError, match=message):
        facade.submit_registration_request(
            _conversation_worker_request(**request_patch),
            requester_user_id=REQUESTER_ID,
        )

    assert _request_count(test_pg_dsn) == 0
    assert cg.calls == []


@pytest.mark.parametrize("invitation_code", (None, "bad-code"))
def test_conversation_worker_rejects_caller_supplied_invitation_before_workflow_or_cg(
    test_pg_dsn: str,
    invitation_code: str | None,
) -> None:
    cg = FailingPostClient()
    facade = AdminServiceByoaFacade(
        test_pg_dsn,
        client=cg,
        authorize_request=_allow_project,
        validate_invitation=_invite_validator(),
    )
    request = _conversation_worker_request(
        invitation={
            "invite_id": "caller-forged-invite",
            "issued_by_user_id": "caller-forged-issuer",
            "approval_mode": BYOA_INVITE_CODE_APPROVAL_MODE,
        },
    )
    if invitation_code is None:
        request.pop("invitation_code")
    else:
        request["invitation_code"] = invitation_code

    with pytest.raises(ConflictError, match="invitation_code"):
        facade.submit_registration_request(
            request,
            requester_user_id=REQUESTER_ID,
        )

    assert _request_count(test_pg_dsn) == 0
    assert cg.calls == []


def test_conversation_worker_rejects_expired_invite_before_workflow_or_cg(test_pg_dsn: str) -> None:
    cg = FailingPostClient()
    facade = AdminServiceByoaFacade(
        test_pg_dsn,
        client=cg,
        authorize_request=_allow_project,
        validate_invitation=ByoaJsonInviteValidator.from_config(
            {
                "invitations": [
                    {
                        "invite_id": INVITE_ID,
                        "project_id": PROJECT_ID,
                        "issued_by_user_id": INVITE_ISSUER_ID,
                        "issuer_role": "project_owner",
                        "allowed_profile_kinds": [BYOA_PROFILE_CONVERSATION_WORKER_V1],
                        "code": INVITATION_CODE,
                        "expires_at": "2025-01-01T00:00:00Z",
                    }
                ]
            },
            now=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        ),
    )

    with pytest.raises(ForbiddenError, match="expired"):
        facade.submit_registration_request(
            _conversation_worker_request(),
            requester_user_id=REQUESTER_ID,
        )

    assert _request_count(test_pg_dsn) == 0
    assert cg.calls == []


def test_conversation_worker_rejects_non_owner_invite_config_before_workflow_or_cg(test_pg_dsn: str) -> None:
    cg = FailingPostClient()

    with pytest.raises(ConflictError, match="issuer_role"):
        AdminServiceByoaFacade(
            test_pg_dsn,
            client=cg,
            authorize_request=_allow_project,
            validate_invitation=ByoaJsonInviteValidator.from_config(
                {
                    "invitations": [
                        {
                            "invite_id": INVITE_ID,
                            "project_id": PROJECT_ID,
                            "issued_by_user_id": INVITE_ISSUER_ID,
                            "issuer_role": "project_member",
                            "allowed_profile_kinds": [BYOA_PROFILE_CONVERSATION_WORKER_V1],
                            "code": INVITATION_CODE,
                        }
                    ]
                }
            ),
        )

    assert not _request_table_exists(test_pg_dsn)
    assert cg.calls == []


def test_conversation_worker_rejects_invite_for_wrong_profile_kind_before_workflow_or_cg(test_pg_dsn: str) -> None:
    cg = FailingPostClient()
    facade = AdminServiceByoaFacade(
        test_pg_dsn,
        client=cg,
        authorize_request=_allow_project,
        validate_invitation=ByoaJsonInviteValidator.from_config(
            {
                "invitations": [
                    {
                        "invite_id": INVITE_ID,
                        "project_id": PROJECT_ID,
                        "issued_by_user_id": INVITE_ISSUER_ID,
                        "issuer_role": "project_owner",
                        "allowed_profile_kinds": [BYOA_PROFILE_WORK_MEMORY_REPORTER_V1],
                        "code": INVITATION_CODE,
                    }
                ]
            }
        ),
    )

    with pytest.raises(ForbiddenError, match="profile_kind"):
        facade.submit_registration_request(
            _conversation_worker_request(),
            requester_user_id=REQUESTER_ID,
        )

    assert _request_count(test_pg_dsn) == 0
    assert cg.calls == []


def test_approval_result_omits_authority_and_keeps_credential_secret_out_of_repr(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    approval, _ = _approve_happy_path(test_pg_dsn, test_client, kernel_app)
    token = approval.credential_secret.reveal_token()

    result_data = asdict(approval)
    profile_data = asdict(approval.profile)

    assert _count_key(result_data, "token") == 0
    assert "token" not in profile_data
    assert "agent_credential_token" not in profile_data
    assert _unsafe_result_keys().isdisjoint(_nested_keys(result_data))
    assert _unsafe_result_keys().isdisjoint(_nested_keys(profile_data))
    assert token not in repr(approval)
    assert token not in repr(result_data)
    assert "token=<redacted>" in repr(approval.credential_secret)

    result_repr = repr(result_data)
    assert "agent.registration.birth" not in result_repr
    assert ADMIN_SERVICE_AGENT_ID not in result_repr
    assert "x-cg-caller" not in result_repr.lower()
    assert "credential_hash" not in result_repr


def test_approve_registered_request_can_reissue_connection_secret_for_recovery(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    first, _ = _approve_happy_path(test_pg_dsn, test_client, kernel_app)
    facade = AdminServiceByoaFacade(
        test_pg_dsn,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
        authorize_request=_allow_project,
    )

    second = facade.approve_registration_request(REQUEST_ID, approved_by=APPROVER_ID)

    assert second.status == BYOA_STATUS_REGISTERED
    assert second.profile.status == "credential_ready"
    assert second.profile.credential_id != first.profile.credential_id
    assert second.credential_secret.reveal_token().startswith("cgac_")


class FailingPostClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def post(self, *args, **kwargs):
        self.calls.append({"args": args, "kwargs": kwargs})
        raise AssertionError("CG should not be called")


def _allow_project(requester_user_id: str, project_id: str) -> bool:
    return requester_user_id == REQUESTER_ID and project_id == PROJECT_ID


def _approve_happy_path(test_pg_dsn: str, test_client, kernel_app):
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    facade = AdminServiceByoaFacade(
        test_pg_dsn,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
        authorize_request=_allow_project,
    )
    submitted = facade.submit_registration_request(
        _byoa_request(),
        requester_user_id=REQUESTER_ID,
        creator_user_id="creator-user-001",
    )
    assert submitted.status == BYOA_STATUS_VALIDATED

    approval = facade.approve_registration_request(REQUEST_ID, approved_by=APPROVER_ID)
    return approval, ByoaWorkflowStore(test_pg_dsn)


def _byoa_request(**overrides: Any) -> dict[str, Any]:
    request = {
        "request_id": REQUEST_ID,
        "project_id": PROJECT_ID,
        "requested_agent_id": AGENT_ID,
        "display_name": "BYOA Reporter",
        "description": "Reports runtime-local work memory.",
        "requested_role": "external.agent.v1",
        "requested_capabilities": [TURN_KIND_WORK_MEMORY_REPORT_V1],
        "runtime_kind": RUNTIME_KIND,
    }
    request.update(overrides)
    return request


def _conversation_worker_request(**overrides: Any) -> dict[str, Any]:
    request = _byoa_request(
        requested_agent_id=CONVERSATION_AGENT_ID,
        display_name="BYOA Conversation Worker",
        description="Handles externally hosted conversation work.",
        profile_kind=BYOA_PROFILE_CONVERSATION_WORKER_V1,
        invitation_code=INVITATION_CODE,
    )
    request.update(overrides)
    return request


def _invite_validator() -> ByoaJsonInviteValidator:
    return ByoaJsonInviteValidator.from_config(
        {
            "invitations": [
                {
                    "invite_id": INVITE_ID,
                    "project_id": PROJECT_ID,
                    "issued_by_user_id": INVITE_ISSUER_ID,
                    "issuer_role": "project_owner",
                    "allowed_profile_kinds": [BYOA_PROFILE_CONVERSATION_WORKER_V1],
                    "code": INVITATION_CODE,
                }
            ]
        }
    )


def _nested_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        keys = set(value)
        for item in value.values():
            keys.update(_nested_keys(item))
        return keys
    if isinstance(value, (list, tuple)):
        keys: set[str] = set()
        for item in value:
            keys.update(_nested_keys(item))
        return keys
    return set()


def _count_key(value: Any, target: str) -> int:
    if isinstance(value, dict):
        return sum(1 for key in value if key == target) + sum(_count_key(item, target) for item in value.values())
    if isinstance(value, (list, tuple)):
        return sum(_count_key(item, target) for item in value)
    return 0


def _unsafe_result_keys() -> set[str]:
    return {
        "binding_id",
        "credential_hash",
        "headers",
        "authority",
        "agent_birth_authority",
        "registration_credential",
        "raw_service_token",
        "service_token",
        "cg_authority",
        "dispatch_authority",
        "agent.registration.birth",
    }


def _request_count(pg_dsn: str) -> int:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from byoa_registration_requests")
        row = cur.fetchone()
    assert row is not None
    return row[0]


def _request_table_exists(pg_dsn: str) -> bool:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            select exists (
              select 1
              from information_schema.tables
              where table_name = 'byoa_registration_requests'
            )
            """
        )
        row = cur.fetchone()
    assert row is not None
    return bool(row[0])


def _drop_admin_service_tables(pg_dsn: str) -> None:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("drop table if exists byoa_registration_events")
        cur.execute("drop table if exists byoa_registration_requests")
