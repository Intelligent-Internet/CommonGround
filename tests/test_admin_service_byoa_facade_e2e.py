from __future__ import annotations

from dataclasses import asdict
from typing import Any

import psycopg
import pytest

from CommonGround.agent_credentials import parse_agent_credential_token
from CommonGround.contracts import (
    AgentRef,
    ConflictError,
    ForbiddenError,
    TURN_KIND_CONVERSATION_V1,
    TURN_KIND_WORK_MEMORY_REPORT_V1,
)
from Integrations.admin_service import (
    ADMIN_SERVICE_AGENT_ID,
    BYOA_PROFILE_WORK_MEMORY_REPORTER_V1,
    BYOA_STATUS_REGISTERED,
    BYOA_STATUS_VALIDATED,
    AdminServiceByoaFacade,
    ByoaRegistrationProcessor,
    ByoaWorkflowStore,
    bootstrap_project_admin_service_agent,
)
from tests.auth_support import agent_token


PROJECT_ID = "admin-byoa-facade"
REQUEST_ID = "byoa-facade-req-001"
REQUESTER_ID = "requester-user-001"
APPROVER_ID = "approver-user-001"
AGENT_ID = "byoa-reporting-agent"
RUNTIME_KIND = "test.external.runtime.v1"


@pytest.fixture(autouse=True)
def fresh_admin_service_tables(test_pg_dsn: str):
    _drop_admin_service_tables(test_pg_dsn)
    yield
    _drop_admin_service_tables(test_pg_dsn)


def test_submit_rejects_unknown_grants_field_before_row_or_cg(test_pg_dsn: str) -> None:
    failing_client = FailingPostClient()
    facade, _ = _facade(test_pg_dsn, failing_client)
    request = _byoa_request(grants=["agent.registration.birth"])

    with pytest.raises(ConflictError, match="unsupported BYOA registration request field.*grants"):
        facade.submit_registration_request(request, requester_user_id=REQUESTER_ID)

    assert _request_count(test_pg_dsn) == 0
    assert failing_client.calls == []


@pytest.mark.parametrize(
    ("request_patch", "message"),
    (
        ({"requested_role": "service.admin.v1"}, "unsupported BYOA requested_role"),
        ({"requested_capabilities": [TURN_KIND_CONVERSATION_V1]}, "unsupported BYOA requested_capability"),
    ),
)
def test_approve_rejects_non_allowlisted_role_and_capability_before_cg_registration(
    test_pg_dsn: str,
    kernel_app,
    request_patch: dict[str, Any],
    message: str,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_service_token = agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID))
    failing_client = FailingPostClient()
    facade, workflow = _facade(test_pg_dsn, failing_client, admin_service_token=admin_service_token)

    submitted = facade.submit_registration_request(_byoa_request(**request_patch), requester_user_id=REQUESTER_ID)

    assert submitted.status == BYOA_STATUS_VALIDATED
    with pytest.raises(ConflictError, match=message):
        facade.approve_registration_request(REQUEST_ID, approved_by=APPROVER_ID)

    row = workflow.get_request(REQUEST_ID)
    assert row is not None
    assert row.status == BYOA_STATUS_VALIDATED
    assert row.registered_agent_id is None
    assert kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)) is None
    assert failing_client.calls == []


def test_submit_rejects_unauthorized_product_user_before_row_or_cg(test_pg_dsn: str) -> None:
    failing_client = FailingPostClient()
    authorize_calls: list[tuple[str, str]] = []

    def deny_request(requester_user_id: str, project_id: str) -> bool:
        authorize_calls.append((requester_user_id, project_id))
        return False

    facade, _ = _facade(test_pg_dsn, failing_client, authorize_request=deny_request)

    with pytest.raises(ForbiddenError, match="not authorized"):
        facade.submit_registration_request(_byoa_request(), requester_user_id=REQUESTER_ID)

    assert authorize_calls == [(REQUESTER_ID, PROJECT_ID)]
    assert _request_count(test_pg_dsn) == 0
    assert failing_client.calls == []


def test_byoa_facade_happy_path_registers_agent_and_returns_direct_profile(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    facade, workflow = _facade(
        test_pg_dsn,
        test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
    )

    submitted = facade.submit_registration_request(
        _byoa_request(),
        requester_user_id=REQUESTER_ID,
        creator_user_id="creator-user-001",
    )

    assert submitted.status == BYOA_STATUS_VALIDATED
    byoa_ref = AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)
    assert kernel_app.topology.get_agent(byoa_ref) is None

    approval = facade.approve_registration_request(REQUEST_ID, approved_by=APPROVER_ID)

    assert approval.status == BYOA_STATUS_REGISTERED
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
    assert parse_agent_credential_token(token).credential_id == approval.profile.credential_id

    approval_data = asdict(approval)
    profile_data = asdict(approval.profile)
    assert _count_key(approval_data, "token") == 0
    assert _unsafe_result_keys().isdisjoint(_nested_keys(approval_data))
    assert _unsafe_result_keys().isdisjoint(_nested_keys(profile_data))
    assert "agent_credential_token" not in profile_data
    assert token.startswith("cgac_")
    assert token not in repr(approval)
    assert token not in repr(approval_data)
    approval_repr = repr(approval_data)
    assert "agent.registration.birth" not in approval_repr
    assert ADMIN_SERVICE_AGENT_ID not in approval_repr
    assert "x-cg-caller" not in approval_repr.lower()
    assert "credential_hash" not in approval_repr
    assert "gateway" not in approval_repr.lower()

    row = workflow.get_request(REQUEST_ID)
    assert row is not None
    assert row.status == BYOA_STATUS_REGISTERED
    assert row.provenance_kind == "admin_service.byoa_registration.v1"
    assert row.provenance_external_ref == REQUEST_ID
    assert row.provenance_payload_hash == approval.provenance_payload_hash

    snapshot = kernel_app.topology.get_agent(byoa_ref)
    assert snapshot is not None
    assert snapshot.role == "external.agent.v1"
    assert snapshot.description == "Reports runtime-local work memory."
    assert snapshot.enabled is True
    assert snapshot.accepts_work is False
    assert snapshot.capacity == 1
    assert snapshot.capabilities == ()
    assert snapshot.grants == ()
    assert snapshot.public_metadata == {
        "admin_service": {
            "byoa_request_id": REQUEST_ID,
            "runtime_kind": RUNTIME_KIND,
        },
        "ui": {"label": "BYOA Reporter"},
    }
    assert snapshot.registered_by_agent_id == ADMIN_SERVICE_AGENT_ID
    assert snapshot.registration_provenance_kind == "admin_service.byoa_registration.v1"
    assert snapshot.registration_provenance_ref == REQUEST_ID
    assert snapshot.registration_provenance_payload_hash == approval.provenance_payload_hash
    assert snapshot.admitted_spec_hash == row.admitted_spec_hash


class FailingPostClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def post(self, *args, **kwargs):
        self.calls.append({"args": args, "kwargs": kwargs})
        raise AssertionError("CG should not be called")


def _facade(test_pg_dsn: str, client, *, authorize_request=None, admin_service_token: str | None = None):
    workflow = ByoaWorkflowStore(test_pg_dsn)
    processor = ByoaRegistrationProcessor(
        workflow,
        client=client,
        admin_service_token=admin_service_token,
    )
    if authorize_request is None:
        authorize_request = _allow_project
    return (
        AdminServiceByoaFacade(
            workflow,
            processor,
            authorize_request=authorize_request,
        ),
        workflow,
    )


def _allow_project(requester_user_id: str, project_id: str) -> bool:
    return requester_user_id == REQUESTER_ID and project_id == PROJECT_ID


def _byoa_request(**overrides) -> dict[str, Any]:
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


def _drop_admin_service_tables(pg_dsn: str) -> None:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("drop table if exists byoa_registration_events")
        cur.execute("drop table if exists byoa_registration_requests")
