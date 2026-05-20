from __future__ import annotations

import psycopg
import pytest

from CommonGround.agent_client import HttpAgentClient
from CommonGround.agent_registration import AgentBirthSpec, AgentRegistrationProvenance
from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from Integrations.admin_service import (
    ADMIN_SERVICE_AGENT_ID,
    ADMIN_SERVICE_ROLE,
    BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
    BYOA_STATUS_FAILED,
    BYOA_STATUS_REGISTERED,
    BYOA_STATUS_REGISTERING,
    ByoaRegistrationProcessor,
    ByoaWorkflowStore,
    bootstrap_project_admin_service_agent,
)
from tests.auth_support import agent_headers, agent_token


PROJECT_ID = "admin-byoa-registration"
REQUEST_ID = "byoa-req-001"
REQUESTER_ID = "requester-001"
APPROVER_ID = "approver-001"
PROVENANCE_KIND = "test.byoa.approval.v1"
PROVENANCE_REF = "approval-001"
PROVENANCE_PAYLOAD_HASH = "sha256:approval-payload"


@pytest.fixture(autouse=True)
def fresh_byoa_workflow_tables(test_pg_dsn: str):
    _drop_byoa_tables(test_pg_dsn)
    yield
    _drop_byoa_tables(test_pg_dsn)


def test_process_registration_registers_approved_request_through_admin_service(test_pg_dsn, test_client, kernel_app) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    store = ByoaWorkflowStore(test_pg_dsn)
    approved = _approved_request(store, agent_id="byoa-agent-001")

    result = ByoaRegistrationProcessor(
        store,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
    ).process_registration(approved.request_id)

    assert result.status == BYOA_STATUS_REGISTERED
    assert result.attempt_count == 1
    assert result.registered_agent_id == "byoa-agent-001"
    assert result.registered_at is not None

    snapshot = kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id="byoa-agent-001"))
    assert snapshot is not None
    assert snapshot.registered_by_agent_id == ADMIN_SERVICE_AGENT_ID
    assert snapshot.registration_provenance_kind == PROVENANCE_KIND
    assert snapshot.registration_provenance_ref == PROVENANCE_REF
    assert snapshot.registration_provenance_payload_hash == PROVENANCE_PAYLOAD_HASH
    assert snapshot.admitted_spec_hash == approved.admitted_spec_hash

    events = store.list_events(approved.request_id)
    assert [event.to_status for event in events][-2:] == [BYOA_STATUS_REGISTERING, BYOA_STATUS_REGISTERED]
    assert events[-1].details["reconcile_existing"] is False


def test_process_next_registration_claims_and_registers_next_approved_request(
    test_pg_dsn,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    store = ByoaWorkflowStore(test_pg_dsn)
    approved = _approved_request(store, agent_id="byoa-agent-next")

    result = ByoaRegistrationProcessor(
        store,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
    ).process_next_registration()

    assert result is not None
    assert result.request_id == approved.request_id
    assert result.status == BYOA_STATUS_REGISTERED
    assert result.attempt_count == 1
    assert result.registered_agent_id == "byoa-agent-next"
    snapshot = kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id="byoa-agent-next"))
    assert snapshot is not None
    assert snapshot.registered_by_agent_id == ADMIN_SERVICE_AGENT_ID
    events = store.list_events(approved.request_id)
    assert [event.to_status for event in events][-2:] == [BYOA_STATUS_REGISTERING, BYOA_STATUS_REGISTERED]
    assert events[-2].details == {"processor": "byoa_registration"}


def test_process_registration_missing_admin_service_grant_marks_failed(test_pg_dsn, test_client, kernel_app) -> None:
    kernel_app.topology.register_agent(
        AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID),
        role=ADMIN_SERVICE_ROLE,
        accepts_work=False,
        grants=(),
        enabled=True,
    )
    store = ByoaWorkflowStore(test_pg_dsn)
    approved = _approved_request(store, agent_id="byoa-agent-no-grant")

    result = ByoaRegistrationProcessor(
        store,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
    ).process_registration(approved.request_id)

    assert result.status == BYOA_STATUS_FAILED
    assert result.attempt_count == 1
    assert result.last_error_code == "cg_http_409"
    assert "caller missing grant: agent.registration.birth" in result.last_error_message
    assert kernel_app.topology.get_agent(AgentRef(project_id=PROJECT_ID, agent_id="byoa-agent-no-grant")) is None

    events = store.list_events(approved.request_id)
    assert [event.to_status for event in events][-2:] == [BYOA_STATUS_REGISTERING, BYOA_STATUS_FAILED]
    assert events[-1].details["error_code"] == "cg_http_409"


def test_process_registration_reconciles_existing_agent_409_success(test_pg_dsn, test_client, kernel_app) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    store = ByoaWorkflowStore(test_pg_dsn)
    spec = _spec("byoa-agent-existing")
    approved = _approved_request(store, agent_id="byoa-agent-existing", spec=spec)
    _register_existing(test_client, spec=spec)

    result = ByoaRegistrationProcessor(
        store,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
    ).process_registration(approved.request_id)

    assert result.status == BYOA_STATUS_REGISTERED
    assert result.attempt_count == 1
    assert result.registered_agent_id == "byoa-agent-existing"

    events = store.list_events(approved.request_id)
    assert [event.to_status for event in events][-2:] == [BYOA_STATUS_REGISTERING, BYOA_STATUS_REGISTERED]
    assert events[-1].details["reconcile_existing"] is True


def test_process_registration_existing_agent_mismatch_requires_review(test_pg_dsn, test_client, kernel_app) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    store = ByoaWorkflowStore(test_pg_dsn)
    spec = _spec("byoa-agent-mismatch")
    approved = _approved_request(store, agent_id="byoa-agent-mismatch", spec=spec)
    _register_existing(
        test_client,
        spec=spec,
        provenance=AgentRegistrationProvenance(
            kind=PROVENANCE_KIND,
            external_ref="different-approval",
            payload_hash=PROVENANCE_PAYLOAD_HASH,
        ),
    )

    result = ByoaRegistrationProcessor(
        store,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
    ).process_registration(approved.request_id)

    assert result.status == BYOA_STATUS_CONFLICT_REQUIRES_REVIEW
    assert result.attempt_count == 1
    assert result.last_error_code == "existing_agent_mismatch"
    assert "registration_provenance_ref" in result.last_error_message

    events = store.list_events(approved.request_id)
    assert [event.to_status for event in events][-2:] == [
        BYOA_STATUS_REGISTERING,
        BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
    ]
    assert events[-1].details["reconcile_existing"] is True
    assert "registration_provenance_ref" in events[-1].details["mismatch_fields"]


def _approved_request(
    store: ByoaWorkflowStore,
    *,
    agent_id: str,
    spec: AgentBirthSpec | None = None,
):
    store.submit_request(
        REQUEST_ID,
        PROJECT_ID,
        agent_id,
        REQUESTER_ID,
        {"agent": {"agent_id": agent_id}, "request_id": REQUEST_ID},
    )
    store.validate_request(REQUEST_ID)
    return store.approve_request(
        REQUEST_ID,
        spec or _spec(agent_id),
        approved_by=APPROVER_ID,
        provenance_kind=PROVENANCE_KIND,
        provenance_external_ref=PROVENANCE_REF,
        provenance_payload_hash=PROVENANCE_PAYLOAD_HASH,
    )


def _spec(agent_id: str) -> AgentBirthSpec:
    return AgentBirthSpec(
        agent_id=agent_id,
        role="external.agent.v1",
        description="Approved BYOA agent",
        enabled=True,
        accepts_work=True,
        capacity=2,
        capabilities=(TURN_KIND_CONVERSATION_V1,),
        grants=("turn.dispatch.any",),
        public_metadata={"ui": {"label": "Approved BYOA Agent"}},
    )


def _register_existing(
    test_client,
    *,
    spec: AgentBirthSpec,
    provenance: AgentRegistrationProvenance | None = None,
) -> None:
    client = HttpAgentClient(
        client=test_client,
        headers=agent_headers(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
    )
    client.register_agent_by_service(
        project_id=PROJECT_ID,
        spec=spec,
        provenance=provenance
        or AgentRegistrationProvenance(
            kind=PROVENANCE_KIND,
            external_ref=PROVENANCE_REF,
            payload_hash=PROVENANCE_PAYLOAD_HASH,
        ),
    )


def _drop_byoa_tables(pg_dsn: str) -> None:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("drop table if exists byoa_registration_events")
        cur.execute("drop table if exists byoa_registration_requests")
