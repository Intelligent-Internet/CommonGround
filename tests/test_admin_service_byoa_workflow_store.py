from __future__ import annotations

import psycopg
import pytest

from CommonGround.agent_registration import AgentBirthSpec, agent_birth_spec_hash, canonical_agent_birth_spec
from CommonGround.contracts import ConflictError
from Integrations.admin_service import (
    BYOA_STATUS_APPROVED,
    BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
    BYOA_STATUS_FAILED,
    BYOA_STATUS_REGISTERED,
    BYOA_STATUS_REGISTERING,
    BYOA_STATUS_SUBMITTED,
    BYOA_STATUS_VALIDATED,
    ByoaWorkflowStore,
    canonical_raw_request_hash,
    ensure_byoa_workflow_schema,
)


@pytest.fixture(autouse=True)
def fresh_byoa_workflow_tables(test_pg_dsn: str):
    _drop_byoa_tables(test_pg_dsn)
    yield
    _drop_byoa_tables(test_pg_dsn)


def test_schema_creation_and_submit_create_request_row_and_submitted_event(test_pg_dsn: str) -> None:
    ensure_byoa_workflow_schema(test_pg_dsn)
    store = ByoaWorkflowStore(test_pg_dsn, ensure_schema=False)

    row = store.submit_request(
        "req-001",
        "project-001",
        "agent-001",
        "requester-001",
        {"b": 2, "a": {"z": 1}},
        actor_id="requester-001",
    )

    assert row.status == BYOA_STATUS_SUBMITTED
    assert row.raw_request_hash == canonical_raw_request_hash({"a": {"z": 1}, "b": 2})
    assert row.attempt_count == 0
    assert _request_columns(test_pg_dsn) >= {"provenance_external_ref"}
    assert "provenance_ref" not in _request_columns(test_pg_dsn)
    assert _request_count(test_pg_dsn) == 1
    events = store.list_events("req-001")
    assert len(events) == 1
    assert events[0].event_type == BYOA_STATUS_SUBMITTED
    assert events[0].from_status is None
    assert events[0].to_status == BYOA_STATUS_SUBMITTED
    assert events[0].details == {"raw_request_hash": row.raw_request_hash}


def test_same_request_id_same_raw_request_is_idempotent(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    raw_request = {"agent": {"id": "agent-001"}, "labels": ["one", "two"]}

    first = store.submit_request("req-001", "project-001", "agent-001", "requester-001", raw_request)
    second = store.submit_request("req-001", "project-001", "agent-001", "requester-001", raw_request)

    assert second == first
    assert _request_count(test_pg_dsn) == 1
    assert len(store.list_events("req-001")) == 1


def test_same_request_id_different_raw_request_raises_conflict(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "agent-001"})

    with pytest.raises(ConflictError, match="idempotency conflict"):
        store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "changed"})

    assert _request_count(test_pg_dsn) == 1


def test_same_project_requested_agent_id_different_request_id_raises_conflict(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "agent-001"})

    with pytest.raises(ConflictError, match="project/agent"):
        store.submit_request("req-002", "project-001", "agent-001", "requester-002", {"agent": "agent-001"})

    assert _request_count(test_pg_dsn) == 1


def test_valid_transitions_append_events(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "agent-001"})

    validated = store.transition_request(
        "req-001",
        from_statuses=(BYOA_STATUS_SUBMITTED,),
        to_status=BYOA_STATUS_VALIDATED,
        actor_kind="admin_service",
        actor_id="admin-service",
    )
    approved = store.transition_request(
        "req-001",
        from_statuses=(BYOA_STATUS_VALIDATED,),
        to_status=BYOA_STATUS_APPROVED,
        actor_kind="user",
        actor_id="approver-001",
    )
    registering = store.transition_request(
        "req-001",
        from_statuses=(BYOA_STATUS_APPROVED,),
        to_status=BYOA_STATUS_REGISTERING,
        actor_kind="admin_service",
        actor_id="admin-service",
        increment_attempt_count=True,
    )
    failed = store.transition_request(
        "req-001",
        from_statuses=(BYOA_STATUS_REGISTERING,),
        to_status=BYOA_STATUS_FAILED,
        actor_kind="admin_service",
        actor_id="admin-service",
        last_error_code="cg_unavailable",
        last_error_message="temporary failure",
    )

    assert validated.status == BYOA_STATUS_VALIDATED
    assert approved.status == BYOA_STATUS_APPROVED
    assert registering.status == BYOA_STATUS_REGISTERING
    assert registering.attempt_count == 1
    assert failed.status == BYOA_STATUS_FAILED
    assert failed.last_error_code == "cg_unavailable"
    assert [event.to_status for event in store.list_events("req-001")] == [
        BYOA_STATUS_SUBMITTED,
        BYOA_STATUS_VALIDATED,
        BYOA_STATUS_APPROVED,
        BYOA_STATUS_REGISTERING,
        BYOA_STATUS_FAILED,
    ]


def test_invalid_transition_raises_conflict_and_preserves_status(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "agent-001"})

    with pytest.raises(ConflictError, match="invalid BYOA transition"):
        store.transition_request(
            "req-001",
            from_statuses=(BYOA_STATUS_SUBMITTED,),
            to_status=BYOA_STATUS_REGISTERING,
            actor_kind="admin_service",
            actor_id="admin-service",
        )

    row = store.get_request("req-001")
    assert row is not None
    assert row.status == BYOA_STATUS_SUBMITTED
    assert len(store.list_events("req-001")) == 1


def test_approve_stores_admitted_spec_json_and_hash(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "agent-001"})
    store.validate_request("req-001")
    spec = AgentBirthSpec(
        agent_id="agent-001",
        role="external.agent.v1",
        capabilities=("turn.conversation.v1",),
        grants=("turn.dispatch.any",),
        description="Approved BYOA agent",
        public_metadata={"ui": {"label": "Approved Agent"}},
    )

    approved = store.approve_request(
        "req-001",
        spec,
        approved_by="approver-001",
        provenance_kind="test.byoa.approval.v1",
        provenance_external_ref="approval-001",
        provenance_payload_hash="sha256:approval-payload",
    )

    assert approved.status == BYOA_STATUS_APPROVED
    assert approved.admitted_spec == {
        "agent_id": "agent-001",
        "role": "external.agent.v1",
        "description": "Approved BYOA agent",
        "enabled": True,
        "accepts_work": True,
        "capacity": 1,
        "capabilities": ["turn.conversation.v1"],
        "grants": ["turn.dispatch.any"],
        "public_metadata": {"ui": {"label": "Approved Agent"}},
    }
    assert approved.admitted_spec_hash == agent_birth_spec_hash(canonical_agent_birth_spec(spec))
    assert approved.provenance_kind == "test.byoa.approval.v1"
    assert approved.provenance_external_ref == "approval-001"
    assert approved.provenance_payload_hash == "sha256:approval-payload"


def test_approve_mapping_spec_uses_cg_canonical_hash_and_json(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "agent-001"})
    store.validate_request("req-001")
    spec = {
        "agent_id": "agent-001",
        "role": "external.agent.v1",
        "capabilities": ["z.capability", "a.capability", "z.capability"],
        "grants": ["z.grant", "a.grant", "z.grant"],
        "public_metadata": {"ui": {"label": "Approved Agent"}},
    }
    expected_spec = canonical_agent_birth_spec(
        AgentBirthSpec(
            agent_id="agent-001",
            role="external.agent.v1",
            capabilities=("z.capability", "a.capability", "z.capability"),
            grants=("z.grant", "a.grant", "z.grant"),
            public_metadata={"ui": {"label": "Approved Agent"}},
        )
    )

    approved = store.approve_request(
        "req-001",
        spec,
        approved_by="approver-001",
        provenance_external_ref="approval-001",
    )

    assert approved.admitted_spec == {
        "agent_id": "agent-001",
        "role": "external.agent.v1",
        "description": None,
        "enabled": True,
        "accepts_work": True,
        "capacity": 1,
        "capabilities": ["a.capability", "z.capability"],
        "grants": ["a.grant", "z.grant"],
        "public_metadata": {"ui": {"label": "Approved Agent"}},
    }
    assert approved.admitted_spec_hash == agent_birth_spec_hash(expected_spec)


def test_approve_request_allows_conflict_requires_review_reapproval(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    store.submit_request("req-001", "project-001", "agent-001", "requester-001", {"agent": "agent-001"})
    store.validate_request("req-001")
    spec = AgentBirthSpec(
        agent_id="agent-001",
        role="external.agent.v1",
        capabilities=("turn.conversation.v1",),
        grants=("turn.dispatch.any",),
    )
    store.approve_request("req-001", spec, approved_by="approver-001")
    store.transition_request(
        "req-001",
        from_statuses=(BYOA_STATUS_APPROVED,),
        to_status=BYOA_STATUS_REGISTERING,
        actor_kind="admin_service",
        actor_id="admin-service",
    )
    store.transition_request(
        "req-001",
        from_statuses=(BYOA_STATUS_REGISTERING,),
        to_status=BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
        actor_kind="admin_service",
        actor_id="admin-service",
        last_error_code="hash_mismatch",
        last_error_message="CG admitted spec hash mismatch",
    )

    reapproved = store.approve_request(
        "req-001",
        spec,
        approved_by="approver-002",
        provenance_external_ref="approval-002",
    )

    assert reapproved.status == BYOA_STATUS_APPROVED
    assert reapproved.provenance_external_ref == "approval-002"
    assert reapproved.last_error_code is None
    assert reapproved.last_error_message is None


def test_claim_next_approved_for_registration_claims_and_appends_event(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    approved = _approved_request(store, "req-001", "agent-001")

    claimed = store.claim_next_approved_for_registration(details={"worker": "pg-worker"})

    assert claimed is not None
    assert claimed.request_id == approved.request_id
    assert claimed.status == BYOA_STATUS_REGISTERING
    assert claimed.attempt_count == 1
    events = store.list_events("req-001")
    assert [event.to_status for event in events][-2:] == [BYOA_STATUS_APPROVED, BYOA_STATUS_REGISTERING]
    assert events[-1].event_type == BYOA_STATUS_REGISTERING
    assert events[-1].actor_kind == "admin_service"
    assert events[-1].actor_id == "admin-service"
    assert events[-1].details == {"worker": "pg-worker"}
    assert store.claim_next_approved_for_registration() is None


def test_claim_next_approved_for_registration_skips_non_approved_requests(test_pg_dsn: str) -> None:
    store = ByoaWorkflowStore(test_pg_dsn)
    _approved_request(store, "req-registering", "agent-registering")
    _approved_request(store, "req-registered", "agent-registered")
    approved = _approved_request(store, "req-approved", "agent-approved")
    store.mark_registering("req-registering")
    store.mark_registering("req-registered")
    store.mark_registered("req-registered", registered_agent_id="agent-registered")

    claimed = store.claim_next_approved_for_registration()

    assert claimed is not None
    assert claimed.request_id == approved.request_id
    assert claimed.status == BYOA_STATUS_REGISTERING
    assert store.get_request("req-registering").status == BYOA_STATUS_REGISTERING
    assert store.get_request("req-registered").status == BYOA_STATUS_REGISTERED
    assert store.claim_next_approved_for_registration() is None


def _approved_request(
    store: ByoaWorkflowStore,
    request_id: str,
    agent_id: str,
):
    store.submit_request(
        request_id,
        f"project-{request_id}",
        agent_id,
        "requester-001",
        {"agent": {"agent_id": agent_id}, "request_id": request_id},
    )
    store.validate_request(request_id)
    return store.approve_request(
        request_id,
        AgentBirthSpec(
            agent_id=agent_id,
            role="external.agent.v1",
            capabilities=("turn.conversation.v1",),
            grants=("turn.dispatch.any",),
        ),
        approved_by="approver-001",
        provenance_external_ref=f"approval-{request_id}",
    )


def _drop_byoa_tables(pg_dsn: str) -> None:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("drop table if exists byoa_registration_events")
        cur.execute("drop table if exists byoa_registration_requests")


def _request_count(pg_dsn: str) -> int:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from byoa_registration_requests")
        return int(cur.fetchone()[0])


def _request_columns(pg_dsn: str) -> set[str]:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_name = 'byoa_registration_requests'
            """
        )
        return {row[0] for row in cur.fetchall()}
