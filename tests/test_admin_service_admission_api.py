from __future__ import annotations

from typing import Any

import psycopg
import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from psycopg.rows import dict_row

from CommonGround.agent_client import agent_auth_headers
from CommonGround.agent_credentials import parse_agent_credential_token
from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from Integrations.admin_service import (
    ADMIN_SERVICE_AGENT_ID,
    AgentJoinInviteStore,
    BYOA_PROFILE_CONVERSATION_WORKER_V1,
    BYOA_PROFILE_WORK_MEMORY_REPORTER_V1,
    AdminServiceByoaFacade,
    ByoaRegistrationProcessor,
    ByoaJsonInviteValidator,
    ByoaWorkflowStore,
    bootstrap_project_admin_service_agent,
    create_agent_credential_token_request_app,
)
from tests.auth_support import agent_token


PROJECT_ID = "admin-admission-api"
REQUEST_ID = "admission-token-req-001"
REQUESTER_ID = "requester-user-001"
AGENT_ID = "external-agent-001"
CONVERSATION_AGENT_ID = "external-conversation-worker-001"
RUNTIME_KIND = "test.external.runtime.v1"
INVITATION_CODE = "admission-invite-code-001"
INVITE_ID = "admission-invite-001"
INVITE_ISSUER_ID = "invite-issuer-user-001"


@pytest.fixture(autouse=True)
def fresh_admin_service_tables(test_pg_dsn: str):
    _drop_admin_service_tables(test_pg_dsn)
    yield
    _drop_admin_service_tables(test_pg_dsn)


def test_request_agent_credential_token_registers_agent_and_returns_token(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client)

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["request_id"] == REQUEST_ID
    assert body["project_id"] == PROJECT_ID
    assert body["agent_id"] == AGENT_ID
    assert body["status"] == "registered"
    assert body["profile"] == {
        "project_id": PROJECT_ID,
        "agent_id": AGENT_ID,
        "runtime_kind": RUNTIME_KIND,
        "profile_kind": BYOA_PROFILE_WORK_MEMORY_REPORTER_V1,
        "profile_ref": f"admin_service/byoa_registration_requests/{REQUEST_ID}/connection-profile",
        "credential_id": body["credential"]["credential_id"],
        "status": "credential_ready",
    }
    assert "agent_credential_token" not in body["profile"]
    token = body["agent_credential_token"]
    assert parse_agent_credential_token(token).credential_id == body["credential"]["credential_id"]

    agent_ref = AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)
    authed = test_client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/{AGENT_ID}",
        headers=agent_auth_headers(agent_ref, token),
    )
    assert authed.status_code == 200
    assert authed.json()["agent"]["agent_id"] == AGENT_ID


def test_request_agent_credential_token_accepts_conversation_worker_profile_and_invite(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client, validate_invitation=_invite_validator())

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(
            request_id="admission-token-conversation-001",
            requested_agent_id=CONVERSATION_AGENT_ID,
            profile_kind=BYOA_PROFILE_CONVERSATION_WORKER_V1,
            invitation_code=INVITATION_CODE,
        ),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["profile"]["profile_kind"] == BYOA_PROFILE_CONVERSATION_WORKER_V1
    assert "agent_credential_token" not in body["profile"]
    token = body["agent_credential_token"]
    agent_ref = AgentRef(project_id=PROJECT_ID, agent_id=CONVERSATION_AGENT_ID)
    authed = test_client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/{CONVERSATION_AGENT_ID}",
        headers=agent_auth_headers(agent_ref, token),
    )
    assert authed.status_code == 200
    assert authed.json()["role"] == "external.conversation_worker.v1"

    snapshot = kernel_app.topology.get_agent(agent_ref)
    assert snapshot is not None
    assert snapshot.accepts_work is True
    assert snapshot.capabilities == (TURN_KIND_CONVERSATION_V1,)
    assert snapshot.grants == ()
    assert "invite_id" not in snapshot.public_metadata["admin_service"]
    assert INVITATION_CODE not in repr(snapshot.public_metadata)
    assert INVITE_ID not in repr(snapshot.public_metadata)


def test_agent_join_invite_create_and_redeem_without_admin_bearer(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client, join_invite_store=AgentJoinInviteStore(test_pg_dsn))

    created = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-join-invites",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json={"agent_id": CONVERSATION_AGENT_ID},
    )

    assert created.status_code == 200
    created_body = created.json()
    join_code = created_body["join_code"]
    assert join_code.startswith("cgjoin_")
    assert created_body["invite"]["agent_id"] == CONVERSATION_AGENT_ID
    assert created_body["invite"]["profile_kind"] == BYOA_PROFILE_CONVERSATION_WORKER_V1
    assert join_code not in repr(_join_invite_rows(test_pg_dsn))
    invite_id = created_body["invite"]["invite_id"]

    redeemed = admin_client.post(
        "/admin/v1/agent-joins:redeem",
        json={"join_code": join_code},
    )

    assert redeemed.status_code == 200
    redeemed_body = redeemed.json()
    assert redeemed_body["project_id"] == PROJECT_ID
    assert redeemed_body["agent_id"] == CONVERSATION_AGENT_ID
    assert redeemed_body["profile"]["profile_kind"] == BYOA_PROFILE_CONVERSATION_WORKER_V1
    assert "agent_credential_token" not in redeemed_body["profile"]
    assert redeemed_body["request_id"].startswith("agjoinreq_")
    assert invite_id not in redeemed_body["request_id"]
    token = redeemed_body["agent_credential_token"]
    agent_ref = AgentRef(project_id=PROJECT_ID, agent_id=CONVERSATION_AGENT_ID)
    authed = test_client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/{CONVERSATION_AGENT_ID}",
        headers=agent_auth_headers(agent_ref, token),
    )
    assert authed.status_code == 200
    assert authed.json()["role"] == "external.conversation_worker.v1"
    snapshot = kernel_app.topology.get_agent(agent_ref)
    assert snapshot is not None
    assert invite_id not in repr(snapshot)
    assert join_code not in repr(snapshot)
    assert invite_id not in repr(snapshot.public_metadata)
    assert "aginv_" not in repr(snapshot.public_metadata)
    assert "aginv_" not in repr(snapshot.registration_provenance_ref)

    second = admin_client.post(
        "/admin/v1/agent-joins:redeem",
        json={"join_code": join_code},
    )
    assert second.status_code == 403
    assert second.json()["code"] == "join_code_used"


def test_agent_join_invite_create_requires_authorized_product_actor(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client, join_invite_store=AgentJoinInviteStore(test_pg_dsn))

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-join-invites",
        headers={"X-Product-User-Id": "not-allowed"},
        json={"agent_id": CONVERSATION_AGENT_ID},
    )

    assert response.status_code == 403
    assert response.json()["message"] == "BYOA registration requester is not authorized for project"


def test_request_agent_credential_token_reports_missing_conversation_worker_invite(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client, validate_invitation=_invite_validator())

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(
            request_id="admission-token-conversation-missing-invite",
            requested_agent_id=CONVERSATION_AGENT_ID,
            profile_kind=BYOA_PROFILE_CONVERSATION_WORKER_V1,
        ),
    )

    assert response.status_code == 403
    body = response.json()
    assert body["code"] == "invitation_code_required"
    assert "invitation_code" in body["message"]


def test_request_agent_credential_token_reports_invalid_conversation_worker_invite(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client, validate_invitation=_invite_validator())

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(
            request_id="admission-token-conversation-invalid-invite",
            requested_agent_id=CONVERSATION_AGENT_ID,
            profile_kind=BYOA_PROFILE_CONVERSATION_WORKER_V1,
            invitation_code="bad-invite-secret",
        ),
    )

    assert response.status_code == 403
    body = response.json()
    assert body["code"] == "invitation_code_invalid"
    assert "bad-invite-secret" not in repr(body)


def test_request_agent_credential_token_reports_missing_conversation_worker_invite_validator(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client)

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(
            request_id="admission-token-conversation-missing-validator",
            requested_agent_id=CONVERSATION_AGENT_ID,
            profile_kind=BYOA_PROFILE_CONVERSATION_WORKER_V1,
            invitation_code=INVITATION_CODE,
        ),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "invitation_validator_required"
    assert "invitation validator" in body["message"]


def test_request_agent_credential_token_requires_product_actor(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client)

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        json=_token_request_body(),
    )

    assert response.status_code == 401
    assert response.json()["message"] == "requester_user_id is required"
    assert _request_count(test_pg_dsn) == 0


def test_request_agent_credential_token_reports_unseeded_project(test_pg_dsn: str, test_client) -> None:
    admin_client = _admin_client_with_token(test_pg_dsn, test_client, admin_service_token="cgac_cred_missing.secret")

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(),
    )

    assert response.status_code == 404
    body = response.json()
    assert body["code"] == "project_not_seeded"
    assert body["message"] == f"project is not seeded: {PROJECT_ID}"
    assert _request_count(test_pg_dsn) == 0


def test_request_agent_credential_token_reports_missing_admin_service_credential(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client_with_token(test_pg_dsn, test_client, admin_service_token=None)

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "admin_service_credential_required"
    assert body["message"] == "admin-service AgentCredential is required"
    assert _request_count(test_pg_dsn) == 0


def test_request_agent_credential_token_requires_current_admin_service_token_even_when_db_has_active_credential(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID))
    admin_client = _admin_client_with_token(test_pg_dsn, test_client, admin_service_token=None)

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(request_id="admission-token-current-token-required"),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "admin_service_credential_required"
    assert body["message"] == "admin-service AgentCredential is required"
    assert _request_count(test_pg_dsn) == 0


def test_request_agent_credential_token_reports_project_bootstrap_conflict(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    admin_service = AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)
    kernel_app.topology.register_agent(
        admin_service,
        role="custom.admin.service",
        capabilities=("custom.capability",),
        accepts_work=True,
        grants=(),
        enabled=True,
    )
    admin_client = _admin_client_with_token(
        test_pg_dsn,
        test_client,
        admin_service_token=agent_token(admin_service),
    )

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": REQUESTER_ID},
        json=_token_request_body(request_id="admission-token-bootstrap-conflict"),
    )

    assert response.status_code == 409
    body = response.json()
    assert body["code"] == "project_bootstrap_conflict"
    assert "project admin-service bootstrap conflict" in body["message"]
    assert _request_count(test_pg_dsn) == 0


def test_request_agent_credential_token_rejects_unauthorized_product_actor(
    test_pg_dsn: str,
    test_client,
    kernel_app,
) -> None:
    bootstrap_project_admin_service_agent(kernel_app, project_id=PROJECT_ID, creator_ref="creator-001")
    admin_client = _admin_client(test_pg_dsn, test_client)

    response = admin_client.post(
        f"/admin/v1/projects/{PROJECT_ID}/agent-credential-tokens:request",
        headers={"X-Product-User-Id": "not-allowed"},
        json=_token_request_body(),
    )

    assert response.status_code == 403
    assert response.json()["message"] == "BYOA registration requester is not authorized for project"
    assert _request_count(test_pg_dsn) == 0


def _admin_client(test_pg_dsn: str, cg_client, *, validate_invitation=None, join_invite_store=None) -> TestClient:
    return _admin_client_with_token(
        test_pg_dsn,
        cg_client,
        admin_service_token=agent_token(AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)),
        validate_invitation=validate_invitation,
        join_invite_store=join_invite_store,
    )


def _admin_client_with_token(test_pg_dsn: str, cg_client, *, admin_service_token: str | None, validate_invitation=None, join_invite_store=None) -> TestClient:
    workflow = ByoaWorkflowStore(test_pg_dsn)
    processor = ByoaRegistrationProcessor(
        workflow,
        client=cg_client,
        admin_service_token=admin_service_token,
    )
    facade = AdminServiceByoaFacade(
        workflow,
        processor,
        authorize_request=_allow_project,
        validate_invitation=validate_invitation,
    )
    return TestClient(
        create_agent_credential_token_request_app(
            facade,
            resolve_requester_user_id=_requester_from_header,
            join_invite_store=join_invite_store,
        )
    )


def _requester_from_header(request: Request) -> str:
    return request.headers.get("X-Product-User-Id", "")


def _allow_project(requester_user_id: str, project_id: str) -> bool:
    return requester_user_id == REQUESTER_ID and project_id == PROJECT_ID


def _token_request_body(**overrides: Any) -> dict[str, Any]:
    body = {
        "request_id": REQUEST_ID,
        "requested_agent_id": AGENT_ID,
        "display_name": "External Agent",
        "description": "External runtime for project automation.",
        "runtime_kind": RUNTIME_KIND,
    }
    body.update(overrides)
    return body


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


def _request_count(pg_dsn: str) -> int:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from byoa_registration_requests")
        row = cur.fetchone()
    assert row is not None
    return row[0]


def _join_invite_rows(pg_dsn: str) -> list[dict[str, Any]]:
    with psycopg.connect(pg_dsn, row_factory=dict_row) as conn, conn.cursor() as cur:
        cur.execute("select * from agent_join_invites")
        return list(cur.fetchall())


def _drop_admin_service_tables(pg_dsn: str) -> None:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute("drop table if exists agent_join_invites")
        cur.execute("drop table if exists byoa_registration_events")
        cur.execute("drop table if exists byoa_registration_requests")
