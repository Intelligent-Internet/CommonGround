from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import Request
from fastapi.testclient import TestClient
import pytest

from CommonGround.agent_credentials import format_agent_credential_token
from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from CommonGround.infra import PostgresAgentCredentialStore
from CommonGround.service.auth import authenticate_agent_request


PROJECT_ID = "agent-auth-project"
AGENT = AgentRef(project_id=PROJECT_ID, agent_id="caller")
OTHER_AGENT = AgentRef(project_id=PROJECT_ID, agent_id="other")


@pytest.fixture()
def auth_probe_client(service_app):
    @service_app.get("/_test/authenticated-caller")
    def authenticated_caller(request: Request):
        caller = authenticate_agent_request(request)
        return {
            "project_id": caller.project_id,
            "agent_id": caller.agent_id,
            "credential_id": caller.credential_id,
        }

    with TestClient(service_app) as client:
        yield client


def test_authenticate_agent_request_accepts_valid_credential_and_marks_used(
    test_pg_dsn: str,
    kernel_app,
    auth_probe_client,
) -> None:
    kernel_app.topology.register_agent(AGENT, capabilities=(TURN_KIND_CONVERSATION_V1,))
    store = PostgresAgentCredentialStore(test_pg_dsn)
    issued = store.issue_agent_credential(AGENT)

    response = auth_probe_client.get("/_test/authenticated-caller", headers=_auth_headers(issued.token))

    assert response.status_code == 200
    assert response.json() == {
        "project_id": PROJECT_ID,
        "agent_id": AGENT.agent_id,
        "credential_id": issued.ref.credential_id,
    }
    row = store.load_agent_credential_by_id(issued.ref.credential_id)
    assert row is not None
    assert row.last_used_at is not None


@pytest.mark.parametrize(
    ("headers", "message"),
    (
        ({}, "claimed agent identity headers are required"),
        ({"X-CG-Project-Id": PROJECT_ID, "X-CG-Agent-Id": AGENT.agent_id}, "agent credential authorization is required"),
        (
            {"X-CG-Project-Id": PROJECT_ID, "X-CG-Agent-Id": AGENT.agent_id, "Authorization": "Token nope"},
            "agent credential authorization must use Bearer token",
        ),
        (
            {"X-CG-Project-Id": PROJECT_ID, "X-CG-Agent-Id": AGENT.agent_id, "Authorization": "Bearer nope"},
            "invalid agent credential token format",
        ),
    ),
)
def test_authenticate_agent_request_rejects_missing_or_malformed_inputs(
    auth_probe_client,
    headers: dict[str, str],
    message: str,
) -> None:
    response = auth_probe_client.get("/_test/authenticated-caller", headers=headers)

    assert response.status_code == 401
    assert response.json()["message"] == message


def test_authenticate_agent_request_rejects_unknown_credential(auth_probe_client) -> None:
    response = auth_probe_client.get(
        "/_test/authenticated-caller",
        headers=_auth_headers(format_agent_credential_token("cred_missing", "secret")),
    )

    assert response.status_code == 401
    assert response.json()["message"] == "agent credential not found"


def test_authenticate_agent_request_rejects_wrong_secret(
    test_pg_dsn: str,
    kernel_app,
    auth_probe_client,
) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    issued = store.issue_agent_credential(AGENT)
    wrong_token = format_agent_credential_token(issued.ref.credential_id, "wrong_secret")

    response = auth_probe_client.get("/_test/authenticated-caller", headers=_auth_headers(wrong_token))

    assert response.status_code == 401
    assert response.json()["message"] == "invalid agent credential secret"


def test_authenticate_agent_request_rejects_revoked_credential(
    test_pg_dsn: str,
    kernel_app,
    auth_probe_client,
) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    issued = store.issue_agent_credential(AGENT)
    store.revoke_agent_credential(issued.ref.credential_id)

    response = auth_probe_client.get("/_test/authenticated-caller", headers=_auth_headers(issued.token))

    assert response.status_code == 403
    assert response.json()["message"] == "agent credential status is not active: revoked"


def test_authenticate_agent_request_rejects_expired_credential(
    test_pg_dsn: str,
    kernel_app,
    auth_probe_client,
) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    issued = store.issue_agent_credential(AGENT, expires_at=datetime.now(UTC) - timedelta(seconds=1))

    response = auth_probe_client.get("/_test/authenticated-caller", headers=_auth_headers(issued.token))

    assert response.status_code == 401
    assert response.json()["message"] == "agent credential expired"


def test_authenticate_agent_request_rejects_header_token_identity_mismatch(
    test_pg_dsn: str,
    kernel_app,
    auth_probe_client,
) -> None:
    kernel_app.topology.register_agent(AGENT)
    kernel_app.topology.register_agent(OTHER_AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    issued = store.issue_agent_credential(AGENT)

    response = auth_probe_client.get(
        "/_test/authenticated-caller",
        headers=_auth_headers(issued.token, agent=OTHER_AGENT),
    )

    assert response.status_code == 403
    assert response.json()["message"] == "agent credential identity does not match claimed identity"


def test_authenticate_agent_request_rejects_disabled_agent(
    test_pg_dsn: str,
    kernel_app,
    auth_probe_client,
) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    issued = store.issue_agent_credential(AGENT)
    kernel_app.topology.register_agent(AGENT, enabled=False)

    response = auth_probe_client.get("/_test/authenticated-caller", headers=_auth_headers(issued.token))

    assert response.status_code == 403
    assert response.json()["message"] == "authenticated agent is disabled"


def _auth_headers(token: str, *, agent: AgentRef = AGENT) -> dict[str, str]:
    return {
        "X-CG-Project-Id": agent.project_id,
        "X-CG-Agent-Id": agent.agent_id,
        "Authorization": f"Bearer {token}",
    }
