from __future__ import annotations

from fastapi.testclient import TestClient

from CommonGround.agent_client import agent_auth_headers
from CommonGround.agent_credentials import AGENT_CREDENTIAL_ISSUE_ANY_GRANT, AGENT_CREDENTIAL_REVOKE_ANY_GRANT
from CommonGround.app import build_kernel_app
from CommonGround.contracts import AgentRef
from CommonGround.infra import PostgresAgentCredentialStore
from CommonGround.service import ServiceConfig, create_service_app

from tests.pg_support import reset_test_db


PROJECT_ID = "credential-service-project"
ADMIN = AgentRef(project_id=PROJECT_ID, agent_id="admin-service")
WORKER = AgentRef(project_id=PROJECT_ID, agent_id="worker")
OTHER = AgentRef(project_id=PROJECT_ID, agent_id="other")
OTHER_PROJECT_ADMIN = AgentRef(project_id="other-credential-project", agent_id="admin-service")


def _make_client(test_pg_dsn: str) -> tuple[TestClient, PostgresAgentCredentialStore]:
    reset_test_db(test_pg_dsn)
    kernel_app = build_kernel_app(pg_dsn=test_pg_dsn, claim_timeout_seconds=30)
    kernel_app.topology.register_agent(
        ADMIN,
        grants=(AGENT_CREDENTIAL_ISSUE_ANY_GRANT, AGENT_CREDENTIAL_REVOKE_ANY_GRANT),
        accepts_work=False,
    )
    kernel_app.topology.register_agent(WORKER)
    kernel_app.topology.register_agent(OTHER)
    kernel_app.topology.register_agent(
        OTHER_PROJECT_ADMIN,
        grants=(AGENT_CREDENTIAL_ISSUE_ANY_GRANT, AGENT_CREDENTIAL_REVOKE_ANY_GRANT),
        accepts_work=False,
    )
    app = create_service_app(
        config=ServiceConfig(pg_dsn=test_pg_dsn, claim_timeout_seconds=30),
        kernel_app=kernel_app,
    )
    return TestClient(app), PostgresAgentCredentialStore(test_pg_dsn)


def _headers(store: PostgresAgentCredentialStore, agent: AgentRef) -> dict[str, str]:
    token = store.issue_agent_credential(agent).token
    return agent_auth_headers(agent, token)


def test_admin_agent_issues_lists_and_revokes_agent_credential(test_pg_dsn: str) -> None:
    client, store = _make_client(test_pg_dsn)

    issued = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials:issue",
        headers=_headers(store, ADMIN),
        json={"provenance_kind": "test", "provenance_ref": "case-001"},
    )
    assert issued.status_code == 200
    body = issued.json()
    credential = body["credential"]
    assert body["token"].startswith("cgac_")
    assert credential["agent_id"] == WORKER.agent_id
    assert credential["issued_by_agent_id"] == ADMIN.agent_id
    assert credential["provenance_kind"] == "test"
    assert "secret_hash" not in credential

    listed = client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials",
        headers=_headers(store, ADMIN),
    )
    assert listed.status_code == 200
    assert [item["credential_id"] for item in listed.json()["credentials"]] == [credential["credential_id"]]
    assert "secret_hash" not in listed.text

    revoked = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials/{credential['credential_id']}:revoke",
        headers=_headers(store, ADMIN),
        json={},
    )
    assert revoked.status_code == 200
    assert revoked.json()["credential"]["status"] == "revoked"
    client.close()


def test_credential_issue_requires_admin_grant(test_pg_dsn: str) -> None:
    client, store = _make_client(test_pg_dsn)

    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials:issue",
        headers=_headers(store, OTHER),
        json={},
    )

    assert response.status_code == 409
    assert response.json()["message"] == f"caller missing grant: {AGENT_CREDENTIAL_ISSUE_ANY_GRANT}"
    client.close()


def test_agent_can_list_and_revoke_own_credentials_without_admin_grant(test_pg_dsn: str) -> None:
    client, store = _make_client(test_pg_dsn)
    issued = store.issue_agent_credential(WORKER)
    headers = agent_auth_headers(WORKER, issued.token)

    listed = client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials",
        headers=headers,
    )
    assert listed.status_code == 200
    assert [item["credential_id"] for item in listed.json()["credentials"]] == [issued.ref.credential_id]

    revoked = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials/{issued.ref.credential_id}:revoke",
        headers=headers,
        json={},
    )

    assert revoked.status_code == 200
    assert revoked.json()["credential"]["status"] == "revoked"
    client.close()


def test_agent_cannot_list_or_revoke_other_agent_credentials_without_admin_grant(test_pg_dsn: str) -> None:
    client, store = _make_client(test_pg_dsn)
    issued = store.issue_agent_credential(WORKER)

    listed = client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials",
        headers=_headers(store, OTHER),
    )
    revoked = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials/{issued.ref.credential_id}:revoke",
        headers=_headers(store, OTHER),
        json={},
    )

    assert listed.status_code == 409
    assert f"caller missing grant: {AGENT_CREDENTIAL_ISSUE_ANY_GRANT}" in listed.json()["message"]
    assert revoked.status_code == 409
    assert revoked.json()["message"] == f"caller missing grant: {AGENT_CREDENTIAL_REVOKE_ANY_GRANT}"
    client.close()


def test_credential_lifecycle_rejects_cross_project_admin_before_target_lookup(test_pg_dsn: str) -> None:
    client, store = _make_client(test_pg_dsn)

    issued = store.issue_agent_credential(WORKER)
    headers = _headers(store, OTHER_PROJECT_ADMIN)

    issue = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/missing-agent/credentials:issue",
        headers=headers,
        json={},
    )
    listed = client.get(
        f"/v3r1/projects/{PROJECT_ID}/agents/missing-agent/credentials",
        headers=headers,
    )
    revoked = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{WORKER.agent_id}/credentials/{issued.ref.credential_id}:revoke",
        headers=headers,
        json={},
    )

    assert issue.status_code == 409
    assert listed.status_code == 409
    assert revoked.status_code == 409
    assert issue.json()["message"] == "caller project must match path project"
    assert listed.json()["message"] == "caller project must match path project"
    assert revoked.json()["message"] == "caller project must match path project"
    client.close()
