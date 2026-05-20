from __future__ import annotations

from fastapi.testclient import TestClient
import pytest
import time

from CommonGround.app import build_kernel_app
from CommonGround.agent_client import HttpAgentClient, agent_auth_headers
from CommonGround.contracts import AgentRef, ClaimToken, TURN_KIND_CONVERSATION_V1
from CommonGround.infra import PostgresAgentCredentialStore
from CommonGround.service import ServiceConfig, create_service_app

from tests.pg_support import reset_test_db


PROJECT_ID = "auth-demo"
FRONTSIDE = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
WORKER = AgentRef(project_id=PROJECT_ID, agent_id="worker")


def _v3_agent_path(agent_id: str) -> str:
    return f"/v3r1/projects/{PROJECT_ID}/agents/{agent_id}"


def _v3_turn_path(turn_id: str, suffix: str = "") -> str:
    return f"/v3r1/projects/{PROJECT_ID}/turns/{turn_id}{suffix}"


def _make_client(*, test_pg_dsn: str) -> tuple[TestClient, object]:
    reset_test_db(test_pg_dsn)
    kernel_app = build_kernel_app(pg_dsn=test_pg_dsn, claim_timeout_seconds=30)
    kernel_app.topology.register_agent(FRONTSIDE, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(WORKER, capabilities=(TURN_KIND_CONVERSATION_V1,))
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
        ),
        kernel_app=kernel_app,
    )
    return TestClient(app), kernel_app


def _auth_headers(test_pg_dsn: str, agent: AgentRef) -> dict[str, str]:
    token = PostgresAgentCredentialStore(test_pg_dsn).issue_agent_credential(agent).token
    return agent_auth_headers(agent, token)


def _claim_body(claim: ClaimToken) -> dict[str, str]:
    return {
        "project_id": claim.project_id,
        "turn_id": claim.turn_id,
        "agent_id": claim.agent_id,
        "token": claim.token,
        "expires_at": claim.expires_at.isoformat(),
    }


def _root_dispatch_body(*, requested_by: AgentRef, target_agent: AgentRef, task: str, request_id: str) -> dict[str, object]:
    return {
        "requested_by": {"project_id": requested_by.project_id, "agent_id": requested_by.agent_id},
        "target_agent": {"project_id": target_agent.project_id, "agent_id": target_agent.agent_id},
        "input": {"task": task},
        "dispatch_key": request_id,
        "turn_kind": TURN_KIND_CONVERSATION_V1,
        "authority": {"mode": "root_request", "request_id": request_id},
    }


def _work_memory_report_body(request_id: str = "auth-work-memory-report") -> dict[str, object]:
    return {
        "kind": "agent_work_memory_report_manifest.v1",
        "request_id": request_id,
        "records": [
            {
                "role": "local_experience_summary",
                "payload": {"summary": "auth coverage"},
                "source_refs": [],
            }
        ],
    }


def _child_dispatch_body(*, requested_by: AgentRef, target_agent: AgentRef, parent_claim: ClaimToken, task: str, dispatch_key: str) -> dict[str, object]:
    return {
        "requested_by": {"project_id": requested_by.project_id, "agent_id": requested_by.agent_id},
        "target_agent": {"project_id": target_agent.project_id, "agent_id": target_agent.agent_id},
        "input": {"task": task},
        "dispatch_key": dispatch_key,
        "turn_kind": TURN_KIND_CONVERSATION_V1,
        "authority": {
            "mode": "child_derivation",
            "parent_claim": _claim_body(parent_claim),
        },
    }


def _spawn_and_claim_turn(client: TestClient, test_pg_dsn: str) -> ClaimToken:
    spawn = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="claim-auth", request_id="auth-claim-route"),
    )
    assert spawn.status_code == 200

    worker_client = HttpAgentClient(
        client=client,
        headers=_auth_headers(test_pg_dsn, WORKER),
    )
    claimed = worker_client.claim_turn(WORKER)
    assert claimed is not None
    return claimed.claim


def test_protected_route_requires_agent_credential_when_bypass_disabled(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="auth", request_id="auth-1"),
    )

    assert response.status_code == 401
    assert response.json()["message"] == "claimed agent identity headers are required"
    client.close()


def test_protected_route_rejects_mismatched_agent_credential_headers(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, WORKER),
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="auth", request_id="auth-2"),
    )

    assert response.status_code == 403
    assert response.json()["message"] == "authenticated caller identity does not match request body"
    client.close()


def test_protected_route_accepts_matching_agent_credential_headers(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="auth", request_id="auth-3"),
    )

    assert response.status_code == 200
    assert response.json()["project_id"] == PROJECT_ID
    client.close()


def test_work_memory_report_route_requires_agent_credential_when_bypass_disabled(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)

    unauthorized = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{FRONTSIDE.agent_id}/work-memory-reports",
        json=_work_memory_report_body(),
    )
    assert unauthorized.status_code == 401
    assert unauthorized.json()["message"] == "claimed agent identity headers are required"

    authorized = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/{FRONTSIDE.agent_id}/work-memory-reports",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_work_memory_report_body(),
    )
    assert authorized.status_code == 200
    assert authorized.json()["turn"]["project_id"] == PROJECT_ID
    client.close()


def test_v3r1_dispatch_rejects_path_body_project_mismatch(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    response = client.post(
        "/v3r1/projects/other-project/turns:dispatch",
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="auth", request_id="auth-project-mismatch"),
    )

    assert response.status_code == 409
    assert response.json()["message"] == "requested_by project must match path project"
    client.close()


def test_v3r1_claim_rejects_path_body_agent_mismatch(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/agents/other-agent/claims:claim",
        json={"agent": {"project_id": PROJECT_ID, "agent_id": WORKER.agent_id}},
    )

    assert response.status_code == 409
    assert response.json()["message"] == "path agent_id must match request agent"
    client.close()


def test_v3r1_claim_fenced_route_rejects_path_claim_turn_mismatch(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    claim = _spawn_and_claim_turn(client, test_pg_dsn)

    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns/other-turn:suspend",
        json={
            "claim": _claim_body(claim),
            "reason": "mismatch",
        },
    )

    assert response.status_code == 409
    assert response.json()["message"] == "path turn_id must match claim.turn_id"
    client.close()


def test_localhost_dev_bypass_does_not_skip_agent_credential(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="auth", request_id="auth-4"),
    )

    assert response.status_code == 401
    assert response.json()["message"] == "claimed agent identity headers are required"
    client.close()


def test_dispatch_child_rejects_mismatched_agent_credential_headers(test_pg_dsn: str) -> None:
    client, kernel_app = _make_client(test_pg_dsn=test_pg_dsn)
    child = AgentRef(project_id=PROJECT_ID, agent_id="child")
    kernel_app.topology.register_agent(child, capabilities=(TURN_KIND_CONVERSATION_V1,))

    spawn = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="auth", request_id="auth-dispatch-1"),
    )
    assert spawn.status_code == 200

    worker_client = HttpAgentClient(
        client=client,
        headers=_auth_headers(test_pg_dsn, WORKER),
    )
    claimed = worker_client.claim_turn(WORKER)
    assert claimed is not None

    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_child_dispatch_body(
            requested_by=WORKER,
            target_agent=child,
            parent_claim=claimed.claim,
            task="child",
            dispatch_key="auth-child-1",
        ),
    )

    assert response.status_code == 403
    assert response.json()["message"] == "authenticated caller identity does not match request body"
    client.close()


def test_dispatch_child_requires_and_accepts_matching_agent_credential_when_bypass_disabled(test_pg_dsn: str) -> None:
    client, kernel_app = _make_client(test_pg_dsn=test_pg_dsn)
    child = AgentRef(project_id=PROJECT_ID, agent_id="child")
    kernel_app.topology.register_agent(child, capabilities=(TURN_KIND_CONVERSATION_V1,))

    spawn = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="dispatch-auth", request_id="auth-dispatch-2"),
    )
    assert spawn.status_code == 200

    worker_client = HttpAgentClient(
        client=client,
        headers=_auth_headers(test_pg_dsn, WORKER),
    )
    claimed = worker_client.claim_turn(WORKER)
    assert claimed is not None

    payload = _child_dispatch_body(
        requested_by=WORKER,
        target_agent=child,
        parent_claim=claimed.claim,
        task="child",
        dispatch_key="auth-child-2",
    )

    unauthorized = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        json=payload,
    )
    assert unauthorized.status_code == 401
    assert unauthorized.json()["message"] == "claimed agent identity headers are required"

    authorized = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, WORKER),
        json=payload,
    )
    assert authorized.status_code == 200
    client.close()


def test_dispatch_child_unauthorized_override_does_not_renew_parent_claim(test_pg_dsn: str) -> None:
    client, kernel_app = _make_client(test_pg_dsn=test_pg_dsn)
    child = AgentRef(project_id=PROJECT_ID, agent_id="child")
    delegate = AgentRef(project_id=PROJECT_ID, agent_id="delegate")
    kernel_app.topology.register_agent(child, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(delegate, capabilities=(TURN_KIND_CONVERSATION_V1,))

    claim = _spawn_and_claim_turn(client, test_pg_dsn)
    worker_client = HttpAgentClient(
        client=client,
        headers=_auth_headers(test_pg_dsn, WORKER),
    )
    before = worker_client.get_turn(claim.turn_ref())
    assert before.claim_expires_at is not None

    time.sleep(0.02)

    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, delegate),
        json=_child_dispatch_body(
            requested_by=delegate,
            target_agent=child,
            parent_claim=claim,
            task="child",
            dispatch_key="auth-child-unauthorized-renew",
        ),
    )

    assert response.status_code == 409
    after = worker_client.get_turn(claim.turn_ref())
    assert after.claim_expires_at == before.claim_expires_at
    client.close()


def test_resume_route_requires_and_accepts_matching_agent_credential_when_bypass_disabled(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    claim = _spawn_and_claim_turn(client, test_pg_dsn)

    worker_client = HttpAgentClient(
        client=client,
        headers=_auth_headers(test_pg_dsn, WORKER),
    )
    worker_client.suspend_turn(claim, reason="waiting_for_input", note="resume-auth")

    payload = {
        "requested_by": {"project_id": PROJECT_ID, "agent_id": WORKER.agent_id},
        "note": "resume-now",
    }

    unauthorized = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns/{claim.turn_id}:resume",
        json=payload,
    )
    assert unauthorized.status_code == 401
    assert unauthorized.json()["message"] == "claimed agent identity headers are required"

    mismatched = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns/{claim.turn_id}:resume",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=payload,
    )
    assert mismatched.status_code == 403
    assert mismatched.json()["message"] == "authenticated caller identity does not match request body"

    authorized = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns/{claim.turn_id}:resume",
        headers=_auth_headers(test_pg_dsn, WORKER),
        json=payload,
    )
    assert authorized.status_code == 200
    client.close()


def test_turn_read_route_requires_agent_credential_when_bypass_disabled(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    spawn = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_root_dispatch_body(requested_by=FRONTSIDE, target_agent=WORKER, task="read-auth", request_id="auth-read-1"),
    )
    assert spawn.status_code == 200
    turn_id = spawn.json()["turn_id"]

    response = client.get(_v3_turn_path(turn_id))

    assert response.status_code == 401
    assert response.json()["message"] == "claimed agent identity headers are required"
    client.close()


@pytest.mark.parametrize("suffix", ["", "/context", "/feed"])
def test_turn_read_routes_do_not_leak_turn_existence_without_headers(test_pg_dsn: str, suffix: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    spawn = client.post(
        f"/v3r1/projects/{PROJECT_ID}/turns:dispatch",
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=_root_dispatch_body(
            requested_by=FRONTSIDE,
            target_agent=WORKER,
            task="read-auth-existence",
            request_id=f"auth-read-existence-{suffix or 'turn'}",
        ),
    )
    assert spawn.status_code == 200
    existing_turn_id = spawn.json()["turn_id"]

    existing = client.get(
        _v3_turn_path(existing_turn_id, suffix),
    )
    missing = client.get(
        _v3_turn_path("missing-turn", suffix),
    )

    assert existing.status_code == 401
    assert missing.status_code == 401
    assert existing.json()["message"] == "claimed agent identity headers are required"
    assert missing.json()["message"] == "claimed agent identity headers are required"
    client.close()


def test_reconcile_route_defaults_to_caller_and_requires_headers_when_bypass_disabled(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)

    response = client.post(f"/v3r1/projects/{PROJECT_ID}/claims:reconcile-expired", json={})
    assert response.status_code == 401

    response = client.post(
        f"/v3r1/projects/{PROJECT_ID}/claims:reconcile-expired",
        headers=_auth_headers(test_pg_dsn, WORKER),
        json={},
    )

    assert response.status_code == 200
    assert set(response.json().keys()) == {"scanned_count", "reconciled_count"}
    client.close()


def test_agent_get_route_requires_agent_credential_when_bypass_disabled(test_pg_dsn: str) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)

    response = client.get(_v3_agent_path(WORKER.agent_id))
    assert response.status_code == 401

    response = client.get(
        _v3_agent_path(WORKER.agent_id),
        headers=_auth_headers(test_pg_dsn, WORKER),
    )
    assert response.status_code == 200
    assert response.json()["agent"]["agent_id"] == WORKER.agent_id
    client.close()


@pytest.mark.parametrize(
    ("route_kind", "payload_builder"),
    [
        ("renew", lambda claim: {"claim": _claim_body(claim)}),
        (
            "append",
            lambda claim: {
                "claim": _claim_body(claim),
                "payload": {"progress": "still-working"},
                "role": "progress",
            },
        ),
        (
            "suspend",
            lambda claim: {
                "claim": _claim_body(claim),
                "reason": "waiting_for_input",
                "note": "auth coverage",
            },
        ),
        (
            "finish",
            lambda claim: {
                "claim": _claim_body(claim),
                "outcome": "succeeded",
                "final_payload": {"result": "done"},
                "final_record_role": "deliverable",
            },
        ),
    ],
)
def test_claim_token_write_routes_require_matching_agent_credential_when_bypass_disabled(
    test_pg_dsn: str,
    route_kind: str,
    payload_builder,
) -> None:
    client, _ = _make_client(test_pg_dsn=test_pg_dsn)
    claim = _spawn_and_claim_turn(client, test_pg_dsn)
    route = {
        "renew": f"/v3r1/projects/{PROJECT_ID}/claims:renew",
        "append": f"/v3r1/projects/{PROJECT_ID}/turns/{claim.turn_id}/semantic-records",
        "suspend": f"/v3r1/projects/{PROJECT_ID}/turns/{claim.turn_id}:suspend",
        "finish": f"/v3r1/projects/{PROJECT_ID}/turns/{claim.turn_id}:finish",
    }[route_kind]
    payload = payload_builder(claim)

    unauthorized = client.post(route, json=payload)
    assert unauthorized.status_code == 401
    assert unauthorized.json()["message"] == "claimed agent identity headers are required"

    mismatched = client.post(
        route,
        headers=_auth_headers(test_pg_dsn, FRONTSIDE),
        json=payload,
    )
    assert mismatched.status_code == 403
    assert mismatched.json()["message"] == "authenticated caller identity does not match request body"

    authorized = client.post(
        route,
        headers=_auth_headers(test_pg_dsn, WORKER),
        json=payload,
    )
    assert authorized.status_code == 200
    client.close()
