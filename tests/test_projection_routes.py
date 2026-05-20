from __future__ import annotations

from fastapi.testclient import TestClient

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from CommonGround.service import ServiceConfig, create_service_app

from tests.auth_support import agent_headers
from tests.projection_support import dispatch_child_turn, dispatch_root_turn, register_agent, set_invalid_public_metadata


PROJECT_ID = "projection-routes"
PROJECTION_BASE = f"/v3r1/projects/{PROJECT_ID}/projection"


def _headers(agent: AgentRef) -> dict[str, str]:
    return agent_headers(agent)


def test_projection_route_requires_agent_credential(test_client) -> None:
    response = test_client.get(f"{PROJECTION_BASE}/agents")

    assert response.status_code == 401
    assert response.json()["message"] == "claimed agent identity headers are required"


def test_projection_route_requires_agent_credential_with_explicit_service_config(kernel_app, test_pg_dsn: str) -> None:
    app = create_service_app(
        config=ServiceConfig(
            pg_dsn=test_pg_dsn,
            claim_timeout_seconds=30,
        ),
        kernel_app=kernel_app,
    )
    with TestClient(app) as client:
        response = client.get(f"{PROJECTION_BASE}/agents")

    assert response.status_code == 401
    assert response.json()["message"] == "claimed agent identity headers are required"


def test_projection_route_rejects_cross_project_headers(test_client, kernel_app) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id="other-project", agent_id="someone"))
    response = test_client.get(
        f"{PROJECTION_BASE}/agents",
        headers=_headers(caller),
    )

    assert response.status_code == 403
    assert response.json()["message"] == "authenticated caller project does not match read project"


def test_projection_routes_return_project_resources(kernel_app, test_client) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))
    provisioner = register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="provisioner"),
        capabilities=("turn.provision.agent.spawn.v1",),
        public_metadata={
            "ui": {"label": "Provisioner"},
            "turn_offers": [
                {
                    "turn_kind": "turn.provision.agent.spawn.v1",
                    "purpose": "Provision workers",
                    "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {"required_fields": ["agent.role"]},
                    "variants": {"roles": [{"role": "worker.runtime.v1"}]},
                }
            ],
        },
    )
    worker = register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="worker"),
        capabilities=(TURN_KIND_CONVERSATION_V1,),
    )
    child_worker = register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="child-worker"),
        capabilities=(TURN_KIND_CONVERSATION_V1,),
    )

    parent_turn = dispatch_root_turn(kernel_app, requested_by=caller, target_agent=worker, request_id="routes-parent")
    claim = kernel_app.lifecycle.claim_turn(worker)
    assert claim is not None
    child_turn = dispatch_child_turn(
        kernel_app,
        parent_agent=worker,
        target_agent=child_worker,
        parent_claim=claim,
        dispatch_key="routes-child",
    )

    agent_response = test_client.get(
        f"{PROJECTION_BASE}/agents",
        headers=_headers(caller),
    )
    offer_response = test_client.get(
        f"{PROJECTION_BASE}/turn-offers",
        headers=_headers(caller),
    )
    turns_response = test_client.get(
        f"{PROJECTION_BASE}/turns",
        headers=_headers(caller),
    )
    lineage_response = test_client.get(
        f"{PROJECTION_BASE}/turns/{parent_turn.turn_id}/lineage",
        headers=_headers(caller),
    )
    feed_response = test_client.get(
        f"{PROJECTION_BASE}/feed",
        headers=_headers(caller),
    )

    assert agent_response.status_code == 200
    assert offer_response.status_code == 200
    assert turns_response.status_code == 200
    assert lineage_response.status_code == 200
    assert feed_response.status_code == 200
    assert provisioner.agent_id in [item["agent_id"] for item in agent_response.json()["items"]]
    assert offer_response.json()["items"][0]["turn_kind"] == "turn.provision.agent.spawn.v1"
    assert offer_response.json()["items"][0]["variants"]["roles"] == [{"role": "worker.runtime.v1"}]
    turn_item = next(item for item in turns_response.json()["items"] if item["turn_id"] == parent_turn.turn_id)
    child_item = lineage_response.json()["direct_children"][0]
    assert turn_item["turn_id"] == f"T-{turn_item['project_turn_seq']}"
    assert isinstance(turn_item["project_turn_seq"], int)
    assert [item["turn_id"] for item in lineage_response.json()["direct_children"]] == [child_turn.turn_id]
    assert child_item["turn_id"] == f"T-{child_item['project_turn_seq']}"
    assert isinstance(child_item["project_turn_seq"], int)
    assert len(feed_response.json()["items"]) >= 2


def test_projection_lineage_returns_404_for_missing_turn(kernel_app, test_client) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))

    response = test_client.get(
        f"{PROJECTION_BASE}/turns/missing-turn/lineage",
        headers=_headers(caller),
    )

    assert response.status_code == 404
    assert response.json()["message"] == "turn not found: missing-turn"


def test_projection_routes_validate_query_params(kernel_app, test_client) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))

    response = test_client.get(
        f"{PROJECTION_BASE}/agents",
        headers=_headers(caller),
        params={"limit": 0},
    )

    assert response.status_code == 422


def test_projection_offer_route_returns_diagnostics_for_invalid_metadata(kernel_app, test_client, test_pg_dsn: str) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))
    register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="valid"),
        capabilities=("turn.provision.agent.spawn.v1",),
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": "turn.provision.agent.spawn.v1",
                    "purpose": "Provision workers",
                    "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {"required_fields": ["agent.role"]},
                    "variants": {"roles": [{"role": "worker.runtime.v1"}]},
                }
            ]
        },
    )
    invalid = register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="invalid"),
        capabilities=("turn.provision.agent.spawn.v1",),
    )
    set_invalid_public_metadata(
        test_pg_dsn=test_pg_dsn,
        agent=invalid,
        public_metadata={"turn_offers": "bad"},
    )

    response = test_client.get(
        f"{PROJECTION_BASE}/turn-offers",
        headers=_headers(caller),
    )

    assert response.status_code == 200
    body = response.json()
    assert [item["agent_id"] for item in body["items"]] == ["valid"]
    assert body["diagnostics"][0]["subject_id"] == "invalid"
