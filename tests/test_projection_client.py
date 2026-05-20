from __future__ import annotations

import httpx
import pytest

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from CommonGround.projection_client import ProjectionHttpClient

from tests.auth_support import agent_headers, agent_token
from tests.projection_support import dispatch_child_turn, dispatch_root_turn, register_agent, set_invalid_public_metadata


PROJECT_ID = "projection-client"


def _headers(agent: AgentRef) -> dict[str, str]:
    return agent_headers(agent)


def test_projection_http_client_parses_projection_resources(kernel_app, test_client) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))
    register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="provisioner"),
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

    parent_turn = dispatch_root_turn(kernel_app, requested_by=caller, target_agent=worker, request_id="client-parent")
    claim = kernel_app.lifecycle.claim_turn(worker)
    assert claim is not None
    child_turn = dispatch_child_turn(
        kernel_app,
        parent_agent=worker,
        target_agent=child_worker,
        parent_claim=claim,
        dispatch_key="client-child",
    )

    client = ProjectionHttpClient(client=test_client, headers=_headers(caller))

    agents = client.list_agents(project_id=PROJECT_ID)
    offers = client.list_turn_offers(project_id=PROJECT_ID)
    turns = client.list_turns(project_id=PROJECT_ID)
    lineage = client.get_turn_lineage(project_id=PROJECT_ID, turn_id=parent_turn.turn_id)
    feed = client.fetch_project_feed(project_id=PROJECT_ID)

    assert any(item.agent_id == "provisioner" for item in agents.items)
    assert offers.items[0].turn_kind == "turn.provision.agent.spawn.v1"
    assert offers.items[0].variants["roles"] == [{"role": "worker.runtime.v1"}]
    assert any(item.project_turn_seq > 0 and item.turn_id == f"T-{item.project_turn_seq}" for item in turns.items)
    assert lineage.parent.turn_id == f"T-{lineage.parent.project_turn_seq}"
    assert lineage.direct_children[0].turn_id == f"T-{lineage.direct_children[0].project_turn_seq}"
    assert lineage.direct_children[0].turn_id == child_turn.turn_id
    assert len(feed.items) >= 2
    client.close()


def test_projection_http_client_can_build_agent_credential_headers(kernel_app, test_client) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))
    register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="worker"))
    client = ProjectionHttpClient(
        client=test_client,
        auth_token=agent_token(caller),
        agent=caller,
    )

    page = client.list_agents(project_id=PROJECT_ID)

    assert {item.agent_id for item in page.items} >= {"caller", "worker"}
    client.close()


def test_projection_http_client_raises_404_for_missing_lineage(test_client, kernel_app) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))
    client = ProjectionHttpClient(client=test_client, headers=_headers(caller))

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        client.get_turn_lineage(project_id=PROJECT_ID, turn_id="missing-turn")

    assert exc_info.value.response.status_code == 404
    client.close()


def test_projection_http_client_parses_offer_diagnostics(test_client, kernel_app, test_pg_dsn: str) -> None:
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

    client = ProjectionHttpClient(client=test_client, headers=_headers(caller))

    page = client.list_turn_offers(project_id=PROJECT_ID)

    assert [item.agent_id for item in page.items] == ["valid"]
    assert page.diagnostics[0].subject_id == "invalid"
    client.close()
