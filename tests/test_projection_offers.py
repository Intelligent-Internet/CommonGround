from __future__ import annotations

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1
from CommonGround.service.projection.filters import TurnOfferFilters
from CommonGround.service.projection.offers import list_turn_offer_entries
from CommonGround.service.projection.postgres_source import PostgresProjectionSource

from tests.projection_support import register_agent, set_invalid_public_metadata


PROJECT_ID = "projection-offers"


def test_list_turn_offer_entries_returns_valid_offers(kernel_app, test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)
    provisioner = AgentRef(project_id=PROJECT_ID, agent_id="provisioner")
    register_agent(
        kernel_app,
        provisioner,
        role="worker.provisioner.v1",
        description="Provisioner",
        capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,),
        public_metadata={
            "ui": {"label": "Provisioner"},
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_PROVISION_AGENT_SPAWN_V1,
                    "purpose": "Provision a worker",
                    "calling": {
                        "operation": "dispatch",
                        "authority_modes": [{"mode": "root_request"}, {"mode": "child_derivation"}],
                    },
                    "input_contract": {"required_fields": ["agent.role"]},
                    "variants": {"roles": [{"role": "worker.runtime.v1", "description": "Runtime worker"}]},
                }
            ],
        },
    )

    page = list_turn_offer_entries(source, project_id=PROJECT_ID, filters=TurnOfferFilters(limit=100))

    assert len(page.items) == 1
    assert page.items[0].agent_id == "provisioner"
    assert page.items[0].agent_label == "Provisioner"
    assert page.items[0].turn_kind == TURN_KIND_PROVISION_AGENT_SPAWN_V1
    assert page.diagnostics == ()


def test_list_turn_offer_entries_returns_diagnostics_for_invalid_metadata(kernel_app, test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)
    valid = AgentRef(project_id=PROJECT_ID, agent_id="valid")
    invalid = AgentRef(project_id=PROJECT_ID, agent_id="invalid")
    register_agent(
        kernel_app,
        valid,
        capabilities=(TURN_KIND_CONVERSATION_V1,),
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Conversation",
                    "calling": {"operation": "dispatch", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {},
                }
            ]
        },
    )
    register_agent(kernel_app, invalid, capabilities=(TURN_KIND_CONVERSATION_V1,))
    set_invalid_public_metadata(
        test_pg_dsn=test_pg_dsn,
        agent=invalid,
        public_metadata={
            "turn_offers": [
                {
                    "turn_kind": TURN_KIND_CONVERSATION_V1,
                    "purpose": "Bad",
                    "calling": {"operation": "submit", "authority_modes": [{"mode": "root_request"}]},
                    "input_contract": {},
                }
            ]
        },
    )

    page = list_turn_offer_entries(source, project_id=PROJECT_ID, filters=TurnOfferFilters(limit=100))

    assert [item.agent_id for item in page.items] == ["valid"]
    assert len(page.diagnostics) == 1
    assert page.diagnostics[0].subject_id == "invalid"
