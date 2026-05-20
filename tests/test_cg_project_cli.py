from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from typing import Any

import pytest

from CommonGround.cli import main
from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1, TurnState
from CommonGround.projection_client import ProjectionHttpClient, ProjectedAgentDirectoryPage, ProjectedAgentEntry, ProjectedTurnOfferEntry, ProjectedTurnOfferEntryPage

from tests.auth_support import agent_token
from tests.projection_support import dispatch_root_turn, register_agent


PROJECT_ID = "cg-project-cli"

pytestmark = pytest.mark.usefixtures("isolated_cli_runtime")


def test_cg_project_agent_list_uses_projection_client() -> None:
    calls: list[dict[str, Any]] = []
    factory_calls: list[dict[str, Any]] = []

    class FakeProjectionClient:
        def list_agents(self, **kwargs):
            calls.append(kwargs)
            return ProjectedAgentDirectoryPage(
                project_id=PROJECT_ID,
                items=(
                    ProjectedAgentEntry(
                        agent_id="worker",
                        role="worker.runtime.v1",
                        description="Worker",
                        enabled=True,
                        accepts_work=True,
                        capabilities=("turn.conversation.v1",),
                        public_metadata={},
                        last_seen_at=datetime(2026, 4, 14, tzinfo=UTC),
                    ),
                ),
                limit=100,
            )

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "project",
            "agent",
            "list",
            "--project-id",
            PROJECT_ID,
            "--role",
            "worker.runtime.v1",
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            "reader",
        ],
        stdout=stdout,
        projection_client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeProjectionClient(),
    )

    assert exit_code == 0
    body = json.loads(stdout.getvalue())
    assert body["ok"] is True
    assert body["result"]["project_id"] == PROJECT_ID
    assert body["result"]["items"][0]["agent_id"] == "worker"
    assert calls == [{"project_id": PROJECT_ID, "enabled_only": None, "accepts_work_only": None, "role": "worker.runtime.v1", "capability": None, "limit": 100}]
    assert factory_calls == [
        {
            "base_url": "http://127.0.0.1:8000",
            "auth_token": "test-token",
            "headers": {
                "X-CG-Project-Id": PROJECT_ID,
                "X-CG-Agent-Id": "reader",
                "Authorization": "Bearer test-token",
            },
        }
    ]


def test_cg_project_offer_list_uses_projection_client() -> None:
    calls: list[dict[str, Any]] = []

    class FakeProjectionClient:
        def list_turn_offers(self, **kwargs):
            calls.append(kwargs)
            return ProjectedTurnOfferEntryPage(
                project_id=PROJECT_ID,
                items=(
                    ProjectedTurnOfferEntry(
                        agent_id="worker",
                        agent_label="Worker",
                        agent_description="Worker",
                        turn_kind="turn.conversation.v1",
                        purpose="Conversation",
                        calling={"operation": "dispatch"},
                        input_contract={"required_fields": []},
                        variants={},
                        enabled=True,
                        accepts_work=True,
                        metadata_source="agent.public_metadata.turn_offers",
                    ),
                ),
                limit=100,
            )

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "project",
            "offer",
            "list",
            "--project-id",
            PROJECT_ID,
            "--turn-kind",
            "turn.conversation.v1",
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            "reader",
        ],
        stdout=stdout,
        projection_client_factory=lambda **_: FakeProjectionClient(),
    )

    assert exit_code == 0
    body = json.loads(stdout.getvalue())
    assert body["result"]["items"][0]["turn_kind"] == "turn.conversation.v1"
    assert calls == [{"project_id": PROJECT_ID, "turn_kind": "turn.conversation.v1", "agent_id": None, "enabled_only": None, "accepts_work_only": None, "limit": 100}]


def test_cg_project_offer_get_uses_projection_client() -> None:
    calls: list[dict[str, Any]] = []

    class FakeProjectionClient:
        def list_turn_offers(self, **kwargs):
            calls.append(kwargs)
            return ProjectedTurnOfferEntryPage(
                project_id=PROJECT_ID,
                items=(
                    ProjectedTurnOfferEntry(
                        agent_id="worker",
                        agent_label="Worker",
                        agent_description="Worker",
                        turn_kind="turn.conversation.v1",
                        purpose="Conversation",
                        calling={"operation": "dispatch"},
                        input_contract={"required_fields": []},
                        variants={},
                        enabled=True,
                        accepts_work=True,
                        metadata_source="agent.public_metadata.turn_offers",
                    ),
                ),
                limit=2,
            )

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "project",
            "offer",
            "get",
            "--project-id",
            PROJECT_ID,
            "--turn-kind",
            "turn.conversation.v1",
            "--agent-id",
            "worker",
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            "reader",
        ],
        stdout=stdout,
        projection_client_factory=lambda **_: FakeProjectionClient(),
    )

    assert exit_code == 0
    body = json.loads(stdout.getvalue())
    assert body["result"]["turn_kind"] == "turn.conversation.v1"
    assert body["result"]["agent_id"] == "worker"
    assert calls == [{"project_id": PROJECT_ID, "turn_kind": "turn.conversation.v1", "agent_id": "worker", "limit": 2}]


def test_cg_project_offer_get_returns_not_found_when_missing() -> None:
    class FakeProjectionClient:
        def list_turn_offers(self, **kwargs):
            return ProjectedTurnOfferEntryPage(project_id=PROJECT_ID, items=(), limit=2)

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "project",
            "offer",
            "get",
            "--project-id",
            PROJECT_ID,
            "--turn-kind",
            "turn.conversation.v1",
            "--agent-id",
            "worker",
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            "reader",
        ],
        stdout=stdout,
        projection_client_factory=lambda **_: FakeProjectionClient(),
    )

    assert exit_code == 1
    assert json.loads(stdout.getvalue()) == {
        "ok": False,
        "error": {
            "code": "not_found",
            "message": "turn offer not found: agent_id=worker turn_kind=turn.conversation.v1",
            "status": 404,
        },
    }


def test_cg_project_cli_roundtrip_with_service(test_client, kernel_app, monkeypatch) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", agent_token(caller))
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
    turn = dispatch_root_turn(kernel_app, requested_by=caller, target_agent=worker, request_id="cli-project-1")

    def make_projection_client(**kwargs: Any):
        return ProjectionHttpClient(
            client=test_client,
            **kwargs,
        )

    turn_out = io.StringIO()
    turn_code = main(
        [
            "project",
            "turn",
            "list",
            "--project-id",
            PROJECT_ID,
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            caller.agent_id,
        ],
        stdout=turn_out,
        projection_client_factory=make_projection_client,
    )
    turn_body = json.loads(turn_out.getvalue())
    assert turn_code == 0
    assert turn_body["result"]["items"][0]["turn_id"] == f"T-{turn_body['result']['items'][0]['project_turn_seq']}"
    assert turn_body["result"]["items"][0]["turn_id"] == turn.turn_id
    assert turn_body["result"]["items"][0]["state"] == TurnState.QUEUED.value

    offer_out = io.StringIO()
    offer_code = main(
        [
            "project",
            "offer",
            "list",
            "--project-id",
            PROJECT_ID,
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            caller.agent_id,
        ],
        stdout=offer_out,
        projection_client_factory=make_projection_client,
    )
    offer_body = json.loads(offer_out.getvalue())
    assert offer_code == 0
    assert offer_body["result"]["items"][0]["turn_kind"] == "turn.provision.agent.spawn.v1"
    assert offer_body["result"]["items"][0]["variants"]["roles"] == [{"role": "worker.runtime.v1"}]

    feed_out = io.StringIO()
    feed_code = main(
        [
            "project",
            "feed",
            "--project-id",
            PROJECT_ID,
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            caller.agent_id,
        ],
        stdout=feed_out,
        projection_client_factory=make_projection_client,
    )
    feed_body = json.loads(feed_out.getvalue())
    assert feed_code == 0
    assert len(feed_body["result"]["items"]) >= 1


def test_cg_project_turn_lineage_not_found_returns_error_envelope(test_client, kernel_app, monkeypatch) -> None:
    caller = register_agent(kernel_app, AgentRef(project_id=PROJECT_ID, agent_id="caller"))
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", agent_token(caller))

    def make_projection_client(**kwargs: Any):
        return ProjectionHttpClient(
            client=test_client,
            **kwargs,
        )

    stdout = io.StringIO()
    exit_code = main(
        [
            "project",
            "turn",
            "lineage",
            "--project-id",
            PROJECT_ID,
            "--turn-id",
            "missing-turn",
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            caller.agent_id,
        ],
        stdout=stdout,
        projection_client_factory=make_projection_client,
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["ok"] is False
    assert body["error"]["code"] == "not_found"
