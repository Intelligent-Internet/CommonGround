from __future__ import annotations

from types import SimpleNamespace

import pytest

from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1
from CommonGround.agent_client import agent_auth_headers
from Integrations.nanobot.adapter.registration_spec import AgentRegistrationSpec, REGISTRATION_SPEC_ENV, encode_registration_spec
from Integrations.nanobot.runtime.leaf_worker_main import load_leaf_worker_env, register_leaf_worker, run_from_env


def _leaf_spec() -> AgentRegistrationSpec:
    return AgentRegistrationSpec(
        role="nanobot.leaf.conversation.v1",
        capabilities=(TURN_KIND_CONVERSATION_V1,),
        grants=(),
        description="NanoBot leaf worker for conversation turns.",
        accepts_work=True,
    )


def _env() -> dict[str, str]:
    return {
        "CG_BASE_URL": "http://cg.test",
        "CG_PROJECT_ID": "demo",
        "CG_AGENT_ID": "nanobot_leaf_001",
        "CG_AGENT_CREDENTIAL_TOKEN": "test-token",
        REGISTRATION_SPEC_ENV: encode_registration_spec(_leaf_spec()),
        "CG_PROVISION_TURN_ID": "provision-1",
        "NANOBOT_REPO_ROOT": "/repo/nanobot",
        "NANOBOT_CONFIG_PATH": "/repo/config.json",
        "NANOBOT_WORKSPACE": "/tmp/leaf",
    }


def test_load_leaf_worker_env_reads_provisioning_context() -> None:
    env = load_leaf_worker_env(_env())

    assert env.base_url == "http://cg.test"
    assert env.agent_ref() == AgentRef(project_id="demo", agent_id="nanobot_leaf_001")
    assert env.credential_token == "test-token"
    assert env.registration_spec == _leaf_spec()
    assert env.provision_turn_id == "provision-1"


def test_run_from_env_checks_existing_leaf_and_starts_runner() -> None:
    clients = []
    runner_calls = []

    class FakeClient:
        def __init__(self, *, base_url, headers=None):
            self.base_url = base_url
            self.headers = headers
            self.get_agent_calls = []
            self.closed = False
            clients.append(self)

        def get_agent(self, agent):
            self.get_agent_calls.append(agent)
            return SimpleNamespace(
                agent=agent,
                role="nanobot.leaf.conversation.v1",
                capabilities=(TURN_KIND_CONVERSATION_V1,),
                grants=(),
                accepts_work=True,
            )

        def close(self):
            self.closed = True

    def fake_runner(**kwargs):
        runner_calls.append(kwargs)

    run_from_env(
        _env(),
        client_factory=FakeClient,
        runner=fake_runner,
    )

    agent = AgentRef(project_id="demo", agent_id="nanobot_leaf_001")
    assert clients[0].base_url == "http://cg.test"
    assert clients[0].headers == agent_auth_headers(agent, "test-token")
    assert clients[0].closed is True
    assert clients[0].get_agent_calls == [agent]
    assert runner_calls == [
        {
            "base_url": "http://cg.test",
            "agent": agent,
            "config_path": "/repo/config.json",
            "workspace": "/tmp/leaf",
            "repo_root": "/repo/nanobot",
        }
    ]


def test_register_leaf_worker_accepts_canonical_capability_and_grant_order() -> None:
    class FakeClient:
        def get_agent(self, agent):
            return SimpleNamespace(
                agent=agent,
                role="nanobot.leaf.conversation.v1",
                capabilities=("z.capability", TURN_KIND_CONVERSATION_V1, "z.capability"),
                grants=("b.grant", "a.grant"),
                accepts_work=True,
            )

    env = load_leaf_worker_env(
        {
            "CG_BASE_URL": "http://cg.test",
            "CG_PROJECT_ID": "demo",
            "CG_AGENT_ID": "nanobot_leaf_001",
            "CG_AGENT_CREDENTIAL_TOKEN": "test-token",
            REGISTRATION_SPEC_ENV: encode_registration_spec(
                AgentRegistrationSpec(
                    role="nanobot.leaf.conversation.v1",
                    capabilities=(TURN_KIND_CONVERSATION_V1, "z.capability"),
                    grants=("a.grant", "b.grant"),
                    description="NanoBot leaf worker for conversation turns.",
                    accepts_work=True,
                )
            ),
        }
    )

    assert register_leaf_worker(FakeClient(), env) == AgentRef(project_id="demo", agent_id="nanobot_leaf_001")


def test_load_leaf_worker_env_requires_registration_spec() -> None:
    with pytest.raises(ValueError, match="CG_AGENT_REGISTRATION_SPEC"):
        load_leaf_worker_env(
            {
                "CG_BASE_URL": "http://cg.test",
                "CG_PROJECT_ID": "demo",
                "CG_AGENT_ID": "nanobot_leaf_001",
                "CG_AGENT_CREDENTIAL_TOKEN": "test-token",
            }
        )


def test_register_leaf_worker_requires_registered_agent() -> None:
    class FakeClient:
        def get_agent(self, agent):
            return None

    env = load_leaf_worker_env(
        {
            "CG_BASE_URL": "http://cg.test",
            "CG_PROJECT_ID": "demo",
            "CG_AGENT_ID": "nanobot_leaf_001",
            "CG_AGENT_CREDENTIAL_TOKEN": "test-token",
            REGISTRATION_SPEC_ENV: encode_registration_spec(_leaf_spec()),
        }
    )

    with pytest.raises(ValueError, match="leaf agent is not registered"):
        register_leaf_worker(FakeClient(), env)
