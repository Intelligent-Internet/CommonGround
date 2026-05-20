from __future__ import annotations

from CommonGround.agent_credentials import AGENT_CREDENTIAL_ISSUE_ANY_GRANT, AGENT_CREDENTIAL_REVOKE_ANY_GRANT
from CommonGround.agent_registration import AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT, AGENT_REGISTRATION_BIRTH_GRANT
from CommonGround.contracts import AgentRef, TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1
from scripts.demo.common_agent_demo import DemoEnv
from scripts.demo.register_agent_demo_topology import TURN_STOP_ANY_GRANT, register_demo_topology


class _FakeClient:
    def __init__(self) -> None:
        self.calls = []

    def register_agent(
        self,
        agent: AgentRef,
        *,
        capabilities: tuple[str, ...] = (),
        accepts_work: bool = True,
        grants: tuple[str, ...] = (),
        meta=None,
    ) -> AgentRef:
        del meta
        self.calls.append(
            {
                "agent": agent,
                "capabilities": capabilities,
                "accepts_work": accepts_work,
                "grants": grants,
            }
        )
        return agent


def test_demo_topology_registers_dynamic_provisioner() -> None:
    client = _FakeClient()
    env = DemoEnv(base_url="http://127.0.0.1:8000", project_id="demo")

    result = register_demo_topology(client, env)

    assert result == {
        "project_id": "demo",
        "frontside": "frontside",
        "nanobot_a": "nanobot_a",
        "nanobot_provisioner": "nanobot_provisioner_alpha",
    }
    assert client.calls == [
        {
            "agent": env.frontside(),
            "capabilities": (),
            "accepts_work": False,
            "grants": (),
        },
        {
            "agent": env.nanobot_a(),
            "capabilities": (TURN_KIND_CONVERSATION_V1,),
            "accepts_work": True,
            "grants": (TURN_STOP_ANY_GRANT,),
        },
        {
            "agent": env.nanobot_provisioner(),
            "capabilities": (TURN_KIND_PROVISION_AGENT_SPAWN_V1,),
            "accepts_work": True,
            "grants": (
                AGENT_REGISTRATION_BIRTH_GRANT,
                AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT,
                AGENT_CREDENTIAL_ISSUE_ANY_GRANT,
                AGENT_CREDENTIAL_REVOKE_ANY_GRANT,
            ),
        },
    ]
