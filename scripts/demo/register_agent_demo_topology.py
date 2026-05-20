from __future__ import annotations

import os
from typing import Protocol

from CommonGround.agent_credentials import AGENT_CREDENTIAL_ISSUE_ANY_GRANT, AGENT_CREDENTIAL_REVOKE_ANY_GRANT
from CommonGround.agent_registration import AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT, AGENT_REGISTRATION_BIRTH_GRANT
from CommonGround.app import build_kernel_app
from CommonGround.contracts import TURN_KIND_CONVERSATION_V1, TURN_KIND_PROVISION_AGENT_SPAWN_V1

from .common_agent_demo import DemoEnv, load_demo_env

TURN_STOP_ANY_GRANT = "turn.stop.any"


class AgentRegistrar(Protocol):
    def register_agent(
        self,
        agent,
        *,
        capabilities: tuple[str, ...] = (),
        accepts_work: bool = True,
        grants: tuple[str, ...] = (),
    ):
        ...


def register_demo_topology(registrar: AgentRegistrar, env: DemoEnv) -> dict[str, str]:
    frontside = registrar.register_agent(env.frontside(), accepts_work=False)
    nanobot_a = registrar.register_agent(
        env.nanobot_a(),
        capabilities=(TURN_KIND_CONVERSATION_V1,),
        grants=(TURN_STOP_ANY_GRANT,),
    )
    provisioner = registrar.register_agent(
        env.nanobot_provisioner(),
        capabilities=(TURN_KIND_PROVISION_AGENT_SPAWN_V1,),
        grants=(
            AGENT_REGISTRATION_BIRTH_GRANT,
            AGENT_ACCEPTS_WORK_UPDATE_ANY_GRANT,
            AGENT_CREDENTIAL_ISSUE_ANY_GRANT,
            AGENT_CREDENTIAL_REVOKE_ANY_GRANT,
        ),
    )
    return {
        "project_id": env.project_id,
        "frontside": frontside.agent_id,
        "nanobot_a": nanobot_a.agent_id,
        "nanobot_provisioner": provisioner.agent_id,
    }


def main() -> None:
    env = load_demo_env()
    pg_dsn = os.environ.get("PG_DSN")
    if not pg_dsn:
        raise ValueError("PG_DSN is required to register the demo topology")
    kernel_app = build_kernel_app(pg_dsn=pg_dsn)
    print(register_demo_topology(kernel_app.topology, env))


if __name__ == "__main__":
    main()
