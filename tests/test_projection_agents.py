from __future__ import annotations

from CommonGround.agent_registration import AGENT_REGISTRATION_BIRTH_GRANT
from CommonGround.contracts import AgentRef
from CommonGround.service.projection.agents import list_agent_directory
from CommonGround.service.projection.filters import AgentDirectoryFilters
from CommonGround.service.projection.postgres_source import PostgresProjectionSource

from tests.projection_support import register_agent


PROJECT_ID = "projection-agents"


def test_list_agent_directory_returns_project_agents(kernel_app, test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)
    provisioner = AgentRef(project_id=PROJECT_ID, agent_id="provisioner")
    worker = AgentRef(project_id=PROJECT_ID, agent_id="worker")
    register_agent(
        kernel_app,
        provisioner,
        role="worker.provisioner.v1",
        description="Provisioner",
        capabilities=("turn.provision.agent.spawn.v1",),
        grants=(AGENT_REGISTRATION_BIRTH_GRANT,),
        public_metadata={"ui": {"label": "Provisioner"}},
    )
    register_agent(
        kernel_app,
        worker,
        role="worker.runtime.v1",
        description="Worker",
        capabilities=("turn.conversation.v1",),
        accepts_work=False,
    )

    page = list_agent_directory(source, project_id=PROJECT_ID, filters=AgentDirectoryFilters(limit=100))

    assert [item.agent_id for item in page.items] == ["provisioner", "worker"]
    assert page.items[0].grants == (AGENT_REGISTRATION_BIRTH_GRANT,)
    assert page.items[1].grants is None


def test_list_agent_directory_supports_role_and_capability_filters(kernel_app, test_pg_dsn: str) -> None:
    source = PostgresProjectionSource(test_pg_dsn)
    register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="provisioner"),
        role="worker.provisioner.v1",
        capabilities=("turn.provision.agent.spawn.v1",),
    )
    register_agent(
        kernel_app,
        AgentRef(project_id=PROJECT_ID, agent_id="runtime"),
        role="worker.runtime.v1",
        capabilities=("turn.conversation.v1",),
    )

    by_role = list_agent_directory(
        source,
        project_id=PROJECT_ID,
        filters=AgentDirectoryFilters(role="worker.runtime.v1", limit=100),
    )
    by_capability = list_agent_directory(
        source,
        project_id=PROJECT_ID,
        filters=AgentDirectoryFilters(capability="turn.provision.agent.spawn.v1", limit=100),
    )

    assert [item.agent_id for item in by_role.items] == ["runtime"]
    assert [item.agent_id for item in by_capability.items] == ["provisioner"]
