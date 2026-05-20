from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping

from CommonGround.agent_client import HttpAgentClient
from CommonGround.contracts import AgentRef

from ..adapter.registration_spec import AgentRegistrationSpec, REGISTRATION_SPEC_ENV, decode_registration_spec
from .client_auth import build_agent_client
from .leaf_worker_runner import run_leaf_worker_forever


@dataclass(frozen=True, slots=True)
class LeafWorkerEnv:
    base_url: str
    project_id: str
    agent_id: str
    credential_token: str
    registration_spec: AgentRegistrationSpec
    repo_root: str | None = None
    config_path: str | None = None
    workspace: str | None = None
    provision_turn_id: str | None = None

    def agent_ref(self) -> AgentRef:
        return AgentRef(project_id=self.project_id, agent_id=self.agent_id)


def load_leaf_worker_env(environ: Mapping[str, str] | None = None) -> LeafWorkerEnv:
    env = os.environ if environ is None else environ
    return LeafWorkerEnv(
        base_url=env.get("CG_BASE_URL", "http://127.0.0.1:8000"),
        project_id=_required_env(env, "CG_PROJECT_ID"),
        agent_id=_required_env(env, "CG_AGENT_ID"),
        credential_token=_required_env(env, "CG_AGENT_CREDENTIAL_TOKEN"),
        registration_spec=decode_registration_spec(env.get(REGISTRATION_SPEC_ENV)),
        repo_root=_optional_env(env, "NANOBOT_REPO_ROOT"),
        config_path=_optional_env(env, "NANOBOT_CONFIG_PATH"),
        workspace=_optional_env(env, "NANOBOT_WORKSPACE"),
        provision_turn_id=_optional_env(env, "CG_PROVISION_TURN_ID"),
    )


def register_leaf_worker(client: HttpAgentClient, env: LeafWorkerEnv) -> AgentRef:
    snapshot = client.get_agent(env.agent_ref())
    if snapshot is None:
        raise ValueError(f"leaf agent is not registered: {env.project_id}/{env.agent_id}")
    if snapshot.role != env.registration_spec.role:
        raise ValueError("CG_AGENT_REGISTRATION_SPEC role must match registered leaf agent")
    if _canonical_tuple(snapshot.capabilities) != _canonical_tuple(env.registration_spec.capabilities):
        raise ValueError("CG_AGENT_REGISTRATION_SPEC capabilities must match registered leaf agent")
    if _canonical_tuple(snapshot.grants) != _canonical_tuple(env.registration_spec.grants):
        raise ValueError("CG_AGENT_REGISTRATION_SPEC grants must match registered leaf agent")
    if snapshot.accepts_work != env.registration_spec.accepts_work:
        raise ValueError("CG_AGENT_REGISTRATION_SPEC accepts_work must match registered leaf agent")
    return env.agent_ref()


def run_from_env(
    environ: Mapping[str, str] | None = None,
    *,
    client_factory=HttpAgentClient,
    runner=run_leaf_worker_forever,
) -> None:
    env = load_leaf_worker_env(environ)
    client = build_agent_client(client_factory, base_url=env.base_url, agent=env.agent_ref(), token=env.credential_token)
    try:
        register_leaf_worker(client, env)
    finally:
        client.close()
    runner(
        base_url=env.base_url,
        agent=env.agent_ref(),
        config_path=env.config_path,
        workspace=env.workspace,
        repo_root=env.repo_root,
    )


def main() -> None:
    run_from_env()


def _required_env(env: Mapping[str, str], key: str) -> str:
    value = env.get(key)
    if value is None or value == "":
        raise ValueError(f"{key} is required")
    return value


def _optional_env(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(key)
    return None if value is None or value == "" else value


def _canonical_tuple(values) -> tuple[str, ...]:
    return tuple(sorted(set(values)))


if __name__ == "__main__":
    main()
