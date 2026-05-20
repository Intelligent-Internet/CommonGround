from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Mapping

from CommonGround.contracts import AgentRef

from .self_root_frontside_runner import run_self_root_frontside_worker_forever


@dataclass(frozen=True, slots=True)
class SelfRootFrontsideEnv:
    base_url: str
    project_id: str
    agent_id: str
    credential_token: str

    def agent_ref(self) -> AgentRef:
        return AgentRef(project_id=self.project_id, agent_id=self.agent_id)


def load_self_root_frontside_env(environ: Mapping[str, str] | None = None) -> SelfRootFrontsideEnv:
    env = os.environ if environ is None else environ
    return SelfRootFrontsideEnv(
        base_url=env.get("CG_BASE_URL", "http://127.0.0.1:8000"),
        project_id=_required_env(env, "CG_PROJECT_ID"),
        agent_id=_required_env(env, "CG_AGENT_ID"),
        credential_token=_required_env(env, "CG_AGENT_CREDENTIAL_TOKEN"),
    )


def run_from_env(
    environ: Mapping[str, str] | None = None,
    *,
    runner=run_self_root_frontside_worker_forever,
) -> None:
    env = load_self_root_frontside_env(environ)
    runner(
        base_url=env.base_url,
        agent=env.agent_ref(),
        credential_token=env.credential_token,
    )


def main() -> None:
    run_from_env()


def _required_env(env: Mapping[str, str], key: str) -> str:
    value = env.get(key)
    if value is None or value == "":
        raise ValueError(f"{key} is required")
    return value


if __name__ == "__main__":
    main()
