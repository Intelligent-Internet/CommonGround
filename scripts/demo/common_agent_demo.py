from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from CommonGround.agent_client import HttpAgentClient, agent_auth_headers
from CommonGround.contracts import AgentRef


@dataclass(frozen=True, slots=True)
class DemoEnv:
    base_url: str
    project_id: str
    frontside_agent_id: str = "frontside"
    nanobot_a_agent_id: str = "nanobot_a"
    nanobot_provisioner_agent_id: str = "nanobot_provisioner_alpha"
    nanobot_repo_root: str | None = None
    nanobot_config_path: str | None = None
    nanobot_requester_workspace: str = str(Path.home() / ".nanobot-cg-requester")
    nanobot_a_workspace: str = str(Path.home() / ".nanobot-a")
    nanobot_leaf_workspace: str = str(Path.home() / ".nanobot-leaf")
    nanobot_provisioner_workspace: str = str(Path.home() / ".nanobot-provisioner")

    def frontside(self) -> AgentRef:
        return AgentRef(self.project_id, self.frontside_agent_id)

    def nanobot_a(self) -> AgentRef:
        return AgentRef(self.project_id, self.nanobot_a_agent_id)

    def nanobot_provisioner(self) -> AgentRef:
        return AgentRef(self.project_id, self.nanobot_provisioner_agent_id)


def load_demo_env() -> DemoEnv:
    return DemoEnv(
        base_url=os.environ.get("CG_BASE_URL", "http://127.0.0.1:8000"),
        project_id=os.environ.get("CG_PROJECT_ID", "cg-demo"),
        frontside_agent_id=os.environ.get("CG_FRONTSIDE_AGENT_ID", "frontside"),
        nanobot_a_agent_id=os.environ.get("CG_NANOBOT_A_AGENT_ID", "nanobot_a"),
        nanobot_provisioner_agent_id=os.environ.get("CG_NANOBOT_PROVISIONER_AGENT_ID", "nanobot_provisioner_alpha"),
        nanobot_repo_root=os.environ.get("NANOBOT_REPO_ROOT"),
        nanobot_config_path=os.environ.get("NANOBOT_CONFIG_PATH"),
        nanobot_requester_workspace=os.environ.get("NANOBOT_REQUESTER_WORKSPACE", str(Path.home() / ".nanobot-cg-requester")),
        nanobot_a_workspace=os.environ.get("NANOBOT_A_WORKSPACE", str(Path.home() / ".nanobot-a")),
        nanobot_leaf_workspace=os.environ.get("NANOBOT_LEAF_WORKSPACE", str(Path.home() / ".nanobot-leaf")),
        nanobot_provisioner_workspace=os.environ.get("NANOBOT_PROVISIONER_WORKSPACE", str(Path.home() / ".nanobot-provisioner")),
    )


def make_client(*, agent: AgentRef | None = None) -> HttpAgentClient:
    env = load_demo_env()
    caller = env.frontside() if agent is None else agent
    token = os.environ.get("CG_AGENT_CREDENTIAL_TOKEN")
    if not token:
        raise ValueError("CG_AGENT_CREDENTIAL_TOKEN is required to connect to CommonGround")
    return HttpAgentClient(base_url=env.base_url, headers=agent_auth_headers(caller, token))


def configure_demo_logging() -> None:
    level_name = os.environ.get("CG_DEMO_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )
    http_level_name = os.environ.get("CG_HTTP_LOG_LEVEL", "WARNING").upper()
    http_level = getattr(logging, http_level_name, logging.WARNING)
    for logger_name in ("httpcore", "httpx", "urllib3"):
        logging.getLogger(logger_name).setLevel(http_level)
