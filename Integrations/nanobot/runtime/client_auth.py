from __future__ import annotations

import os

from CommonGround.agent_client import agent_auth_headers
from CommonGround.contracts import AgentRef


def build_agent_client(client_factory, *, base_url: str, agent: AgentRef, token: str | None = None):
    resolved_token = token or os.environ.get("CG_AGENT_CREDENTIAL_TOKEN")
    if not resolved_token:
        raise ValueError("CG_AGENT_CREDENTIAL_TOKEN is required to connect to CommonGround")
    return client_factory(base_url=base_url, headers=agent_auth_headers(agent, resolved_token))
