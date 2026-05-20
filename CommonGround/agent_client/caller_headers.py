from __future__ import annotations

from CommonGround.contracts.models import AgentRef


def agent_auth_headers(agent: AgentRef, token: str) -> dict[str, str]:
    return {
        "X-CG-Project-Id": agent.project_id,
        "X-CG-Agent-Id": agent.agent_id,
        "Authorization": f"Bearer {token}",
    }
