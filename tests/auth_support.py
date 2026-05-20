from __future__ import annotations

from CommonGround.agent_client import agent_auth_headers
from CommonGround.contracts import AgentRef
from CommonGround.infra import PostgresAgentCredentialStore


_current_test_pg_dsn: str | None = None


def set_current_test_pg_dsn(pg_dsn: str) -> None:
    global _current_test_pg_dsn
    _current_test_pg_dsn = pg_dsn


def agent_headers(agent: AgentRef, pg_dsn: str | None = None) -> dict[str, str]:
    return agent_auth_headers(agent, agent_token(agent, pg_dsn=pg_dsn))


def agent_token(agent: AgentRef, pg_dsn: str | None = None) -> str:
    resolved_pg_dsn = pg_dsn or _current_test_pg_dsn
    if not resolved_pg_dsn:
        raise RuntimeError("test pg dsn is required to issue Agent credential headers")
    return PostgresAgentCredentialStore(resolved_pg_dsn).issue_agent_credential(agent).token


def missing_credential_headers(agent: AgentRef, *, credential_id: str = "cred_missing") -> dict[str, str]:
    return agent_auth_headers(agent, f"cgac_{credential_id}.missing_secret")
