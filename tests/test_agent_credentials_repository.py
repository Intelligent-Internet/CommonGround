from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from CommonGround.agent_credentials import parse_agent_credential_token, verify_agent_credential_secret
from CommonGround.contracts import (
    AGENT_CREDENTIAL_STATUS_ACTIVE,
    AGENT_CREDENTIAL_STATUS_REVOKED,
    AgentRef,
    ConflictError,
    NotFoundError,
)
from CommonGround.infra import PostgresAgentCredentialStore


PROJECT_ID = "credential-project"
AGENT_ID = "credential-agent"
AGENT = AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)


def test_issue_agent_credential_stores_hash_only(test_pg_dsn: str, kernel_app) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)

    issued = store.issue_agent_credential(
        AGENT,
        issued_by_agent_id="admin-service",
        provenance_kind="test_fixture",
        provenance_ref="fixture-001",
        provenance_payload_hash="sha256:fixture",
    )
    parsed = parse_agent_credential_token(issued.token)
    row = store.load_agent_credential_by_id(issued.ref.credential_id)

    assert parsed.credential_id == issued.ref.credential_id
    assert row is not None
    assert row.project_id == PROJECT_ID
    assert row.agent_id == AGENT_ID
    assert row.status == AGENT_CREDENTIAL_STATUS_ACTIVE
    assert row.issued_by_agent_id == "admin-service"
    assert row.provenance_kind == "test_fixture"
    assert row.provenance_ref == "fixture-001"
    assert row.provenance_payload_hash == "sha256:fixture"
    assert row.secret_hash != issued.token
    assert row.secret_hash != parsed.secret
    assert verify_agent_credential_secret(parsed.secret, row.secret_hash) is True
    assert _plaintext_occurrences(test_pg_dsn, parsed.secret) == 0


def test_issue_agent_credential_rejects_missing_or_disabled_agent(test_pg_dsn: str, kernel_app) -> None:
    store = PostgresAgentCredentialStore(test_pg_dsn)

    with pytest.raises(NotFoundError, match="agent not found"):
        store.issue_agent_credential(AGENT)

    kernel_app.topology.register_agent(AGENT, enabled=False)
    with pytest.raises(ConflictError, match="agent disabled"):
        store.issue_agent_credential(AGENT)


def test_revoke_and_list_agent_credentials(test_pg_dsn: str, kernel_app) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    first = store.issue_agent_credential(AGENT)
    second = store.issue_agent_credential(AGENT)

    revoked = store.revoke_agent_credential(first.ref.credential_id)

    assert revoked.status == AGENT_CREDENTIAL_STATUS_REVOKED
    assert revoked.revoked_at is not None
    rows = store.list_agent_credentials(AGENT)
    assert [row.credential_id for row in rows] == [first.ref.credential_id, second.ref.credential_id]
    assert [row.status for row in rows] == [AGENT_CREDENTIAL_STATUS_REVOKED, AGENT_CREDENTIAL_STATUS_ACTIVE]


def test_mark_agent_credential_used_updates_last_used_at(test_pg_dsn: str, kernel_app) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    issued = store.issue_agent_credential(AGENT)
    before = store.load_agent_credential_by_id(issued.ref.credential_id)
    assert before is not None
    assert before.last_used_at is None

    after = store.mark_agent_credential_used(issued.ref.credential_id)

    assert after.last_used_at is not None


def test_mark_agent_credential_used_rejects_inactive_or_expired_rows(test_pg_dsn: str, kernel_app) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    revoked = store.issue_agent_credential(AGENT)
    expired = store.issue_agent_credential(AGENT, expires_at=datetime.now(UTC) - timedelta(seconds=1))

    store.revoke_agent_credential(revoked.ref.credential_id)

    with pytest.raises(ConflictError, match="not active or is expired"):
        store.mark_agent_credential_used(revoked.ref.credential_id)
    with pytest.raises(ConflictError, match="not active or is expired"):
        store.mark_agent_credential_used(expired.ref.credential_id)


def test_expires_at_is_persisted_without_scope_columns(test_pg_dsn: str, kernel_app) -> None:
    kernel_app.topology.register_agent(AGENT)
    store = PostgresAgentCredentialStore(test_pg_dsn)
    expires_at = datetime.now(UTC) + timedelta(days=1)

    issued = store.issue_agent_credential(AGENT, expires_at=expires_at)
    row = store.load_agent_credential_by_id(issued.ref.credential_id)

    assert row is not None
    assert row.expires_at == expires_at
    columns = _credential_columns(test_pg_dsn)
    assert {"scopes", "permissions", "allowed_routes", "allowed_surfaces"}.isdisjoint(columns)


def test_missing_credential_updates_raise_not_found(test_pg_dsn: str) -> None:
    store = PostgresAgentCredentialStore(test_pg_dsn)

    with pytest.raises(NotFoundError, match="agent credential not found"):
        store.revoke_agent_credential("cred_missing")
    with pytest.raises(NotFoundError, match="agent credential not found"):
        store.mark_agent_credential_used("cred_missing")


def _credential_columns(pg_dsn: str) -> set[str]:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_name = 'cg_agent_credentials'
            """
        )
        return {row[0] for row in cur.fetchall()}


def _plaintext_occurrences(pg_dsn: str, plaintext: str) -> int:
    with psycopg.connect(pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            select count(*)
            from cg_agent_credentials
            where secret_hash = %s
               or credential_id = %s
               or provenance_payload_hash = %s
            """,
            (plaintext, plaintext, plaintext),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]
