from __future__ import annotations

from datetime import datetime

from CommonGround.agent_credentials import (
    hash_agent_credential_secret,
    issue_agent_credential_token,
)
from CommonGround.contracts import (
    AgentCredentialIssueResult,
    AgentCredentialRef,
    AgentCredentialRow,
    AgentRef,
    ConflictError,
    NotFoundError,
)
from CommonGround.infra.postgres_pool import ConnectionFactory, SyncPostgresConnectionProvider, postgres_connection


class PostgresAgentCredentialStore:
    """Service-owned Agent credential repository.

    This store is intentionally not a Kernel truth port. Credentials prove
    service-boundary caller identity; they do not define Kernel authority.
    """

    def __init__(
        self,
        pg_dsn: str | None,
        *,
        connection_provider: SyncPostgresConnectionProvider | None = None,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        if not pg_dsn and connection_provider is None and connection_factory is None:
            raise ValueError("pg_dsn or connection provider is required")
        self._pg_dsn = pg_dsn
        self._connection_provider = connection_provider
        self._connection_factory = connection_factory

    def issue_agent_credential(
        self,
        agent: AgentRef,
        *,
        issued_by_agent_id: str | None = None,
        provenance_kind: str | None = None,
        provenance_ref: str | None = None,
        provenance_payload_hash: str | None = None,
        expires_at: datetime | None = None,
    ) -> AgentCredentialIssueResult:
        token, parsed = issue_agent_credential_token()
        secret_hash = hash_agent_credential_secret(parsed.secret)
        with self._connect() as conn, conn.cursor() as cur:
            self._require_enabled_agent(cur, agent)
            cur.execute(
                """
                insert into cg_agent_credentials (
                  credential_id, project_id, agent_id, secret_hash, status,
                  issued_by_agent_id, provenance_kind, provenance_ref,
                  provenance_payload_hash, expires_at, created_at, updated_at
                )
                values (%s, %s, %s, %s, 'active', %s, %s, %s, %s, %s, now(), now())
                returning credential_id
                """,
                (
                    parsed.credential_id,
                    agent.project_id,
                    agent.agent_id,
                    secret_hash,
                    issued_by_agent_id,
                    provenance_kind,
                    provenance_ref,
                    provenance_payload_hash,
                    expires_at,
                ),
            )
            conn.commit()
        return AgentCredentialIssueResult(
            ref=AgentCredentialRef(
                project_id=agent.project_id,
                agent_id=agent.agent_id,
                credential_id=parsed.credential_id,
            ),
            token=token,
        )

    def load_agent_credential_by_id(self, credential_id: str) -> AgentCredentialRow | None:
        _require_non_empty("credential_id", credential_id)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select credential_id, project_id, agent_id, secret_hash, status,
                       issued_by_agent_id, provenance_kind, provenance_ref,
                       provenance_payload_hash, expires_at, created_at, updated_at,
                       revoked_at, last_used_at
                from cg_agent_credentials
                where credential_id = %s
                """,
                (credential_id,),
            )
            row = cur.fetchone()
        return None if row is None else _credential_row(row)

    def list_agent_credentials(self, agent: AgentRef) -> tuple[AgentCredentialRow, ...]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select credential_id, project_id, agent_id, secret_hash, status,
                       issued_by_agent_id, provenance_kind, provenance_ref,
                       provenance_payload_hash, expires_at, created_at, updated_at,
                       revoked_at, last_used_at
                from cg_agent_credentials
                where project_id = %s and agent_id = %s
                order by created_at asc, credential_id asc
                """,
                (agent.project_id, agent.agent_id),
            )
            rows = cur.fetchall()
        return tuple(_credential_row(row) for row in rows)

    def mark_agent_credential_used(self, credential_id: str) -> AgentCredentialRow:
        _require_non_empty("credential_id", credential_id)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update cg_agent_credentials
                set last_used_at = now(), updated_at = now()
                where credential_id = %s
                  and status = 'active'
                  and (expires_at is null or expires_at > now())
                returning credential_id, project_id, agent_id, secret_hash, status,
                          issued_by_agent_id, provenance_kind, provenance_ref,
                          provenance_payload_hash, expires_at, created_at, updated_at,
                          revoked_at, last_used_at
                """,
                (credential_id,),
            )
            row = cur.fetchone()
            if row is None:
                _raise_missing_or_inactive_credential(cur, credential_id)
            conn.commit()
        return _credential_row(row)

    def revoke_agent_credential(self, credential_id: str) -> AgentCredentialRow:
        _require_non_empty("credential_id", credential_id)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                update cg_agent_credentials
                set status = 'revoked',
                    revoked_at = coalesce(revoked_at, now()),
                    updated_at = now()
                where credential_id = %s
                returning credential_id, project_id, agent_id, secret_hash, status,
                          issued_by_agent_id, provenance_kind, provenance_ref,
                          provenance_payload_hash, expires_at, created_at, updated_at,
                          revoked_at, last_used_at
                """,
                (credential_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise NotFoundError(f"agent credential not found: {credential_id}")
            conn.commit()
        return _credential_row(row)

    def _connect(self):
        return postgres_connection(
            self._pg_dsn,
            connection_provider=self._connection_provider,
            connection_factory=self._connection_factory,
        )

    @staticmethod
    def _require_enabled_agent(cur, agent: AgentRef) -> None:
        cur.execute(
            """
            select enabled
            from cg_agents
            where project_id = %s and agent_id = %s
            """,
            (agent.project_id, agent.agent_id),
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"agent not found: {agent.agent_id}")
        if not row[0]:
            raise ConflictError(f"agent disabled: {agent.agent_id}")


def _credential_row(row) -> AgentCredentialRow:
    return AgentCredentialRow(
        credential_id=row[0],
        project_id=row[1],
        agent_id=row[2],
        secret_hash=row[3],
        status=row[4],
        issued_by_agent_id=row[5],
        provenance_kind=row[6],
        provenance_ref=row[7],
        provenance_payload_hash=row[8],
        expires_at=row[9],
        created_at=row[10],
        updated_at=row[11],
        revoked_at=row[12],
        last_used_at=row[13],
    )


def _raise_missing_or_inactive_credential(cur, credential_id: str) -> None:
    cur.execute(
        """
        select status, expires_at
        from cg_agent_credentials
        where credential_id = %s
        """,
        (credential_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"agent credential not found: {credential_id}")
    raise ConflictError(f"agent credential is not active or is expired: {credential_id}")


def _require_non_empty(field_name: str, value: str | None) -> None:
    if not isinstance(value, str) or not value:
        raise ConflictError(f"{field_name} must be non-empty")


__all__ = ["PostgresAgentCredentialStore"]
