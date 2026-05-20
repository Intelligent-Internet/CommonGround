from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import secrets
from threading import Lock
from typing import Any, Callable, TypeVar

from psycopg import errors
from psycopg.rows import dict_row

from CommonGround.contracts import ConflictError, ForbiddenError
from CommonGround.infra.postgres_pool import ConnectionFactory, SyncPostgresConnectionProvider, postgres_connection

from .byoa_primitives import (
    BYOA_INVITE_CODE_APPROVAL_MODE,
    BYOA_PROFILE_CONVERSATION_WORKER_V1,
    ByoaInviteApproval,
)


DEFAULT_JOIN_PROFILE_KIND = BYOA_PROFILE_CONVERSATION_WORKER_V1
DEFAULT_JOIN_RUNTIME_KIND = "manual.shell.v1"
DEFAULT_JOIN_EXPIRES_IN_SECONDS = 24 * 60 * 60
DEFAULT_JOIN_MAX_USES = 1
T = TypeVar("T")


AGENT_JOIN_INVITE_SCHEMA_SQL = """
create table if not exists agent_join_invites (
  invite_id text primary key,
  registration_request_id text not null unique,
  join_code_sha256 text not null unique,
  project_id text not null,
  agent_id text not null,
  profile_kind text not null,
  runtime_kind text not null,
  display_name text not null,
  description text null,
  issued_by_user_id text not null,
  expires_at timestamptz not null,
  max_uses integer not null default 1,
  use_count integer not null default 0,
  disabled_at timestamptz null,
  last_redeemed_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint agent_join_invites_max_uses_check check (max_uses > 0),
  constraint agent_join_invites_use_count_check check (use_count >= 0)
)
"""


@dataclass(frozen=True, slots=True)
class AgentJoinInvite:
    invite_id: str
    registration_request_id: str
    project_id: str
    agent_id: str
    profile_kind: str
    runtime_kind: str
    display_name: str
    description: str | None
    issued_by_user_id: str
    join_code_sha256: str
    expires_at: datetime
    max_uses: int
    use_count: int
    disabled_at: datetime | None
    last_redeemed_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def single_use(self) -> bool:
        return self.max_uses == 1

    def to_approval(self) -> ByoaInviteApproval:
        return ByoaInviteApproval(
            invite_id=self.invite_id,
            issued_by_user_id=self.issued_by_user_id,
            approval_mode=BYOA_INVITE_CODE_APPROVAL_MODE,
        )

    def to_public_payload(self) -> dict[str, Any]:
        return {
            "invite_id": self.invite_id,
            "project_id": self.project_id,
            "agent_id": self.agent_id,
            "profile_kind": self.profile_kind,
            "runtime_kind": self.runtime_kind,
            "display_name": self.display_name,
            "description": self.description,
            "issued_by_user_id": self.issued_by_user_id,
            "expires_at": self.expires_at.isoformat(),
            "single_use": self.single_use,
            "max_uses": self.max_uses,
            "use_count": self.use_count,
            "disabled": self.disabled_at is not None,
            "last_redeemed_at": None if self.last_redeemed_at is None else self.last_redeemed_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def ensure_agent_join_invite_schema(
    pg_dsn: str | None,
    *,
    connection_provider: SyncPostgresConnectionProvider | None = None,
    connection_factory: ConnectionFactory | None = None,
) -> None:
    """Create product-layer Agent join invite tables outside Kernel truth."""

    with postgres_connection(
        pg_dsn,
        connection_provider=connection_provider,
        connection_factory=connection_factory,
    ) as conn, conn.cursor() as cur:
        cur.execute(AGENT_JOIN_INVITE_SCHEMA_SQL)


class AgentJoinInviteStore:
    def __init__(
        self,
        pg_dsn: str | None,
        *,
        ensure_schema: bool = True,
        connection_provider: SyncPostgresConnectionProvider | None = None,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        if not pg_dsn and connection_provider is None and connection_factory is None:
            raise ValueError("pg_dsn or connection provider is required")
        self._pg_dsn = pg_dsn
        self._connection_provider = connection_provider
        self._connection_factory = connection_factory
        self._ensure_schema = ensure_schema
        self._schema_checked = False
        self._schema_lock = Lock()

    def _connect(self):
        return postgres_connection(
            self._pg_dsn,
            connection_provider=self._connection_provider,
            connection_factory=self._connection_factory,
        )

    def _ensure_schema_once(self) -> None:
        if not self._ensure_schema or self._schema_checked:
            return
        with self._schema_lock:
            if self._schema_checked:
                return
            ensure_agent_join_invite_schema(
                self._pg_dsn,
                connection_provider=self._connection_provider,
                connection_factory=self._connection_factory,
            )
            self._schema_checked = True

    def create_invite(
        self,
        *,
        project_id: str,
        agent_id: str,
        issued_by_user_id: str,
        profile_kind: str = DEFAULT_JOIN_PROFILE_KIND,
        runtime_kind: str = DEFAULT_JOIN_RUNTIME_KIND,
        display_name: str | None = None,
        description: str | None = None,
        expires_in_seconds: int = DEFAULT_JOIN_EXPIRES_IN_SECONDS,
        max_uses: int = DEFAULT_JOIN_MAX_USES,
        now: datetime | None = None,
    ) -> tuple[AgentJoinInvite, str]:
        project_id = _required_string("project_id", project_id)
        agent_id = _required_string("agent_id", agent_id)
        issued_by_user_id = _required_string("issued_by_user_id", issued_by_user_id)
        profile_kind = _required_string("profile_kind", profile_kind)
        if profile_kind != DEFAULT_JOIN_PROFILE_KIND:
            raise ConflictError("Agent join invite currently supports conversation worker profiles only")
        runtime_kind = _required_string("runtime_kind", runtime_kind)
        display_name = _required_string("display_name", display_name or _display_name_from_agent_id(agent_id))
        description = _optional_string(description)
        expires_in_seconds = _positive_int("expires_in_seconds", expires_in_seconds)
        max_uses = _positive_int("max_uses", max_uses)
        now = _ensure_aware_utc(now or datetime.now(UTC))
        expires_at = now + timedelta(seconds=expires_in_seconds)
        self._ensure_schema_once()

        for _ in range(3):
            invite_id = _new_invite_id()
            registration_request_id = _new_registration_request_id()
            join_code = _new_join_code()
            try:
                with self._connect() as conn:
                    try:
                        with conn.cursor(row_factory=dict_row) as cur:
                            cur.execute(
                                """
                                insert into agent_join_invites (
                                  invite_id, registration_request_id, join_code_sha256, project_id, agent_id,
                                  profile_kind, runtime_kind, display_name, description,
                                  issued_by_user_id, expires_at, max_uses
                                )
                                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                returning *
                                """,
                                (
                                    invite_id,
                                    registration_request_id,
                                    _sha256_string(join_code),
                                    project_id,
                                    agent_id,
                                    profile_kind,
                                    runtime_kind,
                                    display_name,
                                    description,
                                    issued_by_user_id,
                                    expires_at,
                                    max_uses,
                                ),
                            )
                            row = cur.fetchone()
                        conn.commit()
                        return _invite_from_row(row), join_code
                    except Exception:
                        conn.rollback()
                        raise
            except errors.UniqueViolation:
                continue
        raise ConflictError("unable to allocate unique Agent join invite")

    def reserve_redeem(self, join_code: str, *, now: datetime | None = None) -> AgentJoinInvite:
        invite, _ = self.redeem(join_code, issue=lambda invite: None, now=now)
        return invite

    def redeem(
        self,
        join_code: str,
        *,
        issue: Callable[[AgentJoinInvite], T],
        now: datetime | None = None,
    ) -> tuple[AgentJoinInvite, T]:
        join_code = _required_string("join_code", join_code)
        code_hash = _sha256_string(join_code)
        now = _ensure_aware_utc(now or datetime.now(UTC))
        self._ensure_schema_once()
        with self._connect() as conn:
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        select *
                        from agent_join_invites
                        where join_code_sha256 = %s
                        for update
                        """,
                        (code_hash,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise ForbiddenError("Agent join code is invalid")
                    invite = _invite_from_row(row)
                    if invite.disabled_at is not None:
                        raise ForbiddenError("Agent join code is disabled")
                    if invite.expires_at <= now:
                        raise ForbiddenError("Agent join code is expired")
                    if invite.use_count >= invite.max_uses:
                        raise ForbiddenError("Agent join code has already been redeemed")
                    issued = issue(invite)
                    cur.execute(
                        """
                        update agent_join_invites
                        set use_count = use_count + 1,
                            last_redeemed_at = now(),
                            updated_at = now()
                        where invite_id = %s
                        returning *
                        """,
                        (invite.invite_id,),
                    )
                    updated = cur.fetchone()
                conn.commit()
                return _invite_from_row(updated), issued
            except Exception:
                conn.rollback()
                raise


def _invite_from_row(row: Any) -> AgentJoinInvite:
    if row is None:
        raise ConflictError("Agent join invite row is missing")
    return AgentJoinInvite(
        invite_id=row["invite_id"],
        registration_request_id=row["registration_request_id"],
        project_id=row["project_id"],
        agent_id=row["agent_id"],
        profile_kind=row["profile_kind"],
        runtime_kind=row["runtime_kind"],
        display_name=row["display_name"],
        description=row["description"],
        issued_by_user_id=row["issued_by_user_id"],
        join_code_sha256=row["join_code_sha256"],
        expires_at=_ensure_aware_utc(row["expires_at"]),
        max_uses=row["max_uses"],
        use_count=row["use_count"],
        disabled_at=None if row["disabled_at"] is None else _ensure_aware_utc(row["disabled_at"]),
        last_redeemed_at=None if row["last_redeemed_at"] is None else _ensure_aware_utc(row["last_redeemed_at"]),
        created_at=_ensure_aware_utc(row["created_at"]),
        updated_at=_ensure_aware_utc(row["updated_at"]),
    )


def _display_name_from_agent_id(agent_id: str) -> str:
    return agent_id.replace("-", " ").replace("_", " ").strip().title() or agent_id


def _new_invite_id() -> str:
    return "aginv_" + secrets.token_urlsafe(12)


def _new_registration_request_id() -> str:
    return "agjoinreq_" + secrets.token_urlsafe(18)


def _new_join_code() -> str:
    return "cgjoin_" + secrets.token_urlsafe(24)


def _sha256_string(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _required_string(field_name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConflictError(f"{field_name} must be non-empty")
    return value.strip()


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConflictError("description must be a string when provided")
    value = value.strip()
    return value or None


def _positive_int(field_name: str, value: Any) -> int:
    if isinstance(value, bool):
        raise ConflictError(f"{field_name} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConflictError(f"{field_name} must be a positive integer") from exc
    if parsed <= 0:
        raise ConflictError(f"{field_name} must be a positive integer")
    return parsed


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "DEFAULT_JOIN_EXPIRES_IN_SECONDS",
    "DEFAULT_JOIN_MAX_USES",
    "DEFAULT_JOIN_PROFILE_KIND",
    "DEFAULT_JOIN_RUNTIME_KIND",
    "AGENT_JOIN_INVITE_SCHEMA_SQL",
    "AgentJoinInvite",
    "AgentJoinInviteStore",
    "ensure_agent_join_invite_schema",
]
