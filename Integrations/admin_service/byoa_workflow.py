from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from typing import Any

from psycopg import errors
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb as Json

from CommonGround.agent_registration import AgentBirthSpec, agent_birth_spec_hash, canonical_agent_birth_spec
from CommonGround.contracts import ConflictError, NotFoundError
from CommonGround.infra.postgres_pool import ConnectionFactory, SyncPostgresConnectionProvider, postgres_connection


BYOA_STATUS_SUBMITTED = "submitted"
BYOA_STATUS_VALIDATED = "validated"
BYOA_STATUS_APPROVED = "approved"
BYOA_STATUS_REGISTERING = "registering"
BYOA_STATUS_REGISTERED = "registered"
BYOA_STATUS_REJECTED = "rejected"
BYOA_STATUS_FAILED = "failed"
BYOA_STATUS_CONFLICT_REQUIRES_REVIEW = "conflict_requires_review"

BYOA_STATUSES = (
    BYOA_STATUS_SUBMITTED,
    BYOA_STATUS_VALIDATED,
    BYOA_STATUS_APPROVED,
    BYOA_STATUS_REGISTERING,
    BYOA_STATUS_REGISTERED,
    BYOA_STATUS_REJECTED,
    BYOA_STATUS_FAILED,
    BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
)

ALLOWED_BYOA_TRANSITIONS: Mapping[str, frozenset[str]] = {
    BYOA_STATUS_SUBMITTED: frozenset((BYOA_STATUS_VALIDATED, BYOA_STATUS_REJECTED)),
    BYOA_STATUS_VALIDATED: frozenset((BYOA_STATUS_APPROVED, BYOA_STATUS_REJECTED)),
    BYOA_STATUS_APPROVED: frozenset((BYOA_STATUS_REGISTERING,)),
    BYOA_STATUS_REGISTERING: frozenset(
        (
            BYOA_STATUS_REGISTERED,
            BYOA_STATUS_FAILED,
            BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
        )
    ),
    BYOA_STATUS_FAILED: frozenset((BYOA_STATUS_REGISTERING,)),
    BYOA_STATUS_CONFLICT_REQUIRES_REVIEW: frozenset((BYOA_STATUS_REJECTED, BYOA_STATUS_APPROVED)),
    BYOA_STATUS_REGISTERED: frozenset(),
    BYOA_STATUS_REJECTED: frozenset(),
}

DEFAULT_BYOA_PROVENANCE_KIND = "admin_service.byoa_registration.v1"
BYOA_REGISTRATION_REQUESTS_SCHEMA_SQL = """
create table if not exists byoa_registration_requests (
  request_id text primary key,
  project_id text not null,
  requested_agent_id text not null,
  requester_user_id text not null,
  creator_user_id text null,
  status text not null,
  raw_request jsonb not null,
  raw_request_hash text not null,
  admitted_spec jsonb null,
  admitted_spec_hash text null,
  provenance_kind text null,
  provenance_external_ref text null,
  provenance_payload_hash text null,
  registered_agent_id text null,
  registered_at timestamptz null,
  attempt_count integer not null default 0,
  last_error_code text null,
  last_error_message text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint byoa_registration_requests_status_check check (
    status in (
      'submitted',
      'validated',
      'approved',
      'registering',
      'registered',
      'rejected',
      'failed',
      'conflict_requires_review'
    )
  ),
  constraint byoa_registration_requests_project_agent_unique unique (project_id, requested_agent_id)
)
"""
BYOA_REGISTRATION_EVENTS_SCHEMA_SQL = """
create table if not exists byoa_registration_events (
  event_id bigserial primary key,
  request_id text not null references byoa_registration_requests(request_id),
  event_type text not null,
  actor_kind text not null,
  actor_id text not null,
  from_status text null,
  to_status text null,
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
)
"""


@dataclass(frozen=True, slots=True)
class ByoaRegistrationRequest:
    request_id: str
    project_id: str
    requested_agent_id: str
    requester_user_id: str
    creator_user_id: str | None
    status: str
    raw_request: Any
    raw_request_hash: str
    admitted_spec: Mapping[str, Any] | None
    admitted_spec_hash: str | None
    provenance_kind: str | None
    provenance_external_ref: str | None
    provenance_payload_hash: str | None
    registered_agent_id: str | None
    registered_at: datetime | None
    attempt_count: int
    last_error_code: str | None
    last_error_message: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ByoaRegistrationEvent:
    event_id: int
    request_id: str
    event_type: str
    actor_kind: str
    actor_id: str
    from_status: str | None
    to_status: str | None
    details: Mapping[str, Any]
    created_at: datetime


def ensure_byoa_workflow_schema(
    pg_dsn: str | None,
    *,
    connection_provider: SyncPostgresConnectionProvider | None = None,
    connection_factory: ConnectionFactory | None = None,
) -> None:
    """Create Admin Service BYOA workflow tables outside the Kernel schema."""

    with postgres_connection(
        pg_dsn,
        connection_provider=connection_provider,
        connection_factory=connection_factory,
    ) as conn, conn.cursor() as cur:
        cur.execute(BYOA_REGISTRATION_REQUESTS_SCHEMA_SQL)
        cur.execute(BYOA_REGISTRATION_EVENTS_SCHEMA_SQL)


def canonical_raw_request_hash(raw_request: Any) -> str:
    return canonical_json_sha256(raw_request)


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


class ByoaWorkflowStore:
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
        if ensure_schema:
            ensure_byoa_workflow_schema(
                pg_dsn,
                connection_provider=connection_provider,
                connection_factory=connection_factory,
            )

    def _connect(self):
        return postgres_connection(
            self._pg_dsn,
            connection_provider=self._connection_provider,
            connection_factory=self._connection_factory,
        )

    def submit_request(
        self,
        request_id: str,
        project_id: str,
        requested_agent_id: str,
        requester_user_id: str,
        raw_request: Any,
        creator_user_id: str | None = None,
        actor_kind: str = "user",
        actor_id: str | None = None,
    ) -> ByoaRegistrationRequest:
        """Create a submitted request.

        Reusing the same request_id with the same canonical raw_request hash is
        idempotent and returns the existing row without appending another event.
        """

        _require_non_empty("request_id", request_id)
        _require_non_empty("project_id", project_id)
        _require_non_empty("requested_agent_id", requested_agent_id)
        _require_non_empty("requester_user_id", requester_user_id)
        if creator_user_id is not None:
            _require_non_empty("creator_user_id", creator_user_id)
        actor_id = requester_user_id if actor_id is None else actor_id
        _require_non_empty("actor_kind", actor_kind)
        _require_non_empty("actor_id", actor_id)
        request_hash = canonical_raw_request_hash(raw_request)

        with self._connect() as conn:
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        select *
                        from byoa_registration_requests
                        where request_id = %s
                        for update
                        """,
                        (request_id,),
                    )
                    existing = cur.fetchone()
                    if existing is not None:
                        if existing["raw_request_hash"] != request_hash:
                            raise ConflictError(f"BYOA request idempotency conflict: {request_id}")
                        conn.commit()
                        return _request_from_row(existing)

                    try:
                        cur.execute(
                            """
                            insert into byoa_registration_requests (
                              request_id, project_id, requested_agent_id,
                              requester_user_id, creator_user_id, status,
                              raw_request, raw_request_hash
                            )
                            values (%s, %s, %s, %s, %s, %s, %s, %s)
                            returning *
                            """,
                            (
                                request_id,
                                project_id,
                                requested_agent_id,
                                requester_user_id,
                                creator_user_id,
                                BYOA_STATUS_SUBMITTED,
                                Json(raw_request),
                                request_hash,
                            ),
                        )
                    except errors.UniqueViolation as exc:
                        raise ConflictError(
                            f"BYOA request already exists for project/agent: {project_id}/{requested_agent_id}"
                        ) from exc
                    row = cur.fetchone()
                    _append_event(
                        cur,
                        request_id=request_id,
                        event_type=BYOA_STATUS_SUBMITTED,
                        actor_kind=actor_kind,
                        actor_id=actor_id,
                        from_status=None,
                        to_status=BYOA_STATUS_SUBMITTED,
                        details={"raw_request_hash": request_hash},
                    )
                conn.commit()
                return _request_from_row(row)
            except Exception:
                conn.rollback()
                raise

    def get_request(self, request_id: str) -> ByoaRegistrationRequest | None:
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select *
                from byoa_registration_requests
                where request_id = %s
                """,
                (request_id,),
            )
            row = cur.fetchone()
        return None if row is None else _request_from_row(row)

    def list_events(self, request_id: str) -> tuple[ByoaRegistrationEvent, ...]:
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select *
                from byoa_registration_events
                where request_id = %s
                order by event_id
                """,
                (request_id,),
            )
            rows = cur.fetchall()
        return tuple(_event_from_row(row) for row in rows)

    def transition_request(
        self,
        request_id: str,
        from_statuses: Iterable[str] | str | None = None,
        to_status: str | None = None,
        actor_kind: str = "system",
        actor_id: str = "system",
        event_type: str | None = None,
        details: Mapping[str, Any] | None = None,
        **field_updates: Any,
    ) -> ByoaRegistrationRequest:
        _require_non_empty("request_id", request_id)
        if to_status is None:
            raise ConflictError("to_status must be provided")
        _validate_status(to_status)
        from_status_set = _normalize_from_statuses(from_statuses)
        event_type = to_status if event_type is None else event_type
        _require_non_empty("event_type", event_type)
        _require_non_empty("actor_kind", actor_kind)
        _require_non_empty("actor_id", actor_id)

        with self._connect() as conn:
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    row = _require_request_for_update(cur, request_id)
                    updated = _transition_locked(
                        cur,
                        row,
                        from_statuses=from_status_set,
                        to_status=to_status,
                        actor_kind=actor_kind,
                        actor_id=actor_id,
                        event_type=event_type,
                        details=details,
                        field_updates=field_updates,
                    )
                conn.commit()
                return updated
            except Exception:
                conn.rollback()
                raise

    def claim_next_approved_for_registration(
        self,
        actor_kind: str = "admin_service",
        actor_id: str = "admin-service",
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest | None:
        _require_non_empty("actor_kind", actor_kind)
        _require_non_empty("actor_id", actor_id)

        with self._connect() as conn:
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        """
                        select *
                        from byoa_registration_requests
                        where status = %s
                        order by created_at
                        for update skip locked
                        limit 1
                        """,
                        (BYOA_STATUS_APPROVED,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        conn.commit()
                        return None
                    claimed = _transition_locked(
                        cur,
                        row,
                        from_statuses=frozenset((BYOA_STATUS_APPROVED,)),
                        to_status=BYOA_STATUS_REGISTERING,
                        actor_kind=actor_kind,
                        actor_id=actor_id,
                        event_type=BYOA_STATUS_REGISTERING,
                        details=details,
                        field_updates={
                            "increment_attempt_count": True,
                            "last_error_code": None,
                            "last_error_message": None,
                        },
                    )
                conn.commit()
                return claimed
            except Exception:
                conn.rollback()
                raise

    def validate_request(
        self,
        request_id: str,
        *,
        actor_kind: str = "admin_service",
        actor_id: str = "admin-service",
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        return self.transition_request(
            request_id,
            from_statuses=(BYOA_STATUS_SUBMITTED,),
            to_status=BYOA_STATUS_VALIDATED,
            actor_kind=actor_kind,
            actor_id=actor_id,
            event_type=BYOA_STATUS_VALIDATED,
            details=details,
        )

    def reject_request(
        self,
        request_id: str,
        *,
        actor_kind: str = "user",
        actor_id: str,
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        return self.transition_request(
            request_id,
            from_statuses=(BYOA_STATUS_SUBMITTED, BYOA_STATUS_VALIDATED, BYOA_STATUS_CONFLICT_REQUIRES_REVIEW),
            to_status=BYOA_STATUS_REJECTED,
            actor_kind=actor_kind,
            actor_id=actor_id,
            event_type=BYOA_STATUS_REJECTED,
            details=details,
        )

    def approve_request(
        self,
        request_id: str,
        admitted_spec: AgentBirthSpec | Mapping[str, Any],
        *,
        actor_kind: str = "user",
        actor_id: str | None = None,
        approved_by: str | None = None,
        provenance_kind: str = DEFAULT_BYOA_PROVENANCE_KIND,
        provenance_external_ref: str | None = None,
        provenance_payload_hash: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        admitted_spec_json, admitted_spec_hash = _admitted_spec_json_and_hash(admitted_spec)
        if actor_id is None:
            actor_id = approved_by
        if actor_id is None:
            raise ConflictError("actor_id or approved_by must be provided")
        provenance_external_ref = request_id if provenance_external_ref is None else provenance_external_ref
        _require_non_empty("provenance_kind", provenance_kind)
        _require_non_empty("provenance_external_ref", provenance_external_ref)

        event_details = {
            "admitted_spec_hash": admitted_spec_hash,
            "provenance_kind": provenance_kind,
            "provenance_external_ref": provenance_external_ref,
            "provenance_payload_hash": provenance_payload_hash,
            **dict(details or {}),
        }
        return self.transition_request(
            request_id,
            from_statuses=(BYOA_STATUS_VALIDATED, BYOA_STATUS_CONFLICT_REQUIRES_REVIEW),
            to_status=BYOA_STATUS_APPROVED,
            actor_kind=actor_kind,
            actor_id=actor_id,
            event_type=BYOA_STATUS_APPROVED,
            details=event_details,
            admitted_spec=admitted_spec_json,
            admitted_spec_hash=admitted_spec_hash,
            provenance_kind=provenance_kind,
            provenance_external_ref=provenance_external_ref,
            provenance_payload_hash=provenance_payload_hash,
            last_error_code=None,
            last_error_message=None,
        )

    def mark_registering(
        self,
        request_id: str,
        *,
        actor_kind: str = "admin_service",
        actor_id: str = "admin-service",
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        return self.transition_request(
            request_id,
            from_statuses=(BYOA_STATUS_APPROVED, BYOA_STATUS_FAILED),
            to_status=BYOA_STATUS_REGISTERING,
            actor_kind=actor_kind,
            actor_id=actor_id,
            event_type=BYOA_STATUS_REGISTERING,
            details=details,
            increment_attempt_count=True,
            last_error_code=None,
            last_error_message=None,
        )

    def mark_registered(
        self,
        request_id: str,
        *,
        registered_agent_id: str,
        actor_kind: str = "admin_service",
        actor_id: str = "admin-service",
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        _require_non_empty("registered_agent_id", registered_agent_id)
        return self.transition_request(
            request_id,
            from_statuses=(BYOA_STATUS_REGISTERING,),
            to_status=BYOA_STATUS_REGISTERED,
            actor_kind=actor_kind,
            actor_id=actor_id,
            event_type=BYOA_STATUS_REGISTERED,
            details=details,
            registered_agent_id=registered_agent_id,
            registered_at=True,
            last_error_code=None,
            last_error_message=None,
        )

    def mark_failed(
        self,
        request_id: str,
        *,
        error_code: str,
        error_message: str,
        actor_kind: str = "admin_service",
        actor_id: str = "admin-service",
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        _require_non_empty("error_code", error_code)
        _require_non_empty("error_message", error_message)
        return self.transition_request(
            request_id,
            from_statuses=(BYOA_STATUS_REGISTERING,),
            to_status=BYOA_STATUS_FAILED,
            actor_kind=actor_kind,
            actor_id=actor_id,
            event_type=BYOA_STATUS_FAILED,
            details=details,
            last_error_code=error_code,
            last_error_message=error_message,
        )

    def mark_conflict_requires_review(
        self,
        request_id: str,
        *,
        error_code: str,
        error_message: str,
        actor_kind: str = "admin_service",
        actor_id: str = "admin-service",
        details: Mapping[str, Any] | None = None,
    ) -> ByoaRegistrationRequest:
        _require_non_empty("error_code", error_code)
        _require_non_empty("error_message", error_message)
        return self.transition_request(
            request_id,
            from_statuses=(BYOA_STATUS_REGISTERING,),
            to_status=BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
            actor_kind=actor_kind,
            actor_id=actor_id,
            event_type=BYOA_STATUS_CONFLICT_REQUIRES_REVIEW,
            details=details,
            last_error_code=error_code,
            last_error_message=error_message,
        )


def _transition_locked(
    cur,
    row: Mapping[str, Any],
    *,
    from_statuses: frozenset[str] | None,
    to_status: str,
    actor_kind: str,
    actor_id: str,
    event_type: str,
    details: Mapping[str, Any] | None,
    field_updates: Mapping[str, Any],
) -> ByoaRegistrationRequest:
    from_status = row["status"]
    if from_statuses is not None and from_status not in from_statuses:
        raise ConflictError(f"invalid BYOA transition source: {from_status} -> {to_status}")
    if to_status not in ALLOWED_BYOA_TRANSITIONS.get(from_status, frozenset()):
        raise ConflictError(f"invalid BYOA transition: {from_status} -> {to_status}")

    update_fragments = ["status = %s", "updated_at = now()"]
    params: list[Any] = [to_status]
    _append_field_update_fragments(update_fragments, params, field_updates)
    params.append(row["request_id"])
    cur.execute(
        f"""
        update byoa_registration_requests
        set {", ".join(update_fragments)}
        where request_id = %s
        returning *
        """,
        params,
    )
    updated = cur.fetchone()
    _append_event(
        cur,
        request_id=row["request_id"],
        event_type=event_type,
        actor_kind=actor_kind,
        actor_id=actor_id,
        from_status=from_status,
        to_status=to_status,
        details=dict(details or {}),
    )
    return _request_from_row(updated)


_JSON_REQUEST_COLUMNS = frozenset(("admitted_spec",))
_UPDATABLE_REQUEST_COLUMNS = frozenset(
    (
        "last_error_code",
        "last_error_message",
        "admitted_spec",
        "admitted_spec_hash",
        "provenance_kind",
        "provenance_external_ref",
        "provenance_payload_hash",
        "registered_agent_id",
        "registered_at",
    )
)


def _append_field_update_fragments(
    update_fragments: list[str],
    params: list[Any],
    field_updates: Mapping[str, Any],
) -> None:
    increment_attempt_count = field_updates.get("increment_attempt_count", False)
    if "attempt_count_increment" in field_updates:
        increment_attempt_count = field_updates["attempt_count_increment"]
    if increment_attempt_count:
        delta = 1 if increment_attempt_count is True else int(increment_attempt_count)
        if delta <= 0:
            raise ConflictError("attempt_count increment must be positive")
        update_fragments.append("attempt_count = attempt_count + %s")
        params.append(delta)

    for key, value in field_updates.items():
        if key in ("increment_attempt_count", "attempt_count_increment"):
            continue
        if key not in _UPDATABLE_REQUEST_COLUMNS:
            raise ValueError(f"unsupported BYOA request update field: {key}")
        if key == "registered_at" and value is True:
            update_fragments.append("registered_at = now()")
            continue
        update_fragments.append(f"{key} = %s")
        params.append(Json(value) if key in _JSON_REQUEST_COLUMNS else value)


def _append_event(
    cur,
    *,
    request_id: str,
    event_type: str,
    actor_kind: str,
    actor_id: str,
    from_status: str | None,
    to_status: str | None,
    details: Mapping[str, Any],
) -> None:
    cur.execute(
        """
        insert into byoa_registration_events (
          request_id, event_type, actor_kind, actor_id,
          from_status, to_status, details
        )
        values (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            request_id,
            event_type,
            actor_kind,
            actor_id,
            from_status,
            to_status,
            Json(dict(details)),
        ),
    )


def _require_request_for_update(cur, request_id: str) -> Mapping[str, Any]:
    cur.execute(
        """
        select *
        from byoa_registration_requests
        where request_id = %s
        for update
        """,
        (request_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise NotFoundError(f"BYOA registration request not found: {request_id}")
    return row


def _request_from_row(row: Mapping[str, Any]) -> ByoaRegistrationRequest:
    return ByoaRegistrationRequest(
        request_id=row["request_id"],
        project_id=row["project_id"],
        requested_agent_id=row["requested_agent_id"],
        requester_user_id=row["requester_user_id"],
        creator_user_id=row["creator_user_id"],
        status=row["status"],
        raw_request=row["raw_request"],
        raw_request_hash=row["raw_request_hash"],
        admitted_spec=row["admitted_spec"],
        admitted_spec_hash=row["admitted_spec_hash"],
        provenance_kind=row["provenance_kind"],
        provenance_external_ref=row["provenance_external_ref"],
        provenance_payload_hash=row["provenance_payload_hash"],
        registered_agent_id=row["registered_agent_id"],
        registered_at=row["registered_at"],
        attempt_count=row["attempt_count"],
        last_error_code=row["last_error_code"],
        last_error_message=row["last_error_message"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _event_from_row(row: Mapping[str, Any]) -> ByoaRegistrationEvent:
    return ByoaRegistrationEvent(
        event_id=row["event_id"],
        request_id=row["request_id"],
        event_type=row["event_type"],
        actor_kind=row["actor_kind"],
        actor_id=row["actor_id"],
        from_status=row["from_status"],
        to_status=row["to_status"],
        details=row["details"],
        created_at=row["created_at"],
    )


def _admitted_spec_json_and_hash(admitted_spec: AgentBirthSpec | Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    spec = admitted_spec if isinstance(admitted_spec, AgentBirthSpec) else _agent_birth_spec_from_mapping(admitted_spec)
    canonical_spec = canonical_agent_birth_spec(spec)
    return _agent_birth_spec_to_json(canonical_spec), agent_birth_spec_hash(canonical_spec)


_AGENT_BIRTH_SPEC_FIELDS = frozenset(
    (
        "agent_id",
        "role",
        "description",
        "enabled",
        "accepts_work",
        "capacity",
        "capabilities",
        "grants",
        "public_metadata",
    )
)


def _agent_birth_spec_from_mapping(value: Mapping[str, Any]) -> AgentBirthSpec:
    unknown_fields = set(value) - _AGENT_BIRTH_SPEC_FIELDS
    if unknown_fields:
        raise ConflictError(f"unsupported admitted_spec field(s): {', '.join(sorted(unknown_fields))}")
    agent_id = value.get("agent_id")
    role = value.get("role")
    _require_non_empty("admitted_spec.agent_id", agent_id)
    _require_non_empty("admitted_spec.role", role)
    return AgentBirthSpec(
        agent_id=agent_id,
        role=role,
        description=value.get("description"),
        enabled=bool(value.get("enabled", True)),
        accepts_work=bool(value.get("accepts_work", True)),
        capacity=int(value.get("capacity", 1)),
        capabilities=_string_tuple(value.get("capabilities", ()), "admitted_spec.capabilities"),
        grants=_string_tuple(value.get("grants", ()), "admitted_spec.grants"),
        public_metadata=_metadata_dict(value.get("public_metadata", {})),
    )


def agent_birth_spec_from_admitted_spec(value: Mapping[str, Any]) -> AgentBirthSpec:
    return _agent_birth_spec_from_mapping(value)


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if isinstance(value, str) or not isinstance(value, Iterable):
        raise ConflictError(f"{field_name} must be an iterable of strings")
    result = tuple(value)
    if any(not isinstance(item, str) or not item for item in result):
        raise ConflictError(f"{field_name} must contain only non-empty strings")
    return result


def _metadata_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ConflictError("admitted_spec.public_metadata must be a mapping")
    return dict(value)


def _agent_birth_spec_to_json(spec: AgentBirthSpec) -> dict[str, Any]:
    return {
        "agent_id": spec.agent_id,
        "role": spec.role,
        "description": spec.description,
        "enabled": spec.enabled,
        "accepts_work": spec.accepts_work,
        "capacity": spec.capacity,
        "capabilities": list(spec.capabilities),
        "grants": list(spec.grants),
        "public_metadata": dict(spec.public_metadata),
    }


def _normalize_from_statuses(from_statuses: Iterable[str] | str | None) -> frozenset[str] | None:
    if from_statuses is None:
        return None
    if isinstance(from_statuses, str):
        statuses = (from_statuses,)
    else:
        statuses = tuple(from_statuses)
    for status in statuses:
        _validate_status(status)
    return frozenset(statuses)


def _validate_status(status: str) -> None:
    if status not in BYOA_STATUSES:
        raise ConflictError(f"invalid BYOA status: {status}")


def _require_non_empty(field_name: str, value: str | None) -> None:
    if not isinstance(value, str) or not value:
        raise ConflictError(f"{field_name} must be non-empty")


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


ensure_admin_service_schema = ensure_byoa_workflow_schema
raw_request_hash = canonical_raw_request_hash
BYOARegistrationRequest = ByoaRegistrationRequest
BYOARegistrationEvent = ByoaRegistrationEvent
BYOARegistrationStore = ByoaWorkflowStore
