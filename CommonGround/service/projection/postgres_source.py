from __future__ import annotations

from CommonGround.contracts import AgentRow, CardBoxRef, CauseRef, LedgerEvent, LedgerFeedPage, TurnOutcome, TurnRow, TurnState
from CommonGround.infra.postgres_pool import ConnectionFactory, SyncPostgresConnectionProvider, postgres_connection


def _required_json_object(value, field_name: str) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    return _normalize_json_object(value, field_name=field_name)


def _normalize_json_object(value: dict[str, object], *, field_name: str) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, inner in value.items():
        if not isinstance(key, str):
            raise ValueError(f"{field_name} keys must be strings")
        normalized[key] = _normalize_json_value(inner, field_name=f"{field_name}.{key}")
    return normalized


def _normalize_json_value(value: object, *, field_name: str) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_normalize_json_value(item, field_name=f"{field_name}[]") for item in value]
    if isinstance(value, dict):
        return _normalize_json_object(value, field_name=field_name)
    raise ValueError(f"{field_name} must be JSON-serializable")


class PostgresProjectionSource:
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

    @property
    def pg_dsn(self) -> str | None:
        return self._pg_dsn

    def _connect(self):
        return postgres_connection(
            self._pg_dsn,
            connection_provider=self._connection_provider,
            connection_factory=self._connection_factory,
        )

    def list_agent_rows(
        self,
        *,
        project_id: str,
        agent_id: str | None = None,
        role: str | None = None,
        capability: str | None = None,
        enabled_only: bool | None = None,
        accepts_work_only: bool | None = None,
        limit: int | None = 100,
    ) -> tuple[AgentRow, ...]:
        clauses = ["project_id = %s"]
        params: list[object] = [project_id]
        if agent_id is not None:
            clauses.append("agent_id = %s")
            params.append(agent_id)
        if role is not None:
            clauses.append("role = %s")
            params.append(role)
        if capability is not None:
            clauses.append("capabilities @> %s::text[]")
            params.append([capability])
        if enabled_only:
            clauses.append("enabled = true")
        if accepts_work_only:
            clauses.append("accepts_work = true")
        query = f"""
            select project_id, agent_id, role, description, enabled, accepts_work, capacity, capabilities, grants, public_metadata,
                   created_at, updated_at, last_seen_at
            from cg_agents
            where {' and '.join(clauses)}
            order by agent_id asc
        """
        if limit is not None:
            query = f"{query}\n limit %s"
            params.append(limit)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return tuple(self._agent_row(row) for row in rows)

    def get_turn_row(self, *, project_id: str, turn_id: str) -> TurnRow | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                       state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                       claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                       created_at, updated_at, closed_at, last_turn_seq
                from cg_turns
                where project_id = %s and turn_id = %s
                """,
                (project_id, turn_id),
            )
            row = cur.fetchone()
        return None if row is None else self._turn_row(row)

    def list_turn_rows(
        self,
        *,
        project_id: str,
        target_agent_id: str | None = None,
        turn_kind: str | None = None,
        state: str | None = None,
        outcome: str | None = None,
        stop_requested_only: bool | None = None,
        limit: int = 100,
    ) -> tuple[TurnRow, ...]:
        clauses = ["project_id = %s"]
        params: list[object] = [project_id]
        if target_agent_id is not None:
            clauses.append("target_agent_id = %s")
            params.append(target_agent_id)
        if turn_kind is not None:
            clauses.append("turn_kind = %s")
            params.append(turn_kind)
        if state is not None:
            clauses.append("state = %s")
            params.append(state)
        if outcome is not None:
            clauses.append("outcome = %s")
            params.append(outcome)
        if stop_requested_only:
            clauses.append("stop_requested = true")
        params.append(limit)
        query = f"""
            select project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                   state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                   claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                   created_at, updated_at, closed_at, last_turn_seq
            from cg_turns
            where {' and '.join(clauses)}
            order by project_turn_seq desc
            limit %s
        """
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return tuple(self._turn_row(row) for row in rows)

    def list_child_turn_rows(
        self,
        *,
        project_id: str,
        parent_turn_id: str,
        limit: int = 100,
    ) -> tuple[TurnRow, ...]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select project_id, turn_id, project_turn_seq, target_agent_id, turn_kind, cause_kind, cause_id,
                       state, outcome, stop_requested, current_claim_agent_id, current_claim_token_hash,
                       claim_expires_at, spawn_key, final_record_role, final_cardbox_ref_project_id, final_cardbox_ref_cardbox_id,
                       created_at, updated_at, closed_at, last_turn_seq
                from cg_turns
                where project_id = %s and cause_kind = 'turn' and cause_id = %s
                order by project_turn_seq asc
                limit %s
                """,
                (project_id, parent_turn_id, limit),
            )
            rows = cur.fetchall()
        return tuple(self._turn_row(row) for row in rows)

    def fetch_project_feed(
        self,
        *,
        project_id: str,
        after_ledger_seq: int = 0,
        limit: int = 100,
    ) -> LedgerFeedPage:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                select ledger_seq, project_id, event_type, subject_kind, subject_id, actor_kind, actor_id,
                       cause_kind, cause_id, created_at, note, annotations,
                       payload_ref_project_id, payload_ref_cardbox_id
                from cg_kernel_ledger
                where project_id = %s and ledger_seq > %s
                order by ledger_seq asc
                limit %s
                """,
                (project_id, after_ledger_seq, limit),
            )
            rows = cur.fetchall()
        items = tuple(self._ledger_event(row) for row in rows)
        next_after = after_ledger_seq if not items else items[-1].ledger_seq
        return LedgerFeedPage(items=items, next_after_ledger_seq=next_after)

    @staticmethod
    def _agent_row(row) -> AgentRow:
        return AgentRow(
            project_id=row[0],
            agent_id=row[1],
            role=row[2],
            description=row[3],
            enabled=row[4],
            accepts_work=row[5],
            capacity=row[6],
            capabilities=tuple(row[7] or ()),
            grants=tuple(row[8] or ()),
            public_metadata=_required_json_object(row[9], "public_metadata"),
            created_at=row[10],
            updated_at=row[11],
            last_seen_at=row[12],
        )

    @staticmethod
    def _turn_row(row) -> TurnRow:
        return TurnRow(
            project_id=row[0],
            turn_id=row[1],
            project_turn_seq=row[2],
            target_agent_id=row[3],
            turn_kind=row[4],
            cause_kind=row[5],
            cause_id=row[6],
            state=TurnState(row[7]),
            outcome=None if row[8] is None else TurnOutcome(row[8]),
            stop_requested=row[9],
            current_claim_agent_id=row[10],
            current_claim_token_hash=row[11],
            claim_expires_at=row[12],
            spawn_key=row[13],
            final_record_role=row[14],
            final_cardbox_ref_project_id=row[15],
            final_cardbox_ref_cardbox_id=row[16],
            created_at=row[17],
            updated_at=row[18],
            closed_at=row[19],
            last_turn_seq=row[20],
        )

    @staticmethod
    def _ledger_event(row) -> LedgerEvent:
        return LedgerEvent(
            ledger_seq=row[0],
            project_id=row[1],
            event_type=row[2],
            subject_kind=row[3],
            subject_id=row[4],
            actor_kind=row[5],
            actor_id=row[6],
            cause=None if row[7] is None or row[8] is None else CauseRef(kind=row[7], id=row[8]),
            created_at=row[9],
            note=row[10],
            annotations=_required_json_object(row[11], "annotations"),
            payload_ref=None if row[12] is None or row[13] is None else CardBoxRef(project_id=row[12], cardbox_id=row[13]),
        )
