from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from CommonGround.infra import postgres
from Integrations.admin_service.agent_join_invites import AGENT_JOIN_INVITE_SCHEMA_SQL
from Integrations.admin_service.byoa_workflow import (
    BYOA_REGISTRATION_EVENTS_SCHEMA_SQL,
    BYOA_REGISTRATION_REQUESTS_SCHEMA_SQL,
)


DROP_SQL = """
drop table if exists agent_connection_bindings;
drop table if exists agent_join_invites;
drop table if exists byoa_registration_events;
drop table if exists byoa_registration_requests;
drop table if exists sync_queue;
drop table if exists api_logs;
drop table if exists card_box_history_logs;
drop table if exists card_boxes;
drop table if exists card_transformations;
drop table if exists card_operation_logs;
drop table if exists cards;
drop table if exists cg_ledger_scope_index;
drop table if exists cg_kernel_ledger;
drop table if exists cg_spawn_envelopes;
drop table if exists cg_semantic_records;
drop table if exists cg_turns;
drop table if exists cg_project_turn_counters;
drop table if exists cg_agent_credentials;
drop table if exists cg_agents;
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset CommonGround PostgreSQL schema.")
    parser.add_argument(
        "--grant",
        action="append",
        default=[],
        help="Role name to grant table privileges to. Can be passed multiple times.",
    )
    return parser.parse_args()


def _require_pg_dsn() -> str:
    pg_dsn = os.environ.get("PG_DSN")
    if not pg_dsn:
        raise SystemExit("PG_DSN is required")
    return pg_dsn


def _grant_role(cur, *, role: str, database_name: str) -> None:
    role_ident = sql.Identifier(role)
    cur.execute(
        sql.SQL("grant connect on database {} to {}").format(sql.Identifier(database_name), role_ident)
    )
    cur.execute(sql.SQL("grant usage on schema public to {}").format(role_ident))
    cur.execute(
        sql.SQL(
            "grant select, insert, update, delete, truncate, references, trigger "
            "on all tables in schema public to {}"
        ).format(role_ident)
    )
    cur.execute(
        sql.SQL(
            "alter default privileges in schema public grant "
            "select, insert, update, delete, truncate, references, trigger on tables to {}"
        ).format(role_ident)
    )


def reset_database(pg_dsn: str, grants: tuple[str, ...] = ()) -> None:
    dsn_params = conninfo_to_dict(pg_dsn)
    database_name = dsn_params.get("dbname")
    if not database_name:
        raise SystemExit("PG_DSN must include dbname")

    schema_sql = Path(__file__).resolve().parents[2] / "CG-Cardbox" / "cardbox" / "adapters" / "postgres_schema.sql"
    conn = psycopg.connect(pg_dsn, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("select pg_advisory_lock(hashtext(%s))", ("commonground_v3_reset_db",))
            cur.execute(DROP_SQL)
            cur.execute(postgres.SCHEMA_SQL)
            cur.execute(schema_sql.read_text())
            cur.execute(BYOA_REGISTRATION_REQUESTS_SCHEMA_SQL)
            cur.execute(BYOA_REGISTRATION_EVENTS_SCHEMA_SQL)
            cur.execute(AGENT_JOIN_INVITE_SCHEMA_SQL)
            for role in grants:
                _grant_role(cur, role=role, database_name=database_name)
            cur.execute("select pg_advisory_unlock(hashtext(%s))", ("commonground_v3_reset_db",))
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    args = _parse_args()
    reset_database(_require_pg_dsn(), tuple(args.grant))


if __name__ == "__main__":
    main()
