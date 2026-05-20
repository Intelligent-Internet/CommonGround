from __future__ import annotations

import hashlib
import os
import re

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from scripts.setup.reset_db import reset_database


DEFAULT_TEST_DBNAME = "cg_v3_testdb"
DEFAULT_TEST_BASE_DSN = f"postgresql://postgres@localhost:5432/{DEFAULT_TEST_DBNAME}"
DEFAULT_ADMIN_DBNAME = "postgres"


def get_test_base_dsn() -> str:
    return os.environ.get("TEST_PG_DSN_BASE") or os.environ.get("PG_DSN", DEFAULT_TEST_BASE_DSN)


def derive_test_db_name(*, base_dsn: str, run_id: str, worker_id: str) -> str:
    # Keep names deterministic and below Postgres' 63-character limit.
    base_name = _sanitize_identifier(_require_dbname(base_dsn), fallback=DEFAULT_TEST_DBNAME)
    run_name = _sanitize_identifier(run_id, fallback="run")
    worker_name = _sanitize_identifier(worker_id, fallback="gw0")
    digest = hashlib.sha1(f"{base_name}:{run_id}:{worker_id}".encode("utf-8")).hexdigest()[:12]
    return f"cgtest_{base_name[:20]}_{run_name[:8]}_{worker_name[:8]}_{digest}"


def build_test_pg_dsn(*, base_dsn: str, db_name: str) -> str:
    params = conninfo_to_dict(base_dsn)
    params["dbname"] = db_name
    return make_conninfo(**params)


def ensure_test_database_exists(pg_dsn: str) -> None:
    database_name = _require_dbname(pg_dsn)
    conn = _connect(_admin_dsn(pg_dsn), autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("select 1 from pg_database where datname = %s", (database_name,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("create database {}").format(sql.Identifier(database_name)))
    finally:
        conn.close()


def drop_test_database(pg_dsn: str) -> None:
    database_name = _require_dbname(pg_dsn)
    conn = _connect(_admin_dsn(pg_dsn), autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("select 1 from pg_database where datname = %s", (database_name,))
            if cur.fetchone() is None:
                return
            cur.execute(
                """
                select pg_terminate_backend(pid)
                from pg_stat_activity
                where datname = %s and pid <> pg_backend_pid()
                """,
                (database_name,),
            )
            cur.execute(sql.SQL("drop database {}").format(sql.Identifier(database_name)))
    finally:
        conn.close()


def reset_test_db(pg_dsn: str) -> None:
    reset_database(pg_dsn)


def _admin_dsn(pg_dsn: str) -> str:
    params = conninfo_to_dict(pg_dsn)
    params["dbname"] = os.environ.get("TEST_PG_ADMIN_DBNAME", DEFAULT_ADMIN_DBNAME)
    return make_conninfo(**params)


def _connect(pg_dsn: str, *, autocommit: bool):
    return psycopg.connect(pg_dsn, autocommit=autocommit)


def _require_dbname(pg_dsn: str) -> str:
    db_name = conninfo_to_dict(pg_dsn).get("dbname")
    if not db_name:
        raise ValueError("PostgreSQL DSN must include dbname")
    return db_name


def _sanitize_identifier(value: str, *, fallback: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
    return cleaned or fallback
