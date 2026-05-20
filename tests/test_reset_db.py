from __future__ import annotations

import psycopg

from scripts.setup.reset_db import reset_database


def test_reset_database_recreates_admin_service_tables_and_resets_turn_counters(test_pg_dsn: str) -> None:
    reset_database(test_pg_dsn)
    with psycopg.connect(test_pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into cg_project_turn_counters (project_id, last_assigned_turn_seq)
            values ('reset-fixture', 42)
            """
        )
        cur.execute(
            """
            insert into agent_join_invites (
              invite_id, registration_request_id, join_code_sha256, project_id, agent_id,
              profile_kind, runtime_kind, display_name, issued_by_user_id, expires_at
            )
            values (
              'aginv_reset', 'agjoinreq_reset', 'hash-reset', 'reset-fixture', 'agent-a',
              'byoa.conversation_worker.v1', 'manual.shell.v1', 'Agent A', 'tester',
              now() + interval '1 hour'
            )
            """
        )
        conn.commit()

    reset_database(test_pg_dsn)

    with psycopg.connect(test_pg_dsn) as conn, conn.cursor() as cur:
        for table_name in (
            "byoa_registration_requests",
            "byoa_registration_events",
            "agent_join_invites",
            "cg_project_turn_counters",
        ):
            cur.execute("select to_regclass(%s)", (table_name,))
            assert cur.fetchone()[0] == table_name

        cur.execute("select count(*) from cg_project_turn_counters")
        assert cur.fetchone()[0] == 0
        cur.execute("select count(*) from agent_join_invites")
        assert cur.fetchone()[0] == 0
