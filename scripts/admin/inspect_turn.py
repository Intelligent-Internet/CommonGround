"""
Inspect latest agent turn, steps, and tool results.

Usage:
  uv run -m scripts.admin.inspect_turn --project proj_demo_01 --agent-id tool_agent_01
  uv run -m scripts.admin.inspect_turn --project proj_demo_01 --agent-id tool_agent_01 --agent-turn-id turn_xxx
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Iterable, Optional

import psycopg
import toml

from core.utils import REPO_ROOT


def _load_config() -> Dict[str, Any]:
    cfg_path = REPO_ROOT / "config.toml"
    if not cfg_path.exists():
        return {}
    return toml.load(cfg_path)


def _fetch_latest_agent_turn_id(
    conn: psycopg.Connection,
    project_id: str,
    agent_id: str,
) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT agent_turn_id
            FROM state.agent_steps
            WHERE project_id=%s AND agent_id=%s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (project_id, agent_id),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _iter_tool_results(
    conn: psycopg.Connection,
    project_id: str,
    agent_id: str,
    agent_turn_id: str,
) -> Iterable[tuple]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT correlation_id,
                   payload->>'tool_name' AS tool_name,
                   payload->>'status' AS status,
                   payload->>'tool_result_card_id' AS tool_result_card_id,
                   created_at
            FROM state.agent_inbox
            WHERE project_id=%s
              AND agent_id=%s
              AND message_type='tool_result'
              AND payload->>'agent_turn_id'=%s
            ORDER BY created_at DESC
            """,
            (project_id, agent_id, agent_turn_id),
        )
        yield from cur.fetchall()


def _fetch_card_content(conn: psycopg.Connection, card_id: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT content
            FROM public.cards
            WHERE card_id=%s
            """,
            (card_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _summarize_tool_result(content: Dict[str, Any]) -> str:
    if not isinstance(content, dict):
        return "content=non-dict"
    status = content.get("status")
    result = content.get("result") or {}
    if not isinstance(result, dict):
        return f"status={status}"
    stdout = result.get("stdout") or ""
    stderr = result.get("stderr") or ""
    exit_code = result.get("exit_code")
    created = result.get("created_artifacts") or []
    return (
        f"status={status} exit_code={exit_code} "
        f"stdout_len={len(str(stdout))} stderr_len={len(str(stderr))} "
        f"created_artifacts={len(created)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect agent turn/step results")
    parser.add_argument("--project", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--agent-turn-id", help="Optional agent_turn_id")
    parser.add_argument(
        "--run-id",
        dest="agent_turn_id_legacy",
        help="Deprecated alias of --agent-turn-id",
    )
    args = parser.parse_args()

    cfg = _load_config()
    cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
    dsn = cardbox_cfg.get("postgres_dsn", "postgresql://postgres:postgres@localhost:5433/cardbox")

    with psycopg.connect(dsn) as conn:
        agent_turn_id = (
            args.agent_turn_id
            or args.agent_turn_id_legacy
            or _fetch_latest_agent_turn_id(conn, args.project, args.agent_id)
        )
        if not agent_turn_id:
            raise SystemExit("no turns found for agent")

        print(f"agent_turn_id: {agent_turn_id}")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT step_id, status, started_at, ended_at, error, tool_call_ids
                FROM state.agent_steps
                WHERE project_id=%s AND agent_id=%s AND agent_turn_id=%s
                ORDER BY started_at ASC, step_id ASC
                """,
                (args.project, args.agent_id, agent_turn_id),
            )
            rows = cur.fetchall()
            print(f"steps: {len(rows)}")
            for row in rows:
                step_id, status, started_at, ended_at, error, tool_call_ids = row
                print(
                    f"- step_id={step_id} status={status} started_at={started_at} "
                    f"ended_at={ended_at} error={error} tool_call_ids={tool_call_ids}"
                )

        print("tool_results:")
        for correlation_id, tool_name, status, card_id, created_at in _iter_tool_results(
            conn,
            args.project,
            args.agent_id,
            str(agent_turn_id),
        ):
            summary = ""
            if card_id:
                content = _fetch_card_content(conn, card_id)
                if isinstance(content, dict):
                    summary = _summarize_tool_result(content)
            print(
                f"- tool_call_id={correlation_id} tool={tool_name} status={status} "
                f"card_id={card_id} created_at={created_at} {summary}"
            )


if __name__ == "__main__":
    main()
