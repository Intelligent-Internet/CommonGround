import json
from typing import Optional, List, Any, Dict

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class StepStore(BaseStore):
    """Step-level audit store (agent_steps)."""

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ):
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def insert_step(
        self,
        *,
        project_id: str,
        agent_id: str,
        step_id: str,
        agent_turn_id: str,
        channel_id: Optional[str],
        parent_step_id: Optional[str],
        trace_id: Optional[str],
        status: str,
        profile_box_id: Optional[str],
        context_box_id: Optional[str],
        output_box_id: Optional[str],
    ) -> None:
        sql = """
            INSERT INTO state.agent_steps (
                project_id, agent_id, step_id, agent_turn_id, channel_id, parent_step_id, trace_id, status,
                profile_box_id, context_box_id, output_box_id, started_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (project_id, agent_id, step_id) DO NOTHING
        """
        async with self.pool.connection() as conn:
            await conn.execute(
                sql,
                (
                    project_id,
                    agent_id,
                    step_id,
                    agent_turn_id,
                    channel_id,
                    parent_step_id,
                    trace_id,
                    status,
                    profile_box_id,
                    context_box_id,
                    output_box_id,
                ),
            )

    async def update_step(
        self,
        *,
        project_id: str,
        agent_id: str,
        step_id: str,
        status: Optional[str] = None,
        request_box_id: Optional[str] = None,
        api_log_id: Optional[int] = None,
        tool_call_ids: Optional[List[str]] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        ended: bool = False,
    ) -> None:
        clauses: List[str] = []
        params: List[Any] = []

        if status:
            clauses.append("status=%s")
            params.append(status)
        if request_box_id is not None:
            clauses.append("request_box_id=%s")
            params.append(request_box_id)
        if api_log_id is not None:
            clauses.append("api_log_id=%s")
            params.append(api_log_id)
        if tool_call_ids is not None:
            clauses.append("tool_call_ids=%s::jsonb")
            params.append(json.dumps(tool_call_ids))
        if error is not None:
            clauses.append("error=%s")
            params.append(error)
        if metadata is not None:
            clauses.append("metadata=COALESCE(metadata, '{}'::jsonb) || %s::jsonb")
            params.append(json.dumps(metadata))
        if ended:
            clauses.append("ended_at=NOW()")

        if not clauses:
            return

        sql = f"""
            UPDATE state.agent_steps
            SET {', '.join(clauses)}
            WHERE project_id=%s AND agent_id=%s AND step_id=%s
        """
        params.extend([project_id, agent_id, step_id])

        async with self.pool.connection() as conn:
            await conn.execute(sql, tuple(params))

    async def count_react_steps(self, *, project_id: str, agent_turn_id: str) -> int:
        """Count LLM/ReAct steps for an agent_turn_id (excludes tool resume steps).

        We use step_id prefixes to distinguish:
        - ReAct/LLM steps:     step_{uuid...}
        - Tool resume steps:   step_res_{...}
        - Watchdog reaps:      step_reap_{...}
        """
        sql = """
            SELECT COUNT(*) AS n
            FROM state.agent_steps
            WHERE project_id=%s AND agent_turn_id=%s
              AND step_id LIKE 'step_%%'
              AND step_id NOT LIKE 'step_res_%%'
              AND step_id NOT LIKE 'step_reap_%%'
        """
        row = await self.fetch_one(sql, (project_id, agent_turn_id))
        try:
            return int((row or {}).get("n") or 0)
        except Exception:
            return 0

    async def list_agent_steps(
        self,
        *,
        project_id: str,
        channel_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        agent_turn_id: Optional[str] = None,
        step_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        status: Optional[str] = None,
        started_after: Any = None,
        started_before: Any = None,
        cursor_started_at: Any = None,
        cursor_step_id: Optional[str] = None,
        order: str = "desc",
        limit: int = 80,
    ) -> List[Dict[str, Any]]:
        order_value = (order or "desc").strip().lower()
        if order_value not in ("asc", "desc"):
            order_value = "desc"

        sql = """
            SELECT project_id,
                   agent_id,
                   agent_turn_id::text AS agent_turn_id,
                   step_id::text AS step_id,
                   status,
                   error,
                   started_at,
                   ended_at,
                   channel_id,
                   parent_step_id,
                   trace_id
            FROM state.agent_steps
            WHERE project_id=%s
        """
        params: List[Any] = [project_id]

        ch = (channel_id or "").strip()
        if ch:
            sql += " AND channel_id=%s"
            params.append(ch)
        agent = (agent_id or "").strip()
        if agent:
            sql += " AND agent_id=%s"
            params.append(agent)
        turn = (agent_turn_id or "").strip()
        if turn:
            sql += " AND agent_turn_id=%s"
            params.append(turn)
        sid = (step_id or "").strip()
        if sid:
            sql += " AND step_id=%s"
            params.append(sid)
        parent = (parent_step_id or "").strip()
        if parent:
            sql += " AND parent_step_id=%s"
            params.append(parent)
        trace = (trace_id or "").strip()
        if trace:
            sql += " AND trace_id=%s"
            params.append(trace)
        status_value = (status or "").strip()
        if status_value:
            sql += " AND status=%s"
            params.append(status_value)
        if started_after:
            sql += " AND started_at >= %s"
            params.append(started_after)
        if started_before:
            sql += " AND started_at <= %s"
            params.append(started_before)

        cursor_sid = (cursor_step_id or "").strip()
        if cursor_started_at and cursor_sid:
            if order_value == "desc":
                sql += " AND (started_at < %s OR (started_at = %s AND step_id < %s))"
            else:
                sql += " AND (started_at > %s OR (started_at = %s AND step_id > %s))"
            params.extend([cursor_started_at, cursor_started_at, cursor_sid])

        order_sql = "DESC" if order_value == "desc" else "ASC"
        sql += f" ORDER BY started_at {order_sql} NULLS LAST, step_id {order_sql} LIMIT %s"
        params.append(max(1, int(limit)))

        rows = await self.fetch_all(sql, tuple(params))
        return [dict(row) for row in rows]
