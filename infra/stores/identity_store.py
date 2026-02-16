from __future__ import annotations

import json
from typing import Any, Dict, Optional

import uuid6
from psycopg_pool import AsyncConnectionPool

from .base import BaseStore

class IdentityStore(BaseStore):
    """Identity edges store (state.identity_edges)."""

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ) -> None:
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def open(self) -> None:
        await super().open()
        row = await self.fetch_one("SELECT to_regclass('state.identity_edges') IS NOT NULL AS has_edges")
        if not (row and row.get("has_edges")):
            raise RuntimeError(
                "Missing required schema: state.identity_edges. "
                "Run scripts/setup/init_db.sql or apply the v1r2 identity migration."
            )

    async def close(self, *, timeout: float = 5.0) -> None:
        await super().close(timeout=timeout)

    async def insert_identity_edge(
        self,
        *,
        project_id: str,
        channel_id: Optional[str],
        action: str,
        source_agent_id: Optional[str],
        target_agent_id: str,
        trace_id: Optional[str],
        parent_step_id: Optional[str],
        metadata: Optional[Dict[str, Any]] = None,
        edge_id: Optional[str] = None,
        conn: Any = None,
    ) -> str:
        edge_id = edge_id or f"edge_{uuid6.uuid7().hex}"
        sql = """
            INSERT INTO state.identity_edges (
                edge_id, project_id, channel_id, action,
                source_agent_id, target_agent_id, trace_id, parent_step_id,
                metadata, created_at
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s::jsonb, NOW()
            )
        """
        meta_json = json.dumps(metadata or {})
        if conn is not None:
            await conn.execute(
                sql,
                (
                    edge_id,
                    project_id,
                    channel_id,
                    action,
                    source_agent_id,
                    target_agent_id,
                    trace_id,
                    parent_step_id,
                    meta_json,
                ),
            )
            return edge_id
        async with self.pool.connection() as own_conn:
            await own_conn.execute(
                sql,
                (
                    edge_id,
                    project_id,
                    channel_id,
                    action,
                    source_agent_id,
                    target_agent_id,
                    trace_id,
                    parent_step_id,
                    meta_json,
                ),
            )
        return edge_id

    async def list_identity_edges(
        self,
        *,
        project_id: str,
        channel_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        owner: Optional[str] = None,
        worker_target: Optional[str] = None,
        tag: Optional[str] = None,
        action: Optional[str] = None,
        trace_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        limit: int = 200,
    ) -> list[Dict[str, Any]]:
        sql = """
            SELECT e.edge_id,
                   e.project_id,
                   e.channel_id,
                   e.action,
                   e.source_agent_id,
                   e.target_agent_id,
                   e.trace_id,
                   e.parent_step_id,
                   e.metadata,
                   e.created_at
            FROM state.identity_edges e
            LEFT JOIN resource.project_agents r
              ON r.project_id = e.project_id AND r.agent_id = e.target_agent_id
            WHERE e.project_id=%s
        """
        params: list[Any] = [project_id]

        ch = (channel_id or "").strip()
        if ch:
            sql += " AND e.channel_id=%s"
            params.append(ch)
        action_value = (action or "").strip()
        if action_value:
            sql += " AND e.action=%s"
            params.append(action_value)
        trace_value = (trace_id or "").strip()
        if trace_value:
            sql += " AND e.trace_id=%s"
            params.append(trace_value)
        parent_value = (parent_step_id or "").strip()
        if parent_value:
            sql += " AND e.parent_step_id=%s"
            params.append(parent_value)
        agent = (agent_id or "").strip()
        if agent:
            sql += " AND (e.source_agent_id=%s OR e.target_agent_id=%s)"
            params.extend([agent, agent])
        owner_value = (owner or "").strip()
        if owner_value:
            sql += " AND r.owner_agent_id=%s"
            params.append(owner_value)
        worker_target_value = (worker_target or "").strip()
        if worker_target_value:
            sql += " AND r.worker_target=%s"
            params.append(worker_target_value)
        tag_value = (tag or "").strip()
        if tag_value:
            sql += " AND r.tags @> %s::jsonb"
            params.append(json.dumps([tag_value]))

        sql += " ORDER BY e.created_at DESC LIMIT %s"
        params.append(max(1, int(limit)))

        rows = await self.fetch_all(sql, tuple(params))
        return [dict(row) for row in rows]
