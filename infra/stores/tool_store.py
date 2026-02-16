from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class ToolStore(BaseStore):
    """Manage tool definitions in resource.tools."""

    def __init__(
        self,
        dsn: str | AsyncConnectionPool | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ):
        if pool is None and isinstance(dsn, AsyncConnectionPool):
            pool = dsn
            dsn = None
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def upsert_tool(
        self,
        *,
        project_id: str,
        tool_name: str,
        description: Optional[str],
        target_subject: str,
        after_execution: str,
        parameters: Dict[str, Any],
        options: Dict[str, Any],
    ) -> Dict[str, Any]:
        sql = """
            INSERT INTO resource.tools (
                project_id, tool_name, description, parameters, target_subject, after_execution, options
            ) VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s::jsonb)
            ON CONFLICT (project_id, tool_name) DO UPDATE
            SET description=EXCLUDED.description,
                parameters=EXCLUDED.parameters,
                target_subject=EXCLUDED.target_subject,
                after_execution=EXCLUDED.after_execution,
                options=EXCLUDED.options
            RETURNING project_id, tool_name, description, parameters, target_subject, after_execution, options
        """
        row = await self.fetch_one(
            sql,
            (
                project_id,
                tool_name,
                description,
                json.dumps(parameters or {}),
                target_subject,
                after_execution,
                json.dumps(options or {}),
            ),
        )
        if not row:
            raise RuntimeError("upsert resource.tools failed")
        return row

    async def list_tools(self, *, project_id: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
        sql = """
            SELECT project_id, tool_name, description, parameters, target_subject, after_execution, options
            FROM resource.tools
            WHERE project_id=%s
            ORDER BY tool_name ASC
            LIMIT %s OFFSET %s
        """
        rows = await self.fetch_all(sql, (project_id, limit, offset))
        return rows or []

    async def get_tool(self, *, project_id: str, tool_name: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT project_id, tool_name, description, parameters, target_subject, after_execution, options
            FROM resource.tools
            WHERE project_id=%s AND tool_name=%s
        """
        return await self.fetch_one(sql, (project_id, tool_name))

    async def delete_tool(self, *, project_id: str, tool_name: str) -> bool:
        sql = "DELETE FROM resource.tools WHERE project_id=%s AND tool_name=%s"
        async with self.pool.connection() as conn:
            res = await conn.execute(sql, (project_id, tool_name))
            return res.rowcount == 1
