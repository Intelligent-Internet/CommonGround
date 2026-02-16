from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class ProfileStore(BaseStore):
    """Cache of sys.profile CardBox pointers in resource.profiles."""

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

    async def upsert_profile(
        self,
        *,
        project_id: str,
        profile_box_id: str,
        name: str,
        worker_target: str,
        tags: list[str],
        system_prompt_compiled: str,
    ) -> Dict[str, Any]:
        sql = """
            INSERT INTO resource.profiles (
                project_id, profile_box_id, name, worker_target, tags, system_prompt_compiled, updated_at
            ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, NOW())
            ON CONFLICT (project_id, name) DO UPDATE
            SET profile_box_id=EXCLUDED.profile_box_id,
                worker_target=EXCLUDED.worker_target,
                tags=EXCLUDED.tags,
                system_prompt_compiled=EXCLUDED.system_prompt_compiled,
                updated_at=NOW()
            RETURNING project_id, profile_box_id, name, worker_target, tags, system_prompt_compiled, updated_at
        """
        row = await self.fetch_one(
            sql,
            (
                project_id,
                profile_box_id,
                name,
                worker_target,
                json.dumps(tags or []),
                system_prompt_compiled,
            ),
        )
        if not row:
            raise RuntimeError("upsert resource.profiles failed")
        return row

    async def list_profiles(self, *, project_id: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
        sql = """
            SELECT project_id, profile_box_id, name, worker_target, tags, system_prompt_compiled, updated_at
            FROM resource.profiles
            WHERE project_id=%s
            ORDER BY lower(name) ASC
            LIMIT %s OFFSET %s
        """
        rows = await self.fetch_all(sql, (project_id, limit, offset))
        return rows or []

    async def get_profile_by_name(self, *, project_id: str, name: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT project_id, profile_box_id, name, worker_target, tags, system_prompt_compiled, updated_at
            FROM resource.profiles
            WHERE project_id=%s AND lower(name)=lower(%s)
            LIMIT 1
        """
        return await self.fetch_one(sql, (project_id, name))

    async def delete_profile_by_name(self, *, project_id: str, name: str) -> bool:
        sql = """
            DELETE FROM resource.profiles
            WHERE project_id=%s AND lower(name)=lower(%s)
        """
        async with self.pool.connection() as conn:
            res = await conn.execute(sql, (project_id, name))
            return res.rowcount == 1
