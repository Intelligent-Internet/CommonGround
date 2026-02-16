import json
from typing import Any, Dict, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class SkillTaskStore(BaseStore):
    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ):
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def open(self) -> None:
        await super().open()
        row = await self.fetch_one("SELECT to_regclass('state.skill_task_heads') IS NOT NULL AS has_tasks")
        if not (row and row.get("has_tasks")):
            raise RuntimeError(
                "Missing required schema: state.skill_task_heads. "
                "Run scripts/setup/init_db.sql or apply the skills task migration."
            )
        row = await self.fetch_one("SELECT to_regclass('state.skill_job_heads') IS NOT NULL AS has_jobs")
        if not (row and row.get("has_jobs")):
            raise RuntimeError(
                "Missing required schema: state.skill_job_heads. "
                "Run scripts/setup/init_db.sql or apply the skills job migration."
            )

    async def upsert_task(
        self,
        *,
        task_id: str,
        project_id: str,
        status: str,
        payload: Dict[str, Any],
    ) -> None:
        sql = """
            INSERT INTO state.skill_task_heads (task_id, project_id, status, payload, created_at, updated_at)
            VALUES (%s, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (task_id)
            DO UPDATE SET
                status = EXCLUDED.status,
                payload = EXCLUDED.payload,
                updated_at = NOW()
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (task_id, project_id, status, json.dumps(payload or {})))

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        row = await self.fetch_one(
            "SELECT payload FROM state.skill_task_heads WHERE task_id=%s",
            (task_id,),
        )
        if not row:
            return None
        return row.get("payload")

    async def list_tasks_by_status(
        self,
        *,
        statuses: list[str],
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if not statuses:
            return []
        sql = """
            SELECT task_id, payload
            FROM state.skill_task_heads
            WHERE status = ANY(%s)
            ORDER BY updated_at DESC
            LIMIT %s OFFSET %s
        """
        rows = await self.fetch_all(sql, (statuses, limit, offset))
        return [dict(row) for row in rows or []]

    async def upsert_job(
        self,
        *,
        job_id: str,
        project_id: str,
        status: str,
        payload: Dict[str, Any],
    ) -> None:
        sql = """
            INSERT INTO state.skill_job_heads (job_id, project_id, status, payload, created_at, updated_at)
            VALUES (%s, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (job_id)
            DO UPDATE SET
                status = EXCLUDED.status,
                payload = EXCLUDED.payload,
                updated_at = NOW()
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (job_id, project_id, status, json.dumps(payload or {})))

    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = await self.fetch_one(
            "SELECT payload FROM state.skill_job_heads WHERE job_id=%s",
            (job_id,),
        )
        if not row:
            return None
        return row.get("payload")
