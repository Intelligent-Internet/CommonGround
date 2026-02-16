from __future__ import annotations

from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class ProjectAlreadyExistsError(RuntimeError):
    pass


class ProjectStore(BaseStore):
    """Manage project metadata in public.projects."""

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ):
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def create_project(self, *, project_id: str, title: str, owner_id: str, status: str = "active") -> Dict[str, Any]:
        sql = """
            INSERT INTO public.projects (project_id, title, status, owner_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (project_id) DO NOTHING
            RETURNING *
        """
        row = await self.fetch_one(sql, (project_id, title, status, owner_id))
        if not row:
            raise ProjectAlreadyExistsError(f"project already exists: {project_id}")
        return row

    async def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM public.projects
            WHERE project_id=%s
        """
        return await self.fetch_one(sql, (project_id,))

    async def list_projects(self, *, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM public.projects
            ORDER BY project_id ASC
            LIMIT %s OFFSET %s
        """
        return await self.fetch_all(sql, (limit, offset))

    async def update_project(
        self,
        *,
        project_id: str,
        title: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        fields: list[str] = []
        values: list[Any] = []

        if title is not None:
            fields.append("title=%s")
            values.append(title)
        if status is not None:
            fields.append("status=%s")
            values.append(status)

        if not fields:
            return await self.get_project(project_id)

        sql = f"""
            UPDATE public.projects
            SET {", ".join(fields)}
            WHERE project_id=%s
            RETURNING *
        """
        values.append(project_id)

        return await self.fetch_one(sql, tuple(values))

    async def delete_project(self, project_id: str) -> bool:
        sql = "DELETE FROM public.projects WHERE project_id=%s"
        async with self.pool.connection() as conn:
            res = await conn.execute(sql, (project_id,))
            return res.rowcount == 1
