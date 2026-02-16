from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class SkillStore(BaseStore):
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

    async def upsert_skill(
        self,
        *,
        project_id: str,
        skill_name: str,
        description: Optional[str],
        enabled: bool,
        allow_scripts: bool,
        active_version_hash: Optional[str],
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        sql = """
            INSERT INTO resource.skills (
                project_id, skill_name, description, enabled, allow_scripts,
                active_version_hash, metadata, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
            ON CONFLICT (project_id, skill_name) DO UPDATE
            SET description=EXCLUDED.description,
                enabled=EXCLUDED.enabled,
                allow_scripts=EXCLUDED.allow_scripts,
                active_version_hash=COALESCE(EXCLUDED.active_version_hash, resource.skills.active_version_hash),
                metadata=COALESCE(resource.skills.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                updated_at=NOW()
            RETURNING *
        """
        row = await self.fetch_one(
            sql,
            (
                project_id,
                skill_name,
                description,
                bool(enabled),
                bool(allow_scripts),
                active_version_hash,
                json.dumps(metadata or {}),
            ),
        )
        return dict(row) if row else {}

    async def set_enabled(
        self,
        *,
        project_id: str,
        skill_name: str,
        enabled: bool,
    ) -> Optional[Dict[str, Any]]:
        sql = """
            UPDATE resource.skills
            SET enabled=%s, updated_at=NOW()
            WHERE project_id=%s AND skill_name=%s
            RETURNING *
        """
        row = await self.fetch_one(sql, (bool(enabled), project_id, skill_name))
        return dict(row) if row else None

    async def set_active_version(
        self,
        *,
        project_id: str,
        skill_name: str,
        version_hash: str,
    ) -> Optional[Dict[str, Any]]:
        sql = """
            UPDATE resource.skills
            SET active_version_hash=%s, updated_at=NOW()
            WHERE project_id=%s AND skill_name=%s
            RETURNING *
        """
        row = await self.fetch_one(sql, (version_hash, project_id, skill_name))
        return dict(row) if row else None

    async def get_skill(self, *, project_id: str, skill_name: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM resource.skills
            WHERE project_id=%s AND skill_name=%s
        """
        row = await self.fetch_one(sql, (project_id, skill_name))
        return dict(row) if row else None

    async def list_skills(self, *, project_id: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM resource.skills
            WHERE project_id=%s
            ORDER BY skill_name ASC
            LIMIT %s OFFSET %s
        """
        rows = await self.fetch_all(sql, (project_id, limit, offset))
        return [dict(r) for r in rows or []]

    async def add_skill_version(
        self,
        *,
        project_id: str,
        skill_name: str,
        version_hash: str,
        storage_uri: str,
        description: Optional[str],
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        sql = """
            INSERT INTO resource.skill_versions (
                project_id, skill_name, version_hash, storage_uri, description, metadata, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (project_id, skill_name, version_hash) DO UPDATE
            SET storage_uri=EXCLUDED.storage_uri,
                description=EXCLUDED.description,
                metadata=COALESCE(resource.skill_versions.metadata, '{}'::jsonb) || EXCLUDED.metadata
            RETURNING *
        """
        row = await self.fetch_one(
            sql,
            (
                project_id,
                skill_name,
                version_hash,
                storage_uri,
                description,
                json.dumps(metadata or {}),
            ),
        )
        return dict(row) if row else {}

    async def get_skill_version(
        self,
        *,
        project_id: str,
        skill_name: str,
        version_hash: str,
    ) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM resource.skill_versions
            WHERE project_id=%s AND skill_name=%s AND version_hash=%s
        """
        row = await self.fetch_one(sql, (project_id, skill_name, version_hash))
        return dict(row) if row else None

    async def list_skill_versions(
        self,
        *,
        project_id: str,
        skill_name: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM resource.skill_versions
            WHERE project_id=%s AND skill_name=%s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        rows = await self.fetch_all(sql, (project_id, skill_name, limit, offset))
        return [dict(r) for r in rows or []]

    async def delete_skill(self, *, project_id: str, skill_name: str) -> bool:
        async with self.pool.connection() as conn:
            await conn.execute(
                "DELETE FROM resource.skill_versions WHERE project_id=%s AND skill_name=%s",
                (project_id, skill_name),
            )
            res = await conn.execute(
                "DELETE FROM resource.skills WHERE project_id=%s AND skill_name=%s",
                (project_id, skill_name),
            )
            return res.rowcount > 0
