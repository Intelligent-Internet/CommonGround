from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SandboxStore(BaseStore):
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

    async def close(self, *, timeout: float | None = None) -> None:
        await super().close(timeout=timeout)

    async def get_sandbox(self, *, project_id: str, agent_id: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM resource.sandboxes
            WHERE project_id=%s AND agent_id=%s
        """
        row = await self.fetch_one(sql, (project_id, agent_id))
        return dict(row) if row else None

    async def upsert_sandbox(
        self,
        *,
        project_id: str,
        agent_id: str,
        sandbox_id: str,
        domain: Optional[str],
        template: Optional[str],
        status: str,
        expires_at: Optional[datetime],
        hard_expires_at: Optional[datetime],
        locked_by: Optional[str],
        lock_expires_at: Optional[datetime],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        sql = """
            INSERT INTO resource.sandboxes (
                project_id, agent_id, sandbox_id, domain, template, status,
                created_at, last_used_at, expires_at, hard_expires_at,
                locked_by, locked_at, lock_expires_at, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    NOW(), NOW(), %s, %s,
                    %s, %s, %s, %s::jsonb)
            ON CONFLICT (project_id, agent_id) DO UPDATE
            SET sandbox_id=EXCLUDED.sandbox_id,
                domain=EXCLUDED.domain,
                template=EXCLUDED.template,
                status=EXCLUDED.status,
                created_at=NOW(),
                last_used_at=NOW(),
                expires_at=EXCLUDED.expires_at,
                hard_expires_at=EXCLUDED.hard_expires_at,
                locked_by=EXCLUDED.locked_by,
                locked_at=EXCLUDED.locked_at,
                lock_expires_at=EXCLUDED.lock_expires_at,
                metadata=COALESCE(resource.sandboxes.metadata, '{}'::jsonb) || EXCLUDED.metadata
            RETURNING *
        """
        row = await self.fetch_one(
            sql,
            (
                project_id,
                agent_id,
                sandbox_id,
                domain,
                template,
                status,
                expires_at,
                hard_expires_at,
                locked_by,
                _utcnow() if locked_by else None,
                lock_expires_at,
                json.dumps(metadata or {}),
            ),
        )
        return dict(row) if row else {}

    async def touch_sandbox(
        self,
        *,
        project_id: str,
        agent_id: str,
        expires_at: Optional[datetime],
    ) -> None:
        sql = """
            UPDATE resource.sandboxes
            SET last_used_at=NOW(),
                expires_at=%s
            WHERE project_id=%s AND agent_id=%s
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (expires_at, project_id, agent_id))

    async def try_lock(
        self,
        *,
        project_id: str,
        agent_id: str,
        locked_by: str,
        lock_expires_at: datetime,
    ) -> bool:
        sql = """
            UPDATE resource.sandboxes
            SET locked_by=%s, locked_at=NOW(), lock_expires_at=%s
            WHERE project_id=%s AND agent_id=%s
              AND (locked_by IS NULL OR lock_expires_at < NOW() OR locked_by=%s)
        """
        async with self.pool.connection() as conn:
            res = await conn.execute(
                sql,
                (locked_by, lock_expires_at, project_id, agent_id, locked_by),
            )
            return res.rowcount > 0

    async def release_lock(self, *, project_id: str, agent_id: str, locked_by: str) -> None:
        sql = """
            UPDATE resource.sandboxes
            SET locked_by=NULL, locked_at=NULL, lock_expires_at=NULL
            WHERE project_id=%s AND agent_id=%s AND locked_by=%s
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (project_id, agent_id, locked_by))

    async def mark_status(
        self,
        *,
        project_id: str,
        agent_id: str,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        sql = """
            UPDATE resource.sandboxes
            SET status=%s,
                metadata=COALESCE(resource.sandboxes.metadata, '{}'::jsonb) || %s::jsonb
            WHERE project_id=%s AND agent_id=%s
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (status, json.dumps(metadata or {}), project_id, agent_id))

    async def delete_sandbox(self, *, project_id: str, agent_id: str) -> None:
        sql = """
            DELETE FROM resource.sandboxes
            WHERE project_id=%s AND agent_id=%s
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (project_id, agent_id))

    async def list_expired(
        self,
        *,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM resource.sandboxes
            WHERE status='active'
              AND (
                (expires_at IS NOT NULL AND expires_at < NOW())
                OR (hard_expires_at IS NOT NULL AND hard_expires_at < NOW())
              )
            ORDER BY COALESCE(expires_at, hard_expires_at) ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (int(limit),))
        return [dict(row) for row in rows]
