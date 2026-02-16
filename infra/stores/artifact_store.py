from __future__ import annotations

import json
from typing import Any, Dict, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class ArtifactStore(BaseStore):
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

    async def create_artifact(
        self,
        *,
        project_id: str,
        artifact_id: str,
        filename: Optional[str],
        mime: Optional[str],
        size: int,
        sha256: Optional[str],
        storage_uri: str,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        sql = """
            INSERT INTO resource.artifacts (
                project_id, artifact_id, filename, mime, size, sha256, storage_uri, metadata, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (project_id, artifact_id) DO UPDATE
            SET filename=EXCLUDED.filename,
                mime=EXCLUDED.mime,
                size=EXCLUDED.size,
                sha256=EXCLUDED.sha256,
                storage_uri=EXCLUDED.storage_uri,
                metadata=COALESCE(resource.artifacts.metadata, '{}'::jsonb) || EXCLUDED.metadata
            RETURNING *
        """
        row = await self.fetch_one(
            sql,
            (
                project_id,
                artifact_id,
                filename,
                mime,
                int(size),
                sha256,
                storage_uri,
                json.dumps(metadata or {}),
            ),
        )
        return dict(row) if row else {}

    async def get_artifact(self, *, project_id: str, artifact_id: str) -> Optional[Dict[str, Any]]:
        sql = """
            SELECT *
            FROM resource.artifacts
            WHERE project_id=%s AND artifact_id=%s
        """
        row = await self.fetch_one(sql, (project_id, artifact_id))
        return dict(row) if row else None
