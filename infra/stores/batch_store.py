import json
from typing import Optional, List, Dict, Any

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore


class BatchStore(BaseStore):
    """Fanout/Join Batch store for PMO orchestration state (L1).

    Schema:
    - state.pmo_batches / state.pmo_batch_tasks (batch_id/batch_task_id)
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ):
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def open(self):
        await super().open()
        row = await self.fetch_one("SELECT to_regclass('state.pmo_batches') IS NOT NULL AS has_batches")
        if not (row and row.get("has_batches")):
            raise RuntimeError(
                "Missing required schema: state.pmo_batches/state.pmo_batch_tasks. "
                "Run scripts/setup/init_db.sql or apply the PMO batch migration."
            )

    async def close(self):
        await super().close()

    def _plans_table(self) -> str:
        return "state.pmo_batches"

    def _tasks_table(self) -> str:
        return "state.pmo_batch_tasks"

    async def insert_batch(
        self,
        *,
        batch_id: str,
        project_id: str,
        parent_agent_id: str,
        parent_agent_turn_id: str,
        parent_tool_call_id: str,
        parent_turn_epoch: int,
        parent_channel_id: Optional[str],
        task_count: int,
        deadline_at: Optional[str],
        metadata: Dict[str, Any],
        conn: Any = None,
    ) -> None:
        sql = f"""
            INSERT INTO {self._plans_table()} (
                batch_id, project_id, parent_agent_id, parent_agent_turn_id,
                parent_tool_call_id, parent_turn_epoch, parent_channel_id,
                task_count, status, deadline_at, metadata, created_at, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'running',%s,%s::jsonb,NOW(),NOW())
        """
        if conn is not None:
            await conn.execute(
                sql,
                (
                    batch_id,
                    project_id,
                    parent_agent_id,
                    parent_agent_turn_id,
                    parent_tool_call_id,
                    parent_turn_epoch,
                    parent_channel_id,
                    task_count,
                    deadline_at,
                    json.dumps(metadata),
                ),
            )
            return
        async with self.pool.connection() as own_conn:
            await own_conn.execute(
                sql,
                (
                    batch_id,
                    project_id,
                    parent_agent_id,
                    parent_agent_turn_id,
                    parent_tool_call_id,
                    parent_turn_epoch,
                    parent_channel_id,
                    task_count,
                    deadline_at,
                    json.dumps(metadata),
                ),
            )

    async def insert_batch_tasks(self, tasks: List[Dict[str, Any]], *, conn: Any = None) -> None:
        sql = f"""
            INSERT INTO {self._tasks_table()} (
                batch_task_id, batch_id, project_id, agent_id,
                status, attempt_count, current_agent_turn_id, current_turn_epoch, payload_hash,
                profile_box_id, context_box_id, output_box_id, error_box_id,
                error_detail, metadata, created_at, updated_at
            )
            VALUES (
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s::jsonb,NOW(),NOW()
            )
        """
        if conn is not None:
            for t in tasks:
                await conn.execute(
                    sql,
                    (
                        t["batch_task_id"],
                        t["batch_id"],
                        t["project_id"],
                        t["agent_id"],
                        t.get("status", "pending"),
                        t.get("attempt_count", 0),
                        t.get("current_agent_turn_id"),
                        t.get("current_turn_epoch"),
                        t.get("payload_hash"),
                        t.get("profile_box_id"),
                        t.get("context_box_id"),
                        t.get("output_box_id"),
                        t.get("error_box_id"),
                        t.get("error_detail"),
                        json.dumps(t.get("metadata") or {}),
                    ),
                )
            return
        async with self.pool.connection() as own_conn:
            for t in tasks:
                await own_conn.execute(
                    sql,
                    (
                        t["batch_task_id"],
                        t["batch_id"],
                        t["project_id"],
                        t["agent_id"],
                        t.get("status", "pending"),
                        t.get("attempt_count", 0),
                        t.get("current_agent_turn_id"),
                        t.get("current_turn_epoch"),
                        t.get("payload_hash"),
                        t.get("profile_box_id"),
                        t.get("context_box_id"),
                        t.get("output_box_id"),
                        t.get("error_box_id"),
                        t.get("error_detail"),
                        json.dumps(t.get("metadata") or {}),
                    ),
                )

    async def list_pending_tasks(self, *, limit: int = 200) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT t.batch_task_id, t.batch_id, t.project_id, t.agent_id,
                   t.profile_box_id, t.context_box_id, t.output_box_id, t.payload_hash, t.attempt_count,
                   t.metadata AS task_metadata,
                   p.parent_agent_id, p.parent_agent_turn_id, p.parent_tool_call_id,
                   p.parent_turn_epoch, p.parent_channel_id, p.metadata AS batch_metadata, p.deadline_at
            FROM {self._tasks_table()} t
            JOIN {self._plans_table()} p
              ON p.batch_id=t.batch_id AND p.project_id=t.project_id
            WHERE t.status='pending'
              AND t.retryable=TRUE
              AND t.next_retry_at <= NOW()
              AND p.status='running'
            ORDER BY t.created_at ASC
            LIMIT %s
        """
        return await self.fetch_all(sql, (limit,))

    async def list_stuck_dispatched_missing_epoch(
        self,
        *,
        older_than_seconds: float,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Dispatched batch tasks that have a turn id but are missing turn_epoch.

        This is used as a reconciliation input when L0 ACK is pending/unknown.
        """
        older = max(0.0, float(older_than_seconds))
        sql = f"""
            SELECT t.batch_task_id, t.batch_id, t.project_id, t.agent_id,
                   t.current_agent_turn_id, t.current_turn_epoch, t.attempt_count, t.updated_at,
                   t.metadata AS task_metadata,
                   p.parent_channel_id,
                   p.deadline_at,
                   CASE
                     WHEN jsonb_typeof(p.metadata->'fail_fast') = 'boolean'
                     THEN (p.metadata->>'fail_fast')::boolean
                     ELSE false
                   END AS fail_fast
            FROM {self._tasks_table()} t
            JOIN {self._plans_table()} p
              ON p.batch_id=t.batch_id AND p.project_id=t.project_id
            WHERE t.status='dispatched'
              AND t.current_agent_turn_id IS NOT NULL
              AND t.current_turn_epoch IS NULL
              AND p.status='running'
              AND t.updated_at < NOW() - (%s * INTERVAL '1 second')
            ORDER BY t.updated_at ASC
            LIMIT %s
        """
        return await self.fetch_all(sql, (older, int(limit)))

    async def claim_task_dispatched(
        self,
        *,
        batch_task_id: str,
        project_id: str,
        agent_turn_id: Optional[str],
        output_box_id: str,
    ) -> bool:
        sql = f"""
            UPDATE {self._tasks_table()}
            SET status='dispatched',
                attempt_count=attempt_count + 1,
                current_agent_turn_id=%s,
                current_turn_epoch=NULL,
                output_box_id=%s,
                updated_at=NOW()
            WHERE batch_task_id=%s
              AND project_id=%s
              AND status='pending'
              AND retryable=TRUE
              AND next_retry_at <= NOW()
        """
        async with self.pool.connection() as conn:
            res = await conn.execute(sql, (agent_turn_id, output_box_id, batch_task_id, project_id))
            return res.rowcount == 1

    async def set_task_turn_identity(
        self,
        *,
        batch_task_id: str,
        project_id: str,
        agent_turn_id: str,
        turn_epoch: int,
    ) -> None:
        sql = f"""
            UPDATE {self._tasks_table()}
            SET current_agent_turn_id=%s,
                current_turn_epoch=%s,
                updated_at=NOW()
            WHERE batch_task_id=%s AND project_id=%s AND status='dispatched'
              AND (current_agent_turn_id IS NULL OR current_agent_turn_id=%s)
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (agent_turn_id, turn_epoch, batch_task_id, project_id, agent_turn_id))

    async def patch_task_metadata(
        self,
        *,
        batch_task_id: str,
        project_id: str,
        metadata_patch: Dict[str, Any],
    ) -> None:
        """JSON-merge into task.metadata (used for observability fields like child trace ids)."""
        patch_json = json.dumps(metadata_patch or {})
        sql = f"""
            UPDATE {self._tasks_table()}
            SET metadata=COALESCE(metadata,'{{}}'::jsonb) || %s::jsonb,
                updated_at=NOW()
            WHERE batch_task_id=%s AND project_id=%s
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (patch_json, batch_task_id, project_id))

    async def set_task_turn_identity_epoch(
        self,
        *,
        batch_task_id: str,
        project_id: str,
        agent_turn_id: str,
        turn_epoch: int,
    ) -> None:
        sql = f"""
            UPDATE {self._tasks_table()}
            SET current_agent_turn_id=%s,
                current_turn_epoch=%s,
                updated_at=NOW()
            WHERE batch_task_id=%s AND project_id=%s AND status='dispatched'
              AND (current_agent_turn_id IS NULL OR current_agent_turn_id=%s)
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (agent_turn_id, turn_epoch, batch_task_id, project_id, agent_turn_id))

    async def set_task_turn_epoch(
        self,
        *,
        batch_task_id: str,
        project_id: str,
        agent_turn_id: str,
        turn_epoch: int,
    ) -> None:
        sql = f"""
            UPDATE {self._tasks_table()}
            SET current_turn_epoch=%s, updated_at=NOW()
            WHERE batch_task_id=%s AND project_id=%s
              AND current_agent_turn_id=%s AND status='dispatched'
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (turn_epoch, batch_task_id, project_id, agent_turn_id))

    async def list_active_turns_for_batch(
        self, *, batch_id: str, project_id: str
    ) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT batch_task_id, agent_id, current_agent_turn_id, current_turn_epoch
            FROM {self._tasks_table()}
            WHERE batch_id=%s AND project_id=%s AND status='dispatched'
              AND current_agent_turn_id IS NOT NULL AND current_turn_epoch IS NOT NULL
        """
        return await self.fetch_all(sql, (batch_id, project_id))

    async def revert_task_to_pending(
        self,
        *,
        batch_task_id: str,
        project_id: str,
        agent_turn_id: Optional[str],
        retry_delay_seconds: float = 0.0,
        error_detail: Optional[str] = None,
        metadata_patch: Optional[Dict[str, Any]] = None,
    ) -> None:
        delay_seconds = max(0.0, float(retry_delay_seconds))
        patch_json = json.dumps(metadata_patch or {})
        if agent_turn_id:
            sql = f"""
                UPDATE {self._tasks_table()}
                SET status='pending',
                    retryable=TRUE,
                    next_retry_at=NOW() + (%s * INTERVAL '1 second'),
                    current_agent_turn_id=NULL,
                    current_turn_epoch=NULL,
                    error_detail=COALESCE(%s, error_detail),
                    metadata=COALESCE(metadata,'{{}}'::jsonb) || %s::jsonb,
                    updated_at=NOW()
                WHERE batch_task_id=%s AND project_id=%s AND current_agent_turn_id=%s
            """
            params = (
                delay_seconds,
                error_detail,
                patch_json,
                batch_task_id,
                project_id,
                agent_turn_id,
            )
        else:
            sql = f"""
                UPDATE {self._tasks_table()}
                SET status='pending',
                    retryable=TRUE,
                    next_retry_at=NOW() + (%s * INTERVAL '1 second'),
                    current_agent_turn_id=NULL,
                    current_turn_epoch=NULL,
                    error_detail=COALESCE(%s, error_detail),
                    metadata=COALESCE(metadata,'{{}}'::jsonb) || %s::jsonb,
                    updated_at=NOW()
                WHERE batch_task_id=%s AND project_id=%s AND current_agent_turn_id IS NULL
            """
            params = (
                delay_seconds,
                error_detail,
                patch_json,
                batch_task_id,
                project_id,
            )
        async with self.pool.connection() as conn:
            await conn.execute(sql, params)

    async def mark_task_dispatch_failed(
        self,
        *,
        batch_task_id: str,
        project_id: str,
        agent_turn_id: Optional[str],
        error_detail: str,
        metadata_patch: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        patch_json = json.dumps(metadata_patch or {})
        if agent_turn_id:
            sql = f"""
                UPDATE {self._tasks_table()}
                SET status='failed',
                    retryable=FALSE,
                    next_retry_at=NOW(),
                    error_detail=%s,
                    metadata=COALESCE(metadata,'{{}}'::jsonb) || %s::jsonb,
                    updated_at=NOW()
                WHERE batch_task_id=%s
                  AND project_id=%s
                  AND current_agent_turn_id=%s
                  AND status='dispatched'
                RETURNING batch_id
            """
            params = (error_detail, patch_json, batch_task_id, project_id, agent_turn_id)
        else:
            sql = f"""
                UPDATE {self._tasks_table()}
                SET status='failed',
                    retryable=FALSE,
                    next_retry_at=NOW(),
                    error_detail=%s,
                    metadata=COALESCE(metadata,'{{}}'::jsonb) || %s::jsonb,
                    updated_at=NOW()
                WHERE batch_task_id=%s
                  AND project_id=%s
                  AND status='pending'
                RETURNING batch_id
            """
            params = (error_detail, patch_json, batch_task_id, project_id)
        row = await self.fetch_one(sql, params)
        return row["batch_id"] if row else None

    async def mark_task_terminal_from_turn(
        self,
        *,
        project_id: str,
        agent_turn_id: str,
        new_status: str,
        output_box_id: Optional[str],
        metadata_patch: Optional[Dict[str, Any]] = None,
        error_detail: Optional[str],
    ) -> Optional[str]:
        patch_json = json.dumps(metadata_patch or {})
        sql = f"""
            UPDATE {self._tasks_table()}
            SET status=%s,
                retryable=FALSE,
                next_retry_at=NOW(),
                output_box_id=COALESCE(%s, output_box_id),
                error_detail=%s,
                metadata=COALESCE(metadata,'{{}}'::jsonb) || %s::jsonb,
                updated_at=NOW()
            WHERE project_id=%s AND current_agent_turn_id=%s
              AND status IN ('pending','dispatched')
            RETURNING batch_id
        """
        row = await self.fetch_one(
            sql,
            (new_status, output_box_id, error_detail, patch_json, project_id, agent_turn_id),
        )
        return row["batch_id"] if row else None

    async def fetch_task_context_by_turn(
        self,
        *,
        project_id: str,
        agent_turn_id: str,
    ) -> Optional[Dict[str, Any]]:
        sql = f"""
            SELECT t.batch_id,
                   t.metadata AS task_metadata,
                   p.metadata AS batch_metadata
            FROM {self._tasks_table()} t
            JOIN {self._plans_table()} p
              ON t.batch_id = p.batch_id AND t.project_id = p.project_id
            WHERE t.project_id=%s AND t.current_agent_turn_id=%s
            LIMIT 1
        """
        return await self.fetch_one(sql, (project_id, agent_turn_id))

    async def fetch_batch(self, *, batch_id: str, project_id: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._plans_table()} WHERE batch_id=%s AND project_id=%s"
        return await self.fetch_one(sql, (batch_id, project_id))

    async def fetch_tasks_for_batch(
        self, *, batch_id: str, project_id: str
    ) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT * FROM {self._tasks_table()}
            WHERE batch_id=%s AND project_id=%s
            ORDER BY created_at ASC
        """
        return await self.fetch_all(sql, (batch_id, project_id))

    async def claim_batch_terminal(
        self,
        *,
        batch_id: str,
        project_id: str,
        final_status: str,
    ) -> bool:
        sql = f"""
            UPDATE {self._plans_table()}
            SET status=%s, updated_at=NOW()
            WHERE batch_id=%s AND project_id=%s AND status='running'
        """
        async with self.pool.connection() as conn:
            res = await conn.execute(sql, (final_status, batch_id, project_id))
            return res.rowcount == 1

    async def abort_remaining_tasks(
        self, *, batch_id: str, project_id: str
    ) -> None:
        sql = f"""
            UPDATE {self._tasks_table()}
            SET status='canceled', updated_at=NOW()
            WHERE batch_id=%s AND project_id=%s
              AND status IN ('pending','dispatched')
        """
        async with self.pool.connection() as conn:
            await conn.execute(sql, (batch_id, project_id))

    async def list_expired_running_batches(self, *, limit: int = 50) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT * FROM {self._plans_table()}
            WHERE status='running' AND deadline_at IS NOT NULL AND deadline_at < NOW()
            ORDER BY deadline_at ASC
            LIMIT %s
        """
        return await self.fetch_all(sql, (limit,))

    async def list_recent_batches_for_project(
        self,
        *,
        project_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT batch_id, project_id, parent_agent_id, parent_agent_turn_id, parent_channel_id,
                   task_count, status, created_at, updated_at
            FROM {self._plans_table()}
            WHERE project_id=%s
            ORDER BY created_at DESC
            LIMIT %s
        """
        return await self.fetch_all(sql, (project_id, max(1, int(limit))))

    async def list_recent_tasks_for_project(
        self,
        *,
        project_id: str,
        limit: int = 80,
    ) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT batch_id, batch_task_id, project_id, agent_id, status, attempt_count,
                   current_agent_turn_id, current_turn_epoch, updated_at, error_detail
            FROM {self._tasks_table()}
            WHERE project_id=%s
            ORDER BY updated_at DESC
            LIMIT %s
        """
        return await self.fetch_all(sql, (project_id, max(1, int(limit))))
