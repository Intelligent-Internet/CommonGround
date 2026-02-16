import json
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool

from .base import BaseStore

@dataclass(frozen=True, slots=True)
class InboxInsert:
    inbox_id: str
    project_id: str
    agent_id: str
    message_type: str
    enqueue_mode: Optional[str]
    correlation_id: Optional[str]
    recursion_depth: int
    traceparent: str
    tracestate: Optional[str]
    trace_id: Optional[str]
    parent_step_id: Optional[str]
    source_agent_id: Optional[str]
    source_step_id: Optional[str]
    payload: Dict[str, Any]
    status: str = "pending"


@dataclass(frozen=True, slots=True)
class ExecutionEdgeInsert:
    edge_id: str
    project_id: str
    channel_id: Optional[str]
    primitive: str
    edge_phase: str
    source_agent_id: Optional[str]
    source_agent_turn_id: Optional[str]
    source_step_id: Optional[str]
    target_agent_id: Optional[str]
    target_agent_turn_id: Optional[str]
    correlation_id: Optional[str]
    enqueue_mode: Optional[str]
    recursion_depth: Optional[int]
    trace_id: Optional[str]
    parent_step_id: Optional[str]
    metadata: Dict[str, Any]


class ExecutionStore(BaseStore):
    """Execution inbox + audit edge store (v1r2)."""

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
        row = await self.fetch_one("SELECT to_regclass('state.agent_inbox') IS NOT NULL AS has_inbox")
        if not (row and row.get("has_inbox")):
            raise RuntimeError(
                "Missing required schema: state.agent_inbox. "
                "Run scripts/setup/init_db.sql or apply the v1r2 inbox migration."
            )
        row = await self.fetch_one("SELECT to_regclass('state.execution_edges') IS NOT NULL AS has_edges")
        if not (row and row.get("has_edges")):
            raise RuntimeError(
                "Missing required schema: state.execution_edges. "
                "Run scripts/setup/init_db.sql or apply the v1r2 inbox migration."
            )

    async def close(self, *, timeout: float | None = None) -> None:
        await super().close(timeout=timeout)

    async def insert_inbox(
        self,
        *,
        inbox_id: str,
        project_id: str,
        agent_id: str,
        message_type: str,
        enqueue_mode: Optional[str],
        correlation_id: Optional[str],
        recursion_depth: int,
        traceparent: str,
        tracestate: Optional[str],
        trace_id: Optional[str],
        parent_step_id: Optional[str],
        source_agent_id: Optional[str],
        source_step_id: Optional[str],
        payload: Dict[str, Any],
        status: str = "pending",
    ) -> None:
        record = InboxInsert(
            inbox_id=inbox_id,
            project_id=project_id,
            agent_id=agent_id,
            message_type=message_type,
            enqueue_mode=enqueue_mode,
            correlation_id=correlation_id,
            recursion_depth=recursion_depth,
            traceparent=traceparent,
            tracestate=tracestate,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            source_agent_id=source_agent_id,
            source_step_id=source_step_id,
            payload=payload,
            status=status,
        )
        async with self.pool.connection() as conn:
            await self._insert_inbox_with_conn(conn, record)

    async def _insert_inbox_with_conn(self, conn: Any, record: InboxInsert) -> bool:
        sql = """
            INSERT INTO state.agent_inbox (
                inbox_id, project_id, agent_id, message_type, enqueue_mode,
                correlation_id, recursion_depth, traceparent, tracestate, trace_id,
                parent_step_id, source_agent_id, source_step_id, payload, status, created_at
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s::jsonb,%s,NOW()
            )
            ON CONFLICT DO NOTHING
            RETURNING inbox_id
        """
        row = await self.fetch_one(
            sql,
            (
                record.inbox_id,
                record.project_id,
                record.agent_id,
                record.message_type,
                record.enqueue_mode,
                record.correlation_id,
                int(record.recursion_depth),
                record.traceparent,
                record.tracestate,
                record.trace_id,
                record.parent_step_id,
                record.source_agent_id,
                record.source_step_id,
                json.dumps(record.payload or {}),
                record.status,
            ),
            conn=conn,
        )
        return bool(row)

    async def insert_execution_edge(
        self,
        *,
        edge_id: str,
        project_id: str,
        channel_id: Optional[str],
        primitive: str,
        edge_phase: str,
        source_agent_id: Optional[str],
        source_agent_turn_id: Optional[str],
        source_step_id: Optional[str],
        target_agent_id: Optional[str],
        target_agent_turn_id: Optional[str],
        correlation_id: Optional[str],
        enqueue_mode: Optional[str],
        recursion_depth: Optional[int],
        trace_id: Optional[str],
        parent_step_id: Optional[str],
        metadata: Dict[str, Any],
    ) -> None:
        record = ExecutionEdgeInsert(
            edge_id=edge_id,
            project_id=project_id,
            channel_id=channel_id,
            primitive=primitive,
            edge_phase=edge_phase,
            source_agent_id=source_agent_id,
            source_agent_turn_id=source_agent_turn_id,
            source_step_id=source_step_id,
            target_agent_id=target_agent_id,
            target_agent_turn_id=target_agent_turn_id,
            correlation_id=correlation_id,
            enqueue_mode=enqueue_mode,
            recursion_depth=recursion_depth,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            metadata=metadata,
        )
        async with self.pool.connection() as conn:
            await self._insert_execution_edge_with_conn(conn, record)

    async def insert_execution_edge_with_conn(
        self,
        conn: Any,
        *,
        edge_id: str,
        project_id: str,
        channel_id: Optional[str],
        primitive: str,
        edge_phase: str,
        source_agent_id: Optional[str],
        source_agent_turn_id: Optional[str],
        source_step_id: Optional[str],
        target_agent_id: Optional[str],
        target_agent_turn_id: Optional[str],
        correlation_id: Optional[str],
        enqueue_mode: Optional[str],
        recursion_depth: Optional[int],
        trace_id: Optional[str],
        parent_step_id: Optional[str],
        metadata: Dict[str, Any],
    ) -> None:
        record = ExecutionEdgeInsert(
            edge_id=edge_id,
            project_id=project_id,
            channel_id=channel_id,
            primitive=primitive,
            edge_phase=edge_phase,
            source_agent_id=source_agent_id,
            source_agent_turn_id=source_agent_turn_id,
            source_step_id=source_step_id,
            target_agent_id=target_agent_id,
            target_agent_turn_id=target_agent_turn_id,
            correlation_id=correlation_id,
            enqueue_mode=enqueue_mode,
            recursion_depth=recursion_depth,
            trace_id=trace_id,
            parent_step_id=parent_step_id,
            metadata=metadata,
        )
        await self._insert_execution_edge_with_conn(conn, record)

    async def _insert_execution_edge_with_conn(self, conn: Any, record: ExecutionEdgeInsert) -> None:
        sql = """
            INSERT INTO state.execution_edges (
                edge_id, project_id, channel_id, primitive, edge_phase,
                source_agent_id, source_agent_turn_id, source_step_id,
                target_agent_id, target_agent_turn_id, correlation_id, enqueue_mode,
                recursion_depth, trace_id, parent_step_id, metadata, created_at
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s::jsonb,NOW()
            )
            ON CONFLICT (project_id, md5(correlation_id), primitive, edge_phase) DO NOTHING
        """
        await conn.execute(
            sql,
            (
                record.edge_id,
                record.project_id,
                record.channel_id,
                record.primitive,
                record.edge_phase,
                record.source_agent_id,
                record.source_agent_turn_id,
                record.source_step_id,
                record.target_agent_id,
                record.target_agent_turn_id,
                record.correlation_id,
                record.enqueue_mode,
                int(record.recursion_depth) if record.recursion_depth is not None else None,
                record.trace_id,
                record.parent_step_id,
                json.dumps(record.metadata or {}),
            ),
        )

    async def enqueue_transactional(
        self,
        *,
        inbox: InboxInsert,
        edge: ExecutionEdgeInsert,
    ) -> None:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await self._insert_inbox_with_conn(conn, inbox)
                await self._insert_execution_edge_with_conn(conn, edge)

    async def enqueue_with_conn(
        self,
        conn: Any,
        *,
        inbox: InboxInsert,
        edge: ExecutionEdgeInsert,
    ) -> None:
        await self._insert_inbox_with_conn(conn, inbox)
        await self._insert_execution_edge_with_conn(conn, edge)

    async def list_pending_inbox(
        self,
        *,
        project_id: str,
        agent_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT inbox_id, project_id, agent_id, message_type, enqueue_mode,
                   correlation_id, recursion_depth, traceparent, tracestate, trace_id,
                   parent_step_id, source_agent_id, source_step_id, payload, status,
                   created_at, processed_at, archived_at
            FROM state.agent_inbox
            WHERE project_id=%s AND agent_id=%s AND status='pending'
            ORDER BY created_at ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (project_id, agent_id, int(limit)))
        return [dict(row) for row in rows]

    async def has_open_inbox(
        self,
        *,
        project_id: str,
        agent_id: str,
        exclude_inbox_id: Optional[str] = None,
    ) -> bool:
        sql = """
            SELECT 1
            FROM state.agent_inbox
            WHERE project_id=%s
              AND agent_id=%s
              AND status IN ('pending', 'processing')
              AND (COALESCE(%s::text, '') = '' OR inbox_id <> %s::text)
            LIMIT 1
        """
        row = await self.fetch_one(
            sql,
            (
                project_id,
                agent_id,
                exclude_inbox_id,
                exclude_inbox_id,
            ),
        )
        return bool(row)

    async def claim_pending_inbox(
        self,
        *,
        project_id: str,
        agent_id: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        sql = """
            WITH candidates AS (
                SELECT inbox_id
                FROM state.agent_inbox
                WHERE project_id=%s AND agent_id=%s AND status='pending'
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE state.agent_inbox
            SET status='processing',
                processed_at=COALESCE(processed_at, NOW())
            WHERE inbox_id IN (SELECT inbox_id FROM candidates)
            RETURNING inbox_id, project_id, agent_id, message_type, enqueue_mode,
                      correlation_id, recursion_depth, traceparent, tracestate, trace_id,
                      parent_step_id, source_agent_id, source_step_id, payload, status,
                      created_at, processed_at, archived_at
        """
        rows = await self.fetch_all(sql, (project_id, agent_id, int(limit)))
        return [dict(row) for row in rows]

    async def list_inbox_by_correlation(
        self,
        *,
        project_id: str,
        agent_id: str,
        correlation_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT inbox_id, project_id, agent_id, message_type, enqueue_mode,
                   correlation_id, recursion_depth, traceparent, tracestate, trace_id,
                   parent_step_id, source_agent_id, source_step_id, payload, status,
                   created_at, processed_at, archived_at
            FROM state.agent_inbox
            WHERE project_id=%s AND agent_id=%s AND correlation_id=%s
            ORDER BY created_at DESC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (project_id, agent_id, correlation_id, int(limit)))
        return [dict(row) for row in rows]

    async def get_request_recursion_depth(
        self,
        *,
        project_id: str,
        correlation_id: str,
    ) -> Optional[int]:
        async with self.pool.connection() as conn:
            return await self.get_request_recursion_depth_with_conn(
                conn,
                project_id=project_id,
                correlation_id=correlation_id,
            )

    async def get_request_recursion_depth_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        correlation_id: str,
    ) -> Optional[int]:
        sql = """
            SELECT recursion_depth
            FROM state.execution_edges
            WHERE project_id=%s
              AND correlation_id=%s
              AND edge_phase='request'
              AND recursion_depth IS NOT NULL
            ORDER BY created_at ASC
            LIMIT 1
        """
        row = await self.fetch_one(sql, (project_id, correlation_id), conn=conn)
        if not row:
            return None
        depth = row.get("recursion_depth")
        return int(depth) if depth is not None else None

    async def get_active_recursion_depth(
        self,
        *,
        project_id: str,
        agent_id: str,
    ) -> Optional[int]:
        async with self.pool.connection() as conn:
            return await self.get_active_recursion_depth_with_conn(
                conn,
                project_id=project_id,
                agent_id=agent_id,
            )

    async def get_active_recursion_depth_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        agent_id: str,
    ) -> Optional[int]:
        sql = """
            SELECT active_recursion_depth
            FROM state.agent_state_head
            WHERE project_id=%s AND agent_id=%s
        """
        row = await self.fetch_one(sql, (project_id, agent_id), conn=conn)
        if not row:
            return None
        depth = row.get("active_recursion_depth")
        return int(depth) if depth is not None else None

    async def list_pending_inbox_by_correlation(
        self,
        *,
        project_id: str,
        agent_id: str,
        correlation_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT inbox_id, project_id, agent_id, message_type, enqueue_mode,
                   correlation_id, recursion_depth, traceparent, tracestate, trace_id,
                   parent_step_id, source_agent_id, source_step_id, payload, status,
                   created_at, processed_at, archived_at
            FROM state.agent_inbox
            WHERE project_id=%s AND agent_id=%s AND correlation_id=%s AND status='pending'
            ORDER BY created_at ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (project_id, agent_id, correlation_id, int(limit)))
        return [dict(row) for row in rows]

    async def claim_pending_inbox_by_correlation(
        self,
        *,
        project_id: str,
        agent_id: str,
        correlation_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        sql = """
            WITH candidates AS (
                SELECT inbox_id
                FROM state.agent_inbox
                WHERE project_id=%s AND agent_id=%s AND correlation_id=%s AND status='pending'
                ORDER BY created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE state.agent_inbox
            SET status='processing',
                processed_at=COALESCE(processed_at, NOW())
            WHERE inbox_id IN (SELECT inbox_id FROM candidates)
            RETURNING inbox_id, project_id, agent_id, message_type, enqueue_mode,
                      correlation_id, recursion_depth, traceparent, tracestate, trace_id,
                      parent_step_id, source_agent_id, source_step_id, payload, status,
                      created_at, processed_at, archived_at
        """
        rows = await self.fetch_all(sql, (project_id, agent_id, correlation_id, int(limit)))
        return [dict(row) for row in rows]

    async def update_inbox_status(
        self,
        *,
        inbox_id: str,
        project_id: str,
        status: str,
        expected_status: Optional[str] = None,
    ) -> bool:
        clauses = ["status=%s"]
        params: List[Any] = [status]
        if status == "pending":
            clauses.append("processed_at=NULL")
            clauses.append("archived_at=NULL")
        if status == "processing":
            clauses.append("processed_at=COALESCE(processed_at, NOW())")
        if status in ("consumed", "error", "skipped"):
            clauses.append("archived_at=COALESCE(archived_at, NOW())")
        sql = f"""
            UPDATE state.agent_inbox
            SET {', '.join(clauses)}
            WHERE project_id=%s AND inbox_id=%s
        """
        params.extend([project_id, inbox_id])
        if expected_status is not None:
            sql += " AND status=%s"
            params.append(expected_status)
        async with self.pool.connection() as conn:
            res = await conn.execute(sql, tuple(params))
            return res.rowcount == 1

    async def requeue_stale_inbox(
        self,
        *,
        timeout_seconds: float,
        limit: int = 200,
    ) -> int:
        if timeout_seconds <= 0:
            return 0
        cutoff = datetime.now(UTC) - timedelta(seconds=float(timeout_seconds))
        sql = """
            WITH candidates AS (
                SELECT inbox_id
                FROM state.agent_inbox
                WHERE status='processing'
                  AND processed_at IS NOT NULL
                  AND processed_at < %s
                ORDER BY processed_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE state.agent_inbox
            SET status='pending',
                processed_at=NULL,
                archived_at=NULL
            WHERE inbox_id IN (SELECT inbox_id FROM candidates)
            RETURNING inbox_id
        """
        rows = await self.fetch_all(sql, (cutoff, int(limit)))
        return len(rows)

    async def list_stale_pending_inbox(
        self,
        *,
        older_than_seconds: float,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        if older_than_seconds <= 0:
            return []
        cutoff = datetime.now(UTC) - timedelta(seconds=float(older_than_seconds))
        sql = """
            SELECT inbox_id, project_id, agent_id, message_type, payload,
                   recursion_depth, traceparent, tracestate, trace_id, created_at
            FROM state.agent_inbox
            WHERE status='pending'
              AND created_at < %s
            ORDER BY created_at ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (cutoff, int(limit)))
        return [dict(row) for row in rows]

    async def patch_inbox_payload(
        self,
        *,
        inbox_id: str,
        project_id: str,
        patch: Dict[str, Any],
    ) -> bool:
        sql = """
            UPDATE state.agent_inbox
            SET payload = payload || %s::jsonb
            WHERE project_id=%s AND inbox_id=%s
        """
        async with self.pool.connection() as conn:
            res = await conn.execute(sql, (json.dumps(patch or {}), project_id, inbox_id))
            return res.rowcount == 1

    async def list_execution_edges(
        self,
        *,
        project_id: str,
        channel_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        primitive: Optional[str] = None,
        edge_phase: Optional[str] = None,
        trace_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT edge_id,
                   project_id,
                   channel_id,
                   primitive,
                   edge_phase,
                   source_agent_id,
                   source_agent_turn_id,
                   source_step_id,
                   target_agent_id,
                   target_agent_turn_id,
                   correlation_id,
                   enqueue_mode,
                   recursion_depth,
                   trace_id,
                   parent_step_id,
                   metadata,
                   created_at
            FROM state.execution_edges
            WHERE project_id=%s
        """
        params: List[Any] = [project_id]

        ch = (channel_id or "").strip()
        if ch:
            sql += " AND channel_id=%s"
            params.append(ch)
        primitive_value = (primitive or "").strip()
        if primitive_value:
            sql += " AND primitive=%s"
            params.append(primitive_value)
        edge_phase_value = (edge_phase or "").strip()
        if edge_phase_value:
            sql += " AND edge_phase=%s"
            params.append(edge_phase_value)
        trace_value = (trace_id or "").strip()
        if trace_value:
            sql += " AND trace_id=%s"
            params.append(trace_value)
        parent_value = (parent_step_id or "").strip()
        if parent_value:
            sql += " AND parent_step_id=%s"
            params.append(parent_value)
        correlation_value = (correlation_id or "").strip()
        if correlation_value:
            sql += " AND correlation_id=%s"
            params.append(correlation_value)
        agent = (agent_id or "").strip()
        if agent:
            sql += " AND (source_agent_id=%s OR target_agent_id=%s)"
            params.extend([agent, agent])

        sql += " ORDER BY created_at DESC LIMIT %s"
        params.append(max(1, int(limit)))

        rows = await self.fetch_all(sql, tuple(params))
        return [dict(row) for row in rows]
