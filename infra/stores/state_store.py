import json
import inspect
from datetime import UTC, datetime, timedelta
from typing import Optional, List, Dict, Any, Iterable, Awaitable, Callable

from psycopg_pool import AsyncConnectionPool

from core.state import AgentStateHead, AgentStatePointers
from .base import BaseStore
from .turn_lease import (
    ensure_agent_state_row_with_conn as _ensure_agent_state_row_with_conn,
    lease_agent_turn_with_conn as _lease_agent_turn_with_conn,
)

_UNSET = object()


class StateStore(BaseStore):
    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: AsyncConnectionPool | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
    ):
        super().__init__(dsn, pool=pool, min_size=min_size, max_size=max_size)

    async def init_if_absent(
        self,
        project_id: str,
        agent_id: str,
        active_channel_id: Optional[str] = None,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        conn: Any = None,
    ):
        """Insert a state row if missing (used by PMO when spawning agents)."""
        if conn is not None:
            await _ensure_agent_state_row_with_conn(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                active_channel_id=active_channel_id,
                parent_step_id=parent_step_id,
                trace_id=trace_id,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
            )
            return
        async with self.pool.connection() as own_conn:
            await _ensure_agent_state_row_with_conn(
                own_conn,
                project_id=project_id,
                agent_id=agent_id,
                active_channel_id=active_channel_id,
                parent_step_id=parent_step_id,
                trace_id=trace_id,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
            )

    async def fetch(
        self,
        project_id: str,
        agent_id: str,
        *,
        conn: Any = None,
    ) -> Optional[AgentStateHead]:
        data = await self.fetch_one(
            """
            SELECT project_id, agent_id, status, active_agent_turn_id,
                   turn_epoch, active_recursion_depth, updated_at, active_channel_id, parent_step_id, trace_id,
                   profile_box_id, context_box_id, output_box_id,
                   expecting_correlation_id, waiting_tool_count, resume_deadline
            FROM state.agent_state_head
            WHERE project_id=%s AND agent_id=%s
            """,
            (project_id, agent_id),
            conn=conn,
        )
        if not data:
            return None
        return AgentStateHead.from_row(data)

    async def fetch_pointers(
        self,
        project_id: str,
        agent_id: str,
        *,
        conn: Any = None,
    ) -> Optional[AgentStatePointers]:
        data = await self.fetch_one(
            """
            SELECT project_id, agent_id, memory_context_box_id, last_output_box_id, updated_at
            FROM state.agent_state_pointers
            WHERE project_id=%s AND agent_id=%s
            """,
            (project_id, agent_id),
            conn=conn,
        )
        if not data:
            return None
        return AgentStatePointers.from_row(data)

    async def delete_agent_state(self, *, project_id: str, agent_id: str) -> None:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DELETE FROM state.agent_state_head
                    WHERE project_id=%s AND agent_id=%s
                    """,
                    (project_id, agent_id),
                )
                await conn.execute(
                    """
                    DELETE FROM state.agent_state_pointers
                    WHERE project_id=%s AND agent_id=%s
                    """,
                    (project_id, agent_id),
                )

    async def list_agent_state_heads(
        self,
        *,
        project_id: str,
        channel_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT h.project_id,
                   h.agent_id,
                   h.status,
                   h.active_agent_turn_id::text AS active_agent_turn_id,
                   h.active_channel_id,
                   h.turn_epoch,
                   h.active_recursion_depth,
                   h.updated_at,
                   h.parent_step_id,
                   h.trace_id,
                   h.profile_box_id::text AS profile_box_id,
                   COALESCE(h.context_box_id, p.memory_context_box_id)::text AS context_box_id,
                   COALESCE(h.output_box_id, p.last_output_box_id)::text AS output_box_id,
                   h.expecting_correlation_id,
                   h.waiting_tool_count,
                   h.resume_deadline,
                   pa.display_name
            FROM state.agent_state_head h
            LEFT JOIN state.agent_state_pointers p
              ON h.project_id = p.project_id
             AND h.agent_id = p.agent_id
            LEFT JOIN resource.project_agents pa
              ON h.project_id = pa.project_id
             AND h.agent_id = pa.agent_id
            WHERE h.project_id=%s
        """
        params: List[Any] = [project_id]
        ch = (channel_id or "").strip()
        if ch:
            sql += " AND h.active_channel_id=%s"
            params.append(ch)
        sql += " ORDER BY h.updated_at DESC LIMIT %s"
        params.append(max(1, int(limit)))
        rows = await self.fetch_all(sql, tuple(params))
        return [dict(row) for row in rows]

    async def ensure_memory_context_box_id(
        self,
        *,
        project_id: str,
        agent_id: str,
        ensure: bool = True,
        normalize_box_id: Optional[Callable[..., Awaitable[str]]] = None,
        create_box: Optional[Callable[..., Awaitable[str]]] = None,
    ) -> Optional[str]:
        """Atomically ensure a memory_context_box_id for an agent using an advisory lock."""
        if ensure and not create_box:
            raise ValueError("create_box is required when ensure=True")

        async def _call_with_optional_conn(
            fn: Callable[..., Awaitable[str]],
            *args: Any,
            conn: Any,
        ) -> str:
            sig = inspect.signature(fn)
            params = sig.parameters
            if "conn" in params:
                return await fn(*args, conn=conn)
            if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
                return await fn(*args, conn=conn)
            positional_params = [
                p for p in params.values()
                if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            ]
            if len(positional_params) > len(args):
                return await fn(*args, conn)
            return await fn(*args)

        lock_sql = "SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))"
        ensure_row_sql = """
            INSERT INTO state.agent_state_pointers (project_id, agent_id, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (project_id, agent_id) DO NOTHING
        """
        select_sql = """
            SELECT memory_context_box_id
            FROM state.agent_state_pointers
            WHERE project_id=%s AND agent_id=%s
        """
        update_sql = """
            UPDATE state.agent_state_pointers
            SET memory_context_box_id=%s, updated_at=NOW()
            WHERE project_id=%s AND agent_id=%s
        """

        lock_key = f"memory_context_box:{agent_id}"
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(lock_sql, (project_id, lock_key))
                if ensure:
                    await conn.execute(ensure_row_sql, (project_id, agent_id))
                row = await self.fetch_one(select_sql, (project_id, agent_id), conn=conn)
                current = row.get("memory_context_box_id") if row else None
                if current:
                    box_id = str(current)
                    if normalize_box_id:
                        normalized = await _call_with_optional_conn(normalize_box_id, box_id, conn=conn)
                        if str(normalized) != box_id:
                            await conn.execute(update_sql, (str(normalized), project_id, agent_id))
                            box_id = str(normalized)
                    return box_id

                if not ensure:
                    return None

                new_box_id = await _call_with_optional_conn(create_box, conn=conn) if create_box else None
                if not new_box_id:
                    return None
                await conn.execute(update_sql, (str(new_box_id), project_id, agent_id))
                return str(new_box_id)

    async def finish_turn_idle(
        self,
        *,
        project_id: str,
        agent_id: str,
        expect_turn_epoch: int,
        expect_agent_turn_id: str,
        expect_status: Optional[str] = None,
        last_output_box_id: Optional[str] = None,
        bump_epoch: bool = False,
        conn: Any = None,
    ) -> bool:
        """Atomically transition an agent turn to idle and persist last_output_box_id pointer.

        This clears active-turn fields in state.agent_state_head and updates state.agent_state_pointers
        in the same transaction.
        """
        if conn is not None:
            return await self.finish_turn_idle_with_conn(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                expect_turn_epoch=expect_turn_epoch,
                expect_agent_turn_id=expect_agent_turn_id,
                expect_status=expect_status,
                last_output_box_id=last_output_box_id,
                bump_epoch=bump_epoch,
            )
        async with self.pool.connection() as own_conn:
            async with own_conn.transaction():
                return await self.finish_turn_idle_with_conn(
                    own_conn,
                    project_id=project_id,
                    agent_id=agent_id,
                    expect_turn_epoch=expect_turn_epoch,
                    expect_agent_turn_id=expect_agent_turn_id,
                    expect_status=expect_status,
                    last_output_box_id=last_output_box_id,
                    bump_epoch=bump_epoch,
                )

    async def finish_turn_idle_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        agent_id: str,
        expect_turn_epoch: int,
        expect_agent_turn_id: str,
        expect_status: Optional[str] = None,
        last_output_box_id: Optional[str] = None,
        bump_epoch: bool = False,
    ) -> bool:
        clauses = [
            "status='idle'",
            "active_agent_turn_id=NULL",
            "active_channel_id=NULL",
            "active_recursion_depth=0",
            "parent_step_id=NULL",
            "trace_id=NULL",
            "context_box_id=NULL",
            "output_box_id=NULL",
            "expecting_correlation_id=NULL",
            "waiting_tool_count=0",
            "resume_deadline=NULL",
            "updated_at=NOW()",
        ]
        if bump_epoch:
            clauses.append("turn_epoch = turn_epoch + 1")

        update_head_sql = f"""
            UPDATE state.agent_state_head
            SET {', '.join(clauses)}
            WHERE project_id=%s AND agent_id=%s
              AND turn_epoch=%s
              AND active_agent_turn_id=%s
        """
        params: List[Any] = [project_id, agent_id, int(expect_turn_epoch), str(expect_agent_turn_id)]
        if expect_status is not None:
            update_head_sql += " AND status=%s"
            params.append(str(expect_status))

        upsert_pointer_sql = """
            INSERT INTO state.agent_state_pointers (project_id, agent_id, last_output_box_id, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (project_id, agent_id) DO UPDATE
            SET last_output_box_id = EXCLUDED.last_output_box_id,
                updated_at = NOW()
        """
        cleanup_waiting_sql = """
            DELETE FROM state.turn_waiting_tools
            WHERE project_id=%s AND agent_id=%s AND agent_turn_id=%s AND turn_epoch=%s
        """
        cleanup_resume_ledger_sql = """
            DELETE FROM state.turn_resume_ledger
            WHERE project_id=%s AND agent_id=%s AND agent_turn_id=%s AND turn_epoch=%s
        """

        res = await conn.execute(update_head_sql, tuple(params))
        if res.rowcount != 1:
            return False
        await conn.execute(
            cleanup_waiting_sql,
            (project_id, agent_id, str(expect_agent_turn_id), int(expect_turn_epoch)),
        )
        await conn.execute(
            cleanup_resume_ledger_sql,
            (project_id, agent_id, str(expect_agent_turn_id), int(expect_turn_epoch)),
        )
        if last_output_box_id:
            await conn.execute(
                upsert_pointer_sql,
                (project_id, agent_id, str(last_output_box_id)),
            )
        return True

    async def lease_agent_turn(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        active_channel_id: Optional[str] = None,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        active_recursion_depth: Optional[int] = None,
    ) -> Optional[int]:
        """Atomically lease an idle agent for a new turn.

        Returns new turn_epoch if lease succeeded, else None (agent busy).
        """
        async with self.pool.connection() as conn:
            return await self.lease_agent_turn_with_conn(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                agent_turn_id=agent_turn_id,
                active_channel_id=active_channel_id,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
                parent_step_id=parent_step_id,
                trace_id=trace_id,
                active_recursion_depth=active_recursion_depth,
            )

    async def lease_agent_turn_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        active_channel_id: Optional[str] = None,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
        parent_step_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        active_recursion_depth: Optional[int] = None,
    ) -> Optional[int]:
        return await _lease_agent_turn_with_conn(
            conn,
            project_id=project_id,
            agent_id=agent_id,
            agent_turn_id=agent_turn_id,
            active_channel_id=active_channel_id,
            profile_box_id=profile_box_id,
            context_box_id=context_box_id,
            output_box_id=output_box_id,
            parent_step_id=parent_step_id,
            trace_id=trace_id,
            active_recursion_depth=active_recursion_depth,
        )

    async def update(
        self,
        *,
        project_id: str,
        agent_id: str,
        expect_turn_epoch: Optional[int],
        expect_agent_turn_id: Optional[str],
        expect_status: Optional[str] = None,
        new_status: Optional[str] = None,
        active_agent_turn_id: Any = _UNSET,
        active_channel_id: Any = _UNSET,
        active_recursion_depth: Any = _UNSET,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
        parent_step_id: Any = _UNSET,
        trace_id: Any = _UNSET,
        expecting_correlation_id: Any = _UNSET,
        waiting_tool_count: Any = _UNSET,
        resume_deadline: Any = _UNSET,
        bump_epoch: bool = False,
        conn: Any = None,
    ) -> bool:
        """CAS update with strong L0 guard."""
        if conn is not None:
            return await self.update_with_conn(
                conn,
                project_id=project_id,
                agent_id=agent_id,
                expect_turn_epoch=expect_turn_epoch,
                expect_agent_turn_id=expect_agent_turn_id,
                expect_status=expect_status,
                new_status=new_status,
                active_agent_turn_id=active_agent_turn_id,
                active_channel_id=active_channel_id,
                active_recursion_depth=active_recursion_depth,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
                parent_step_id=parent_step_id,
                trace_id=trace_id,
                expecting_correlation_id=expecting_correlation_id,
                waiting_tool_count=waiting_tool_count,
                resume_deadline=resume_deadline,
                bump_epoch=bump_epoch,
            )
        async with self.pool.connection() as own_conn:
            return await self.update_with_conn(
                own_conn,
                project_id=project_id,
                agent_id=agent_id,
                expect_turn_epoch=expect_turn_epoch,
                expect_agent_turn_id=expect_agent_turn_id,
                expect_status=expect_status,
                new_status=new_status,
                active_agent_turn_id=active_agent_turn_id,
                active_channel_id=active_channel_id,
                active_recursion_depth=active_recursion_depth,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
                parent_step_id=parent_step_id,
                trace_id=trace_id,
                expecting_correlation_id=expecting_correlation_id,
                waiting_tool_count=waiting_tool_count,
                resume_deadline=resume_deadline,
                bump_epoch=bump_epoch,
            )

    async def update_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        agent_id: str,
        expect_turn_epoch: Optional[int],
        expect_agent_turn_id: Optional[str],
        expect_status: Optional[str] = None,
        new_status: Optional[str] = None,
        active_agent_turn_id: Any = _UNSET,
        active_channel_id: Any = _UNSET,
        active_recursion_depth: Any = _UNSET,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
        parent_step_id: Any = _UNSET,
        trace_id: Any = _UNSET,
        expecting_correlation_id: Any = _UNSET,
        waiting_tool_count: Any = _UNSET,
        resume_deadline: Any = _UNSET,
        bump_epoch: bool = False,
    ) -> bool:
        clauses = ["updated_at = NOW()"]
        params: List[Any] = []

        if new_status:
            clauses.append("status=%s")
            params.append(new_status)
        if active_agent_turn_id is not _UNSET:
            clauses.append("active_agent_turn_id=%s")
            params.append(active_agent_turn_id)
        if active_channel_id is not _UNSET:
            clauses.append("active_channel_id=%s")
            params.append(active_channel_id)
        if active_recursion_depth is not _UNSET:
            clauses.append("active_recursion_depth=%s")
            params.append(active_recursion_depth)
        if parent_step_id is not _UNSET:
            clauses.append("parent_step_id=%s")
            params.append(parent_step_id)
        if trace_id is not _UNSET:
            clauses.append("trace_id=%s")
            params.append(trace_id)
        if expecting_correlation_id is not _UNSET:
            clauses.append("expecting_correlation_id=%s")
            params.append(expecting_correlation_id)
        if waiting_tool_count is not _UNSET:
            clauses.append("waiting_tool_count=%s")
            params.append(waiting_tool_count)
        if resume_deadline is not _UNSET:
            clauses.append("resume_deadline=%s")
            params.append(resume_deadline)
        if profile_box_id is not None:
            clauses.append("profile_box_id=%s")
            params.append(profile_box_id)
        if context_box_id is not None:
            clauses.append("context_box_id=%s")
            params.append(context_box_id)
        if output_box_id is not None:
            clauses.append("output_box_id=%s")
            params.append(output_box_id)
        if bump_epoch:
            clauses.append("turn_epoch = turn_epoch + 1")

        sql = f"""
            UPDATE state.agent_state_head
            SET {', '.join(clauses)}
            WHERE project_id=%s AND agent_id=%s
        """
        params.extend([project_id, agent_id])
        if expect_turn_epoch is not None:
            sql += " AND turn_epoch=%s"
            params.append(expect_turn_epoch)
        if expect_agent_turn_id is not None:
            sql += " AND active_agent_turn_id=%s"
            params.append(expect_agent_turn_id)
        if expect_status is not None:
            sql += " AND status=%s"
            params.append(expect_status)

        res = await conn.execute(sql, tuple(params))
        return res.rowcount == 1

    async def list_stuck_dispatched(
        self, *, older_than_seconds: float, limit: int = 100
    ) -> List[AgentStateHead]:
        """List dispatched heads that have not been picked up in time."""
        sql = """
            SELECT project_id, agent_id, status, active_agent_turn_id,
                   turn_epoch, active_recursion_depth, updated_at, active_channel_id, parent_step_id, trace_id,
                   profile_box_id, context_box_id, output_box_id,
                   expecting_correlation_id, waiting_tool_count, resume_deadline
            FROM state.agent_state_head
            WHERE status='dispatched'
              AND updated_at < NOW() - (%s * INTERVAL '1 second')
            ORDER BY updated_at ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (older_than_seconds, limit))
        return [AgentStateHead.from_row(r) for r in rows]

    async def list_stuck_active(
        self,
        *,
        statuses: Iterable[str],
        older_than_seconds: float,
        limit: int = 100,
    ) -> List[AgentStateHead]:
        """List running heads that exceeded active timeout."""
        status_list = list(statuses)
        sql = """
            SELECT project_id, agent_id, status, active_agent_turn_id,
                   turn_epoch, active_recursion_depth, updated_at, active_channel_id, parent_step_id, trace_id,
                   profile_box_id, context_box_id, output_box_id,
                   expecting_correlation_id, waiting_tool_count, resume_deadline
            FROM state.agent_state_head
            WHERE status = ANY(%s)
              AND updated_at < NOW() - (%s * INTERVAL '1 second')
            ORDER BY updated_at ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (status_list, older_than_seconds, limit))
        return [AgentStateHead.from_row(r) for r in rows]

    async def list_suspended_timeouts(self, *, limit: int = 100) -> List[AgentStateHead]:
        """List suspended heads whose resume_deadline has passed."""
        sql = """
            SELECT project_id, agent_id, status, active_agent_turn_id,
                   turn_epoch, active_recursion_depth, updated_at, active_channel_id, parent_step_id, trace_id,
                   profile_box_id, context_box_id, output_box_id,
                   expecting_correlation_id, waiting_tool_count, resume_deadline
            FROM state.agent_state_head
            WHERE status='suspended'
              AND resume_deadline IS NOT NULL
              AND resume_deadline < NOW()
            ORDER BY resume_deadline ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (limit,))
        return [AgentStateHead.from_row(r) for r in rows]

    async def replace_turn_waiting_tools(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        wait_items: List[Dict[str, Any]],
        conn: Any = None,
    ) -> None:
        delete_sql = """
            DELETE FROM state.turn_waiting_tools
            WHERE project_id=%s AND agent_id=%s AND agent_turn_id=%s AND turn_epoch=%s
        """
        insert_sql = """
            INSERT INTO state.turn_waiting_tools (
                project_id, agent_id, agent_turn_id, turn_epoch, step_id, tool_call_id, tool_name,
                wait_status, tool_result_card_id, received_at, applied_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                'waiting', NULL, NULL, NULL, NOW(), NOW()
            )
            ON CONFLICT (project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id)
            DO UPDATE SET
                step_id = EXCLUDED.step_id,
                tool_name = EXCLUDED.tool_name,
                wait_status = 'waiting',
                tool_result_card_id = NULL,
                received_at = NULL,
                applied_at = NULL,
                updated_at = NOW()
        """
        if conn is not None:
            await conn.execute(
                delete_sql,
                (project_id, agent_id, agent_turn_id, int(turn_epoch)),
            )
            for item in wait_items or []:
                tool_call_id = str(item.get("tool_call_id") or "").strip()
                if not tool_call_id:
                    continue
                step_id = str(item.get("step_id") or "").strip()
                tool_name = str(item.get("tool_name") or "unknown").strip() or "unknown"
                await conn.execute(
                    insert_sql,
                    (
                        project_id,
                        agent_id,
                        agent_turn_id,
                        int(turn_epoch),
                        step_id,
                        tool_call_id,
                        tool_name,
                    ),
                )
            return

        async with self.pool.connection() as own_conn:
            async with own_conn.transaction():
                await self.replace_turn_waiting_tools(
                    project_id=project_id,
                    agent_id=agent_id,
                    agent_turn_id=agent_turn_id,
                    turn_epoch=turn_epoch,
                    wait_items=wait_items,
                    conn=own_conn,
                )

    async def list_turn_waiting_tools(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        statuses: Optional[Iterable[str]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT project_id, agent_id, agent_turn_id, turn_epoch, step_id, tool_call_id, tool_name,
                   wait_status, tool_result_card_id, received_at, applied_at, created_at, updated_at
            FROM state.turn_waiting_tools
            WHERE project_id=%s AND agent_id=%s AND agent_turn_id=%s AND turn_epoch=%s
        """
        params: List[Any] = [project_id, agent_id, agent_turn_id, int(turn_epoch)]
        status_list = [str(s) for s in (statuses or []) if str(s).strip()]
        if status_list:
            sql += " AND wait_status = ANY(%s)"
            params.append(status_list)
        sql += " ORDER BY created_at ASC"
        rows = await self.fetch_all(sql, tuple(params))
        return [dict(row) for row in rows]

    async def get_turn_waiting_tool(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        tool_call_id: str,
    ) -> Optional[Dict[str, Any]]:
        row = await self.fetch_one(
            """
            SELECT project_id, agent_id, agent_turn_id, turn_epoch, step_id, tool_call_id, tool_name,
                   wait_status, tool_result_card_id, received_at, applied_at, created_at, updated_at
            FROM state.turn_waiting_tools
            WHERE project_id=%s AND agent_id=%s AND agent_turn_id=%s AND turn_epoch=%s AND tool_call_id=%s
            """,
            (project_id, agent_id, agent_turn_id, int(turn_epoch), tool_call_id),
        )
        return dict(row) if row else None

    async def mark_turn_waiting_tool_received(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        tool_call_id: str,
        tool_result_card_id: str,
        step_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        conn: Any = None,
    ) -> bool:
        sql = """
            UPDATE state.turn_waiting_tools
            SET wait_status='received',
                tool_result_card_id=%s,
                received_at=COALESCE(received_at, NOW()),
                step_id=COALESCE(%s, step_id),
                tool_name=COALESCE(%s, tool_name),
                updated_at=NOW()
            WHERE project_id=%s
              AND agent_id=%s
              AND agent_turn_id=%s
              AND turn_epoch=%s
              AND tool_call_id=%s
              AND wait_status IN ('waiting', 'received')
        """
        args = (
            tool_result_card_id,
            step_id,
            tool_name,
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            tool_call_id,
        )
        if conn is not None:
            res = await conn.execute(sql, args)
            return res.rowcount == 1
        async with self.pool.connection() as own_conn:
            res = await own_conn.execute(sql, args)
            return res.rowcount == 1

    async def mark_turn_waiting_tool_applied(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        tool_call_id: str,
        tool_result_card_id: Optional[str] = None,
        conn: Any = None,
    ) -> bool:
        sql = """
            UPDATE state.turn_waiting_tools
            SET wait_status='applied',
                tool_result_card_id=COALESCE(%s, tool_result_card_id),
                applied_at=NOW(),
                updated_at=NOW()
            WHERE project_id=%s
              AND agent_id=%s
              AND agent_turn_id=%s
              AND turn_epoch=%s
              AND tool_call_id=%s
              AND wait_status IN ('waiting', 'received', 'applied')
        """
        args = (
            tool_result_card_id,
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            tool_call_id,
        )
        if conn is not None:
            res = await conn.execute(sql, args)
            return res.rowcount == 1
        async with self.pool.connection() as own_conn:
            res = await own_conn.execute(sql, args)
            return res.rowcount == 1

    async def list_suspended_heads_with_waiting(self, *, limit: int = 100) -> List[AgentStateHead]:
        sql = """
            SELECT project_id, agent_id, status, active_agent_turn_id,
                   turn_epoch, active_recursion_depth, updated_at, active_channel_id, parent_step_id, trace_id,
                   profile_box_id, context_box_id, output_box_id,
                   expecting_correlation_id, waiting_tool_count, resume_deadline
            FROM state.agent_state_head
            WHERE status='suspended'
              AND waiting_tool_count > 0
            ORDER BY updated_at ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (limit,))
        return [AgentStateHead.from_row(r) for r in rows]

    async def list_timed_out_waiting_tools(self, *, limit: int = 200) -> List[Dict[str, Any]]:
        sql = """
            SELECT h.project_id,
                   h.agent_id,
                   h.active_agent_turn_id,
                   h.active_channel_id,
                   h.turn_epoch,
                   h.active_recursion_depth,
                   h.parent_step_id,
                   h.trace_id,
                   h.resume_deadline,
                   w.step_id,
                   w.tool_call_id,
                   w.tool_name,
                   w.wait_status
            FROM state.agent_state_head h
            JOIN state.turn_waiting_tools w
              ON h.project_id = w.project_id
             AND h.agent_id = w.agent_id
             AND h.active_agent_turn_id = w.agent_turn_id
             AND h.turn_epoch = w.turn_epoch
            WHERE h.status='suspended'
              AND h.resume_deadline IS NOT NULL
              AND h.resume_deadline < NOW()
              AND w.wait_status='waiting'
            ORDER BY h.resume_deadline ASC, w.created_at ASC
            LIMIT %s
        """
        rows = await self.fetch_all(sql, (limit,))
        return [dict(row) for row in rows]

    async def record_turn_resume_ledger(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        tool_call_id: str,
        tool_result_card_id: str,
        payload: Optional[Dict[str, Any]] = None,
        conn: Any = None,
    ) -> bool:
        sql = """
            INSERT INTO state.turn_resume_ledger (
                project_id, agent_id, agent_turn_id, turn_epoch,
                tool_call_id, tool_result_card_id, status, attempt_count,
                next_retry_at, lease_owner, lease_expires_at, last_error, payload, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, 'pending', 0,
                NOW(), NULL, NULL, NULL, %s::jsonb, NOW(), NOW()
            )
            ON CONFLICT (project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id, tool_result_card_id)
            DO NOTHING
        """
        payload_json = json.dumps(payload or {}, ensure_ascii=False)
        args = (
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            tool_call_id,
            tool_result_card_id,
            payload_json,
        )
        if conn is not None:
            res = await conn.execute(sql, args)
            return res.rowcount == 1
        async with self.pool.connection() as own_conn:
            res = await own_conn.execute(sql, args)
            return res.rowcount == 1

    async def claim_turn_resume_ledgers(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        lease_owner: str,
        lease_seconds: float = 30.0,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        lease_until = datetime.now(UTC) + timedelta(seconds=max(1.0, float(lease_seconds)))
        sql = """
            WITH candidates AS (
                SELECT project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id, tool_result_card_id
                FROM state.turn_resume_ledger
                WHERE project_id=%s
                  AND agent_id=%s
                  AND agent_turn_id=%s
                  AND turn_epoch=%s
                  AND status='pending'
                  AND next_retry_at <= NOW()
                ORDER BY next_retry_at ASC, created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE state.turn_resume_ledger l
            SET status='processing',
                lease_owner=%s,
                lease_expires_at=%s,
                updated_at=NOW()
            FROM candidates c
            WHERE l.project_id=c.project_id
              AND l.agent_id=c.agent_id
              AND l.agent_turn_id=c.agent_turn_id
              AND l.turn_epoch=c.turn_epoch
              AND l.tool_call_id=c.tool_call_id
              AND l.tool_result_card_id=c.tool_result_card_id
            RETURNING l.project_id, l.agent_id, l.agent_turn_id, l.turn_epoch,
                      l.tool_call_id, l.tool_result_card_id, l.status, l.attempt_count,
                      l.next_retry_at, l.lease_owner, l.lease_expires_at, l.last_error,
                      l.payload, l.created_at, l.updated_at
        """
        rows = await self.fetch_all(
            sql,
            (
                project_id,
                agent_id,
                agent_turn_id,
                int(turn_epoch),
                int(max(1, limit)),
                lease_owner,
                lease_until,
            ),
        )
        return [dict(row) for row in rows]

    async def requeue_expired_resume_ledgers(
        self,
        *,
        lease_owner: Optional[str] = None,
        limit: int = 200,
    ) -> int:
        sql = """
            WITH candidates AS (
                SELECT project_id, agent_id, agent_turn_id, turn_epoch, tool_call_id, tool_result_card_id
                FROM state.turn_resume_ledger
                WHERE status='processing'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < NOW()
                  {owner_filter}
                ORDER BY lease_expires_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE state.turn_resume_ledger l
            SET status='pending',
                lease_owner=NULL,
                lease_expires_at=NULL,
                updated_at=NOW()
            FROM candidates c
            WHERE l.project_id=c.project_id
              AND l.agent_id=c.agent_id
              AND l.agent_turn_id=c.agent_turn_id
              AND l.turn_epoch=c.turn_epoch
              AND l.tool_call_id=c.tool_call_id
              AND l.tool_result_card_id=c.tool_result_card_id
            RETURNING l.tool_call_id
        """
        params: List[Any] = []
        owner_filter = ""
        if lease_owner:
            owner_filter = "AND lease_owner=%s"
            params.append(lease_owner)
        final_sql = sql.format(owner_filter=owner_filter)
        params.append(int(max(1, limit)))
        rows = await self.fetch_all(final_sql, tuple(params))
        return len(rows)

    async def mark_turn_resume_ledger_applied(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        tool_call_id: str,
        tool_result_card_id: str,
        conn: Any = None,
    ) -> bool:
        sql = """
            UPDATE state.turn_resume_ledger
            SET status='applied',
                lease_owner=NULL,
                lease_expires_at=NULL,
                last_error=NULL,
                updated_at=NOW()
            WHERE project_id=%s
              AND agent_id=%s
              AND agent_turn_id=%s
              AND turn_epoch=%s
              AND tool_call_id=%s
              AND tool_result_card_id=%s
              AND status IN ('pending', 'processing', 'applied')
        """
        args = (
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            tool_call_id,
            tool_result_card_id,
        )
        if conn is not None:
            res = await conn.execute(sql, args)
            return res.rowcount == 1
        async with self.pool.connection() as own_conn:
            res = await own_conn.execute(sql, args)
            return res.rowcount == 1

    async def mark_turn_resume_ledger_pending_with_backoff(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        tool_call_id: str,
        tool_result_card_id: str,
        delay_seconds: float,
        last_error: Optional[str] = None,
        conn: Any = None,
    ) -> bool:
        next_retry = datetime.now(UTC) + timedelta(seconds=max(0.0, float(delay_seconds)))
        sql = """
            UPDATE state.turn_resume_ledger
            SET status='pending',
                attempt_count=attempt_count + 1,
                next_retry_at=%s,
                lease_owner=NULL,
                lease_expires_at=NULL,
                last_error=%s,
                updated_at=NOW()
            WHERE project_id=%s
              AND agent_id=%s
              AND agent_turn_id=%s
              AND turn_epoch=%s
              AND tool_call_id=%s
              AND tool_result_card_id=%s
              AND status IN ('pending', 'processing')
        """
        args = (
            next_retry,
            last_error,
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            tool_call_id,
            tool_result_card_id,
        )
        if conn is not None:
            res = await conn.execute(sql, args)
            return res.rowcount == 1
        async with self.pool.connection() as own_conn:
            res = await own_conn.execute(sql, args)
            return res.rowcount == 1

    async def mark_turn_resume_ledger_dropped(
        self,
        *,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        turn_epoch: int,
        tool_call_id: str,
        tool_result_card_id: str,
        reason: Optional[str] = None,
        conn: Any = None,
    ) -> bool:
        sql = """
            UPDATE state.turn_resume_ledger
            SET status='dropped',
                lease_owner=NULL,
                lease_expires_at=NULL,
                last_error=%s,
                updated_at=NOW()
            WHERE project_id=%s
              AND agent_id=%s
              AND agent_turn_id=%s
              AND turn_epoch=%s
              AND tool_call_id=%s
              AND tool_result_card_id=%s
              AND status IN ('pending', 'processing', 'dropped')
        """
        args = (
            reason,
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            tool_call_id,
            tool_result_card_id,
        )
        if conn is not None:
            res = await conn.execute(sql, args)
            return res.rowcount == 1
        async with self.pool.connection() as own_conn:
            res = await own_conn.execute(sql, args)
            return res.rowcount == 1

    async def record_force_termination(
        self,
        project_id: str,
        agent_id: str,
        agent_turn_id: str,
        reason: str,
        output_box_id: Optional[str] = None,
    ) -> None:
        """Insert a failed step record for a watchdog-reaped turn."""
        sql = """
            INSERT INTO state.agent_steps (
                project_id, agent_id, step_id, agent_turn_id, status, 
                output_box_id, error, started_at, ended_at
            ) VALUES (
                %s, %s, %s, %s, 'failed', 
                %s, %s, NOW(), NOW()
            )
        """
        fake_step_id = f"step_reap_{agent_turn_id[-8:]}"
        async with self.pool.connection() as conn:
            await conn.execute(
                sql, 
                (project_id, agent_id, fake_step_id, agent_turn_id, output_box_id, reason)
            )
