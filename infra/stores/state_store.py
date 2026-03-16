import inspect
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Iterable, Awaitable, Callable

from psycopg_pool import AsyncConnectionPool

from core.cg_context import CGContext
from core.state import AgentStateHead, AgentStatePointers
from .base import BaseStore
from .turn_lease import (
    ensure_agent_state_row_with_conn as _ensure_agent_state_row_with_conn,
    lease_agent_turn_with_conn as _lease_agent_turn_with_conn,
)

_UNSET = object()


@dataclass(frozen=True, slots=True)
class TurnIdleTransition:
    committed_ctx: CGContext
    last_output_box_id: Optional[str]


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
        *,
        ctx: CGContext,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
        conn: Any = None,
    ):
        """Insert a state row if missing (used by PMO when spawning agents)."""
        if conn is not None:
            await _ensure_agent_state_row_with_conn(
                conn,
                ctx=ctx,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
            )
            return
        async with self.pool.connection() as own_conn:
            await _ensure_agent_state_row_with_conn(
                own_conn,
                ctx=ctx,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
            )

    async def fetch(
        self,
        ctx: CGContext,
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
            (ctx.project_id, ctx.agent_id),
            conn=conn,
        )
        if not data:
            return None
        return AgentStateHead.from_row(data)

    async def fetch_pointers(
        self,
        ctx: CGContext,
        *,
        conn: Any = None,
    ) -> Optional[AgentStatePointers]:
        data = await self.fetch_one(
            """
            SELECT project_id, agent_id, memory_context_box_id, last_output_box_id, updated_at
            FROM state.agent_state_pointers
            WHERE project_id=%s AND agent_id=%s
            """,
            (ctx.project_id, ctx.agent_id),
            conn=conn,
        )
        if not data:
            return None
        return AgentStatePointers.from_row(data)

    async def delete_agent_state(self, *, ctx: CGContext) -> None:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DELETE FROM state.agent_state_head
                    WHERE project_id=%s AND agent_id=%s
                    """,
                    (ctx.project_id, ctx.agent_id),
                )
                await conn.execute(
                    """
                    DELETE FROM state.agent_state_pointers
                    WHERE project_id=%s AND agent_id=%s
                    """,
                    (ctx.project_id, ctx.agent_id),
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
        ctx: CGContext,
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

        lock_key = f"memory_context_box:{ctx.agent_id}"
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(lock_sql, (ctx.project_id, lock_key))
                if ensure:
                    await conn.execute(ensure_row_sql, (ctx.project_id, ctx.agent_id))
                row = await self.fetch_one(select_sql, (ctx.project_id, ctx.agent_id), conn=conn)
                current = row.get("memory_context_box_id") if row else None
                if current:
                    box_id = str(current)
                    if normalize_box_id:
                        normalized = await _call_with_optional_conn(normalize_box_id, box_id, conn=conn)
                        if str(normalized) != box_id:
                            await conn.execute(update_sql, (str(normalized), ctx.project_id, ctx.agent_id))
                            box_id = str(normalized)
                    return box_id

                if not ensure:
                    return None

                new_box_id = await _call_with_optional_conn(create_box, conn=conn) if create_box else None
                if not new_box_id:
                    return None
                await conn.execute(update_sql, (str(new_box_id), ctx.project_id, ctx.agent_id))
                return str(new_box_id)

    async def finish_turn_idle(
        self,
        *,
        ctx: CGContext,
        expect_status: Optional[str] = None,
        last_output_box_id: Optional[str] = None,
        bump_epoch: bool = False,
        conn: Any = None,
    ) -> bool:
        """Atomically transition an agent turn to idle and persist last_output_box_id pointer.

        This clears active-turn fields in state.agent_state_head and updates state.agent_state_pointers
        in the same transaction.
        """
        transition = await self.finish_turn_idle_transition(
            ctx=ctx,
            expect_status=expect_status,
            last_output_box_id=last_output_box_id,
            bump_epoch=bump_epoch,
            conn=conn,
        )
        return transition is not None

    async def finish_turn_idle_transition(
        self,
        *,
        ctx: CGContext,
        expect_status: Optional[str] = None,
        last_output_box_id: Optional[str] = None,
        bump_epoch: bool = False,
        conn: Any = None,
    ) -> Optional[TurnIdleTransition]:
        if conn is not None:
            return await self.finish_turn_idle_transition_with_conn(
                conn,
                ctx=ctx,
                expect_status=expect_status,
                last_output_box_id=last_output_box_id,
                bump_epoch=bump_epoch,
            )
        async with self.pool.connection() as own_conn:
            async with own_conn.transaction():
                return await self.finish_turn_idle_transition_with_conn(
                    own_conn,
                    ctx=ctx,
                    expect_status=expect_status,
                    last_output_box_id=last_output_box_id,
                    bump_epoch=bump_epoch,
                )

    async def finish_turn_idle_with_conn(
        self,
        conn: Any,
        *,
        ctx: CGContext,
        expect_status: Optional[str] = None,
        last_output_box_id: Optional[str] = None,
        bump_epoch: bool = False,
    ) -> bool:
        transition = await self.finish_turn_idle_transition_with_conn(
            conn,
            ctx=ctx,
            expect_status=expect_status,
            last_output_box_id=last_output_box_id,
            bump_epoch=bump_epoch,
        )
        return transition is not None

    async def finish_turn_idle_transition_with_conn(
        self,
        conn: Any,
        *,
        ctx: CGContext,
        expect_status: Optional[str] = None,
        last_output_box_id: Optional[str] = None,
        bump_epoch: bool = False,
    ) -> Optional[TurnIdleTransition]:
        project_id, agent_id, expect_agent_turn_id, expect_turn_epoch = self._required_turn_ctx(
            ctx, "finish_turn_idle"
        )
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
        params: List[Any] = [project_id, agent_id, expect_turn_epoch, expect_agent_turn_id]
        if expect_status is not None:
            update_head_sql += " AND status=%s"
            params.append(expect_status)

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

        res = await conn.execute(update_head_sql, tuple(params))
        if res.rowcount != 1:
            return None
        await conn.execute(
            cleanup_waiting_sql,
            (project_id, agent_id, expect_agent_turn_id, expect_turn_epoch),
        )
        if last_output_box_id:
            await conn.execute(
                upsert_pointer_sql,
                (project_id, agent_id, str(last_output_box_id)),
            )
        committed_epoch = int(expect_turn_epoch) + (1 if bump_epoch else 0)
        return TurnIdleTransition(
            committed_ctx=ctx.with_bumped_epoch(committed_epoch),
            last_output_box_id=str(last_output_box_id) if last_output_box_id is not None else None,
        )

    async def lease_agent_turn(
        self,
        *,
        ctx: CGContext,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
    ) -> Optional[int]:
        """Atomically lease an idle agent for a new turn.

        Returns new turn_epoch if lease succeeded, else None (agent busy).
        """
        async with self.pool.connection() as conn:
            return await self.lease_agent_turn_with_conn(
                conn,
                ctx=ctx,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
            )

    async def lease_agent_turn_with_conn(
        self,
        conn: Any,
        *,
        ctx: CGContext,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
    ) -> Optional[int]:
        _ = ctx.require_agent_turn_id
        return await _lease_agent_turn_with_conn(
            conn,
            ctx=ctx,
            profile_box_id=profile_box_id,
            context_box_id=context_box_id,
            output_box_id=output_box_id,
        )

    async def update(
        self,
        *,
        ctx: CGContext,
        expect_status: Optional[str] = None,
        new_status: Optional[str] = None,
        active_agent_turn_id: Any = _UNSET,
        active_channel_id: Any = _UNSET,
        active_recursion_depth: Any = _UNSET,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
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
                ctx=ctx,
                expect_status=expect_status,
                new_status=new_status,
                active_agent_turn_id=active_agent_turn_id,
                active_channel_id=active_channel_id,
                active_recursion_depth=active_recursion_depth,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
                expecting_correlation_id=expecting_correlation_id,
                waiting_tool_count=waiting_tool_count,
                resume_deadline=resume_deadline,
                bump_epoch=bump_epoch,
            )
        async with self.pool.connection() as own_conn:
            return await self.update_with_conn(
                own_conn,
                ctx=ctx,
                expect_status=expect_status,
                new_status=new_status,
                active_agent_turn_id=active_agent_turn_id,
                active_channel_id=active_channel_id,
                active_recursion_depth=active_recursion_depth,
                profile_box_id=profile_box_id,
                context_box_id=context_box_id,
                output_box_id=output_box_id,
                expecting_correlation_id=expecting_correlation_id,
                waiting_tool_count=waiting_tool_count,
                resume_deadline=resume_deadline,
                bump_epoch=bump_epoch,
            )

    async def update_with_conn(
        self,
        conn: Any,
        *,
        ctx: CGContext,
        expect_status: Optional[str] = None,
        new_status: Optional[str] = None,
        active_agent_turn_id: Any = _UNSET,
        active_channel_id: Any = _UNSET,
        active_recursion_depth: Any = _UNSET,
        profile_box_id: Optional[str] = None,
        context_box_id: Optional[str] = None,
        output_box_id: Optional[str] = None,
        expecting_correlation_id: Any = _UNSET,
        waiting_tool_count: Any = _UNSET,
        resume_deadline: Any = _UNSET,
        bump_epoch: bool = False,
    ) -> bool:
        project_id, agent_id, expect_agent_turn_id, expect_turn_epoch = self._required_turn_ctx(ctx, "update")
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
        sql += " AND turn_epoch=%s"
        params.append(expect_turn_epoch)
        sql += " AND active_agent_turn_id=%s"
        params.append(expect_agent_turn_id)
        if expect_status is not None:
            sql += " AND status=%s"
            params.append(expect_status)

        res = await conn.execute(sql, tuple(params))
        return res.rowcount == 1

    @staticmethod
    def _required_turn_ctx(ctx: CGContext, _method_name: str) -> tuple[str, str, str, int]:
        project_id = ctx.project_id
        agent_id = ctx.agent_id
        agent_turn_id = ctx.require_agent_turn_id
        turn_epoch = ctx.turn_epoch
        return project_id, agent_id, agent_turn_id, turn_epoch

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
        ctx: CGContext,
        wait_items: List[Dict[str, Any]],
        conn: Any = None,
    ) -> None:
        project_id, agent_id, agent_turn_id, turn_epoch = self._required_turn_ctx(ctx, "replace_turn_waiting_tools")
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
                    ctx=ctx,
                    wait_items=wait_items,
                    conn=own_conn,
                )

    async def list_turn_waiting_tools(
        self,
        *,
        ctx: CGContext,
        statuses: Optional[Iterable[str]] = None,
    ) -> List[Dict[str, Any]]:
        project_id, agent_id, agent_turn_id, turn_epoch = self._required_turn_ctx(ctx, "list_turn_waiting_tools")
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
        ctx: CGContext,
        tool_call_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        project_id, agent_id, agent_turn_id, turn_epoch = self._required_turn_ctx(ctx, "get_turn_waiting_tool")
        resolved_tool_call_id = str(tool_call_id).strip() if tool_call_id is not None else ctx.require_tool_call_id
        if not resolved_tool_call_id:
            raise ValueError("get_turn_waiting_tool requires tool_call_id (parameter or ctx.tool_call_id)")
        row = await self.fetch_one(
            """
            SELECT project_id, agent_id, agent_turn_id, turn_epoch, step_id, tool_call_id, tool_name,
                   wait_status, tool_result_card_id, received_at, applied_at, created_at, updated_at
            FROM state.turn_waiting_tools
            WHERE project_id=%s AND agent_id=%s AND agent_turn_id=%s AND turn_epoch=%s AND tool_call_id=%s
            """,
            (project_id, agent_id, agent_turn_id, int(turn_epoch), resolved_tool_call_id),
        )
        return dict(row) if row else None

    async def mark_turn_waiting_tool_received(
        self,
        *,
        ctx: CGContext,
        tool_result_card_id: str,
        tool_name: Optional[str] = None,
        conn: Any = None,
    ) -> bool:
        project_id, agent_id, agent_turn_id, turn_epoch = self._required_turn_ctx(
            ctx, "mark_turn_waiting_tool_received"
        )
        resolved_tool_call_id = ctx.require_tool_call_id
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
            ctx.step_id,
            tool_name,
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            resolved_tool_call_id,
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
        ctx: CGContext,
        tool_result_card_id: Optional[str] = None,
        conn: Any = None,
    ) -> bool:
        project_id, agent_id, agent_turn_id, turn_epoch = self._required_turn_ctx(
            ctx, "mark_turn_waiting_tool_applied"
        )
        resolved_tool_call_id = ctx.require_tool_call_id
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
              AND wait_status IN ('waiting', 'received')
        """
        args = (
            tool_result_card_id,
            project_id,
            agent_id,
            agent_turn_id,
            int(turn_epoch),
            resolved_tool_call_id,
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

    async def record_force_termination(
        self,
        ctx: CGContext,
        reason: str,
        output_box_id: Optional[str] = None,
    ) -> None:
        """Insert a failed step record for a watchdog-reaped turn."""
        agent_turn_id = ctx.require_agent_turn_id
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
                (ctx.project_id, ctx.agent_id, fake_step_id, agent_turn_id, output_box_id, reason)
            )
