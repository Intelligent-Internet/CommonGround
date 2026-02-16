from __future__ import annotations

from typing import Any, Optional


async def ensure_agent_state_row_with_conn(
    conn: Any,
    *,
    project_id: str,
    agent_id: str,
    active_channel_id: Optional[str] = None,
    parent_step_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    profile_box_id: Optional[str] = None,
    context_box_id: Optional[str] = None,
    output_box_id: Optional[str] = None,
) -> None:
    """Insert idle state row if absent."""
    insert_sql = """
        INSERT INTO state.agent_state_head (
            project_id, agent_id, status, active_agent_turn_id,
            turn_epoch, updated_at, active_channel_id, parent_step_id, trace_id,
            profile_box_id, context_box_id, output_box_id
        )
        VALUES (%s, %s, 'idle', NULL, 0, NOW(), %s, %s, %s, %s, %s, %s)
        ON CONFLICT (project_id, agent_id) DO NOTHING
    """
    await conn.execute(
        insert_sql,
        (
            project_id,
            agent_id,
            active_channel_id,
            parent_step_id,
            trace_id,
            profile_box_id,
            context_box_id,
            output_box_id,
        ),
    )


async def lease_agent_turn_with_conn(
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
    """Atomically lease an idle agent turn using an existing transaction/connection."""
    await ensure_agent_state_row_with_conn(
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

    lease_sql = """
        UPDATE state.agent_state_head
        SET status='dispatched',
            active_agent_turn_id=%s,
            turn_epoch=turn_epoch + 1,
            active_channel_id=COALESCE(%s, active_channel_id),
            active_recursion_depth=COALESCE(%s, 0),
            parent_step_id=%s,
            trace_id=%s,
            profile_box_id=COALESCE(%s, profile_box_id),
            context_box_id=COALESCE(%s, context_box_id),
            output_box_id=COALESCE(%s, output_box_id),
            expecting_correlation_id=NULL,
            waiting_tool_count=0,
            resume_deadline=NULL,
            updated_at=NOW()
        WHERE project_id=%s AND agent_id=%s AND status='idle'
        RETURNING turn_epoch
    """
    res = await conn.execute(
        lease_sql,
        (
            agent_turn_id,
            active_channel_id,
            active_recursion_depth,
            parent_step_id,
            trace_id,
            profile_box_id,
            context_box_id,
            output_box_id,
            project_id,
            agent_id,
        ),
    )
    row = await res.fetchone()
    if row is not None:
        return int(row["turn_epoch"])
    return None
