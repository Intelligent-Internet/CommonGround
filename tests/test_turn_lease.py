import pytest

from infra.stores.turn_lease import (
    ensure_agent_state_row_with_conn,
    lease_agent_turn_with_conn,
)


class _FakeResult:
    def __init__(self, row):
        self._row = row

    async def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, rows=None):
        self.calls = []
        self._rows = list(rows or [])

    async def execute(self, sql, params):
        self.calls.append((sql, params))
        row = self._rows.pop(0) if self._rows else None
        return _FakeResult(row)


@pytest.mark.asyncio
async def test_ensure_agent_state_row_with_conn_builds_expected_insert_params() -> None:
    conn = _FakeConn()

    await ensure_agent_state_row_with_conn(
        conn,
        project_id="proj_1",
        agent_id="agent_1",
        active_channel_id="public",
        parent_step_id="step_parent",
        trace_id="trace_1",
        profile_box_id="box_profile",
        context_box_id="box_context",
        output_box_id="box_output",
    )

    assert len(conn.calls) == 1
    sql, params = conn.calls[0]
    assert "INSERT INTO state.agent_state_head" in sql
    assert params == (
        "proj_1",
        "agent_1",
        "public",
        "step_parent",
        "trace_1",
        "box_profile",
        "box_context",
        "box_output",
    )


@pytest.mark.asyncio
async def test_lease_agent_turn_with_conn_insert_then_update_and_return_epoch() -> None:
    conn = _FakeConn(rows=[None, {"turn_epoch": 7}])

    epoch = await lease_agent_turn_with_conn(
        conn,
        project_id="proj_1",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        active_channel_id="public",
        profile_box_id="box_profile",
        context_box_id="box_context",
        output_box_id="box_output",
        parent_step_id="step_parent",
        trace_id="trace_1",
        active_recursion_depth=3,
    )

    assert epoch == 7
    assert len(conn.calls) == 2
    insert_sql, insert_params = conn.calls[0]
    update_sql, update_params = conn.calls[1]
    assert "INSERT INTO state.agent_state_head" in insert_sql
    assert "SET status='dispatched'" in update_sql
    assert insert_params == (
        "proj_1",
        "agent_1",
        "public",
        "step_parent",
        "trace_1",
        "box_profile",
        "box_context",
        "box_output",
    )
    assert update_params == (
        "turn_1",
        "public",
        3,
        "step_parent",
        "trace_1",
        "box_profile",
        "box_context",
        "box_output",
        "proj_1",
        "agent_1",
    )


@pytest.mark.asyncio
async def test_lease_agent_turn_with_conn_returns_none_when_row_missing() -> None:
    conn = _FakeConn(rows=[None, None])

    epoch = await lease_agent_turn_with_conn(
        conn,
        project_id="proj_1",
        agent_id="agent_1",
        agent_turn_id="turn_1",
    )

    assert epoch is None
    assert len(conn.calls) == 2
