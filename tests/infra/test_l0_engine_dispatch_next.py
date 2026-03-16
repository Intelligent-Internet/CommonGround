from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import pytest

from core.cg_context import CGContext
from core.headers import RECURSION_DEPTH_HEADER
from core.trace import TRACEPARENT_HEADER
from infra.l0_engine import L0Engine


class _DummyNats:
    def merge_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        return dict(headers or {})


class _DummyExecutionStore:
    pass


class _DummyResourceStore:
    pass


class _DummyStateStore:
    def __init__(self, leased_turn_epoch: Optional[int] = 1) -> None:
        self.leased_turn_epoch = leased_turn_epoch
        self.lease_calls: List[Dict[str, Any]] = []

    async def lease_agent_turn_with_conn(self, conn: Any, **kwargs: Any) -> Optional[int]:
        _ = conn
        self.lease_calls.append(dict(kwargs))
        return self.leased_turn_epoch


class _Result:
    def __init__(self, row: Optional[Dict[str, Any]]) -> None:
        self._row = row

    async def fetchone(self) -> Optional[Dict[str, Any]]:
        return self._row


class _DummyConn:
    def __init__(
        self,
        *,
        queued_rows: list[Dict[str, Any]],
        promote_rows: Optional[list[Optional[Dict[str, Any]]]] = None,
    ) -> None:
        self.queued_rows = list(queued_rows)
        self.promote_rows = list(promote_rows or [])
        self.archived_patches: List[Dict[str, Any]] = []
        self.promote_attempts = 0

    async def execute(self, sql: str, params: Any) -> _Result:
        if "FROM state.agent_inbox" in sql and "FOR UPDATE SKIP LOCKED" in sql:
            return _Result(self.queued_rows.pop(0) if self.queued_rows else None)
        if "SET status='error'" in sql:
            self.archived_patches.append({"patch": json.loads(params[0])})
            return _Result(None)
        if "SET status='pending'" in sql:
            self.promote_attempts += 1
            return _Result(self.promote_rows.pop(0) if self.promote_rows else {"inbox_id": params[3]})
        raise AssertionError(f"unexpected sql: {sql}")


def _ctx() -> CGContext:
    return CGContext(project_id="proj_test", channel_id="public", agent_id="agent_target")


def _valid_headers() -> Dict[str, str]:
    return {
        TRACEPARENT_HEADER: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        RECURSION_DEPTH_HEADER: "1",
    }


def _queued_turn_row(payload: Any, *, context_headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    return {
        "inbox_id": "inbox_1",
        "project_id": "proj_test",
        "agent_id": "agent_target",
        "message_type": "turn",
        "correlation_id": "turn_1",
        "channel_id": "public",
        "agent_turn_id": "turn_1",
        "turn_epoch": 0,
        "context_headers": context_headers if context_headers is not None else _valid_headers(),
        "payload": payload,
        "created_at": "2026-01-01T00:00:00Z",
    }


def _valid_payload() -> Dict[str, Any]:
    return {
        "profile_box_id": "box_profile",
        "context_box_id": "box_context",
        "output_box_id": "box_output",
    }


def _engine(state_store: _DummyStateStore) -> L0Engine:
    return L0Engine(
        nats=_DummyNats(),
        execution_store=_DummyExecutionStore(),
        resource_store=_DummyResourceStore(),
        state_store=state_store,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row", "expected_error", "expect_error_detail"),
    [
        (_queued_turn_row("{not_json"), "invalid_payload_json", True),
        (_queued_turn_row('["bad"]'), "invalid_payload_type", False),
        (
            _queued_turn_row(
                _valid_payload(),
                context_headers={RECURSION_DEPTH_HEADER: "1"},
            ),
            "invalid_context",
            True,
        ),
        (
            _queued_turn_row({"profile_box_id": "box_profile", "context_box_id": "box_context"}),
            "missing_required_fields",
            False,
        ),
    ],
)
async def test_dispatch_next_archives_invalid_rows(
    row: Dict[str, Any],
    expected_error: str,
    expect_error_detail: bool,
) -> None:
    conn = _DummyConn(queued_rows=[row])
    state_store = _DummyStateStore()

    result = await _engine(state_store)._dispatch_next_if_idle_with_conn(conn, ctx=_ctx())

    assert result is None
    assert state_store.lease_calls == []
    patch = conn.archived_patches[0]["patch"]
    assert patch["dispatch_error"] == expected_error
    if expect_error_detail:
        assert "error" in patch


@pytest.mark.asyncio
async def test_dispatch_next_returns_none_when_promote_stale() -> None:
    conn = _DummyConn(queued_rows=[_queued_turn_row(_valid_payload())], promote_rows=[None])
    state_store = _DummyStateStore(leased_turn_epoch=7)

    result = await _engine(state_store)._dispatch_next_if_idle_with_conn(conn, ctx=_ctx())

    assert result is None
    assert len(state_store.lease_calls) == 1
    assert conn.promote_attempts == 1
    assert conn.archived_patches == []


@pytest.mark.asyncio
async def test_dispatch_next_returns_dispatch_payload_when_successful() -> None:
    conn = _DummyConn(queued_rows=[_queued_turn_row(_valid_payload())])
    state_store = _DummyStateStore(leased_turn_epoch=5)

    result = await _engine(state_store)._dispatch_next_if_idle_with_conn(conn, ctx=_ctx())

    assert isinstance(result, dict)
    assert result["inbox_id"] == "inbox_1"
    assert result["agent_turn_id"] == "turn_1"
    assert result["turn_epoch"] == 5
    assert result["recursion_depth"] == 1
    assert conn.archived_patches == []
