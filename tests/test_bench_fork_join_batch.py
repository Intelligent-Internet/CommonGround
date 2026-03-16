from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, List

import pytest

from scripts.benchmarks.bench_fork_join_batch import _wait_for_tool_result_inbox


class _StubExecutionStore:
    def __init__(self) -> None:
        self.list_calls: List[Dict[str, Any]] = []
        self.update_calls: List[Dict[str, Any]] = []

    async def list_inbox_by_correlation(
        self,
        *,
        ctx: Any,
        correlation_id: str,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        self.list_calls.append(
            {
                "ctx": ctx,
                "correlation_id": correlation_id,
                "limit": limit,
            }
        )
        return [
            {
                "inbox_id": "inbox_tool_result_1",
                "message_type": "tool_result",
                "created_at": datetime.now(UTC),
                "payload": {"status": "ok"},
            }
        ]

    async def update_inbox_status(
        self,
        *,
        inbox_id: str,
        project_id: str,
        status: str,
    ) -> None:
        self.update_calls.append(
            {
                "inbox_id": inbox_id,
                "project_id": project_id,
                "status": status,
            }
        )


@pytest.mark.asyncio
async def test_wait_for_tool_result_inbox_uses_ctx_signature() -> None:
    store = _StubExecutionStore()
    payload, created_at = await _wait_for_tool_result_inbox(
        execution_store=store,
        project_id="proj_test",
        agent_id="agent_target",
        tool_call_id="call_1",
        timeout_s=0.3,
        poll_interval_s=0.01,
    )

    assert payload == {"status": "ok"}
    assert isinstance(created_at, datetime)
    assert len(store.list_calls) == 1
    call = store.list_calls[0]
    ctx = call["ctx"]
    assert ctx.project_id == "proj_test"
    assert ctx.agent_id == "agent_target"
    assert call["correlation_id"] == "call_1"
    assert call["limit"] == 20
    assert len(store.update_calls) == 1
    assert store.update_calls[0]["inbox_id"] == "inbox_tool_result_1"
    assert store.update_calls[0]["project_id"] == "proj_test"
    assert store.update_calls[0]["status"] == "consumed"
