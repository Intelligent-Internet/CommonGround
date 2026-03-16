import pytest

from core.cg_context import CGContext
from infra.stores.execution_store import ExecutionStore


class _CaptureExecutionStore(ExecutionStore):
    def __init__(self) -> None:
        # Avoid pool setup; tests only exercise SQL/params assembly.
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch_all(self, sql, params=None, *, conn=None):  # type: ignore[override]
        _ = conn
        self.calls.append((sql, tuple(params or ())))
        return []


@pytest.mark.asyncio
async def test_claim_pending_inbox_update_scoped_by_project_id() -> None:
    store = _CaptureExecutionStore()
    ctx = CGContext(project_id="proj_a", agent_id="agent_1")

    await store.claim_pending_inbox(ctx=ctx, limit=5)

    assert len(store.calls) == 1
    sql, params = store.calls[0]
    assert "UPDATE state.agent_inbox" in sql
    assert "WHERE project_id=%s" in sql
    assert "AND inbox_id IN (SELECT inbox_id FROM candidates)" in sql
    assert params == ("proj_a", "agent_1", 5, "proj_a")


@pytest.mark.asyncio
async def test_claim_pending_inbox_by_correlation_update_scoped_by_project_id() -> None:
    store = _CaptureExecutionStore()
    ctx = CGContext(project_id="proj_a", agent_id="agent_1")

    await store.claim_pending_inbox_by_correlation(
        ctx=ctx,
        correlation_id="corr_1",
        limit=7,
    )

    assert len(store.calls) == 1
    sql, params = store.calls[0]
    assert "UPDATE state.agent_inbox" in sql
    assert "WHERE project_id=%s" in sql
    assert "AND inbox_id IN (SELECT inbox_id FROM candidates)" in sql
    assert params == ("proj_a", "agent_1", "corr_1", 7, "proj_a")
