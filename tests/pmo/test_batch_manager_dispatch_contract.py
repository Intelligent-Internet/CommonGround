from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from core.cg_context import CGContext
from infra.agent_dispatcher import DispatchResult
from services.pmo.l1_orchestrators.batch_manager import BatchManager
import services.pmo.l1_orchestrators.batch_manager as batch_manager_module


class _DummySpan:
    def set_attribute(self, *_args: Any, **_kwargs: Any) -> None:
        return None


@contextmanager
def _dummy_start_span(*_args: Any, **_kwargs: Any):
    yield _DummySpan()


class _DummyBatchStore:
    def __init__(
        self,
        *,
        pending_rows: Optional[List[Dict[str, Any]]] = None,
        stuck_sequences: Optional[List[List[Dict[str, Any]]]] = None,
    ) -> None:
        self.pending_rows = list(pending_rows or [])
        self.stuck_sequences = list(stuck_sequences or [])
        self.claim_calls: List[Dict[str, Any]] = []
        self.patch_calls: List[Dict[str, Any]] = []
        self.identity_calls: List[Dict[str, Any]] = []
        self.failed_calls: List[Dict[str, Any]] = []
        self.revert_calls: List[Dict[str, Any]] = []
        self.stuck_calls: List[Dict[str, Any]] = []
        self.terminal_calls: List[Dict[str, Any]] = []

    async def list_pending_tasks(self, *, limit: int = 200) -> List[Dict[str, Any]]:
        _ = limit
        return list(self.pending_rows)

    async def claim_task_dispatched(self, **kwargs: Any) -> bool:
        self.claim_calls.append(dict(kwargs))
        return True

    async def patch_task_metadata(self, **kwargs: Any) -> None:
        self.patch_calls.append(dict(kwargs))

    async def set_task_turn_identity(self, **kwargs: Any) -> None:
        self.identity_calls.append(dict(kwargs))

    async def mark_task_dispatch_failed(self, **kwargs: Any) -> Optional[str]:
        self.failed_calls.append(dict(kwargs))
        return None

    async def revert_task_to_pending(self, **kwargs: Any) -> None:
        self.revert_calls.append(dict(kwargs))

    async def list_stuck_dispatched_missing_epoch(
        self,
        *,
        older_than_seconds: float,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        self.stuck_calls.append(
            {
                "older_than_seconds": older_than_seconds,
                "limit": limit,
            }
        )
        if not self.stuck_sequences:
            return []
        return list(self.stuck_sequences.pop(0))

    async def mark_task_terminal_from_turn(self, **kwargs: Any) -> Optional[str]:
        self.terminal_calls.append(dict(kwargs))
        return None

    async def set_task_turn_epoch(self, **_kwargs: Any) -> None:
        raise AssertionError("BatchManager should not archaeology-backfill turn_epoch in this contract")


class _DummyCardBox:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    async def ensure_box_id(
        self,
        *,
        project_id: str,
        box_id: Optional[str],
        conn: Any = None,
    ) -> str:
        self.calls.append(
            {
                "project_id": project_id,
                "box_id": box_id,
                "conn": conn,
            }
        )
        return str(box_id or "output_box_generated")


class _DummyExecutionStore:
    def __init__(self, *, inbox_rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.inbox_rows = list(inbox_rows or [])
        self.turn_lookup_calls: List[Dict[str, Any]] = []
        self.correlation_lookup_calls: List[Dict[str, Any]] = []

    async def list_inbox_by_correlation(self, *, ctx: CGContext, correlation_id: str, limit: int = 1) -> List[Dict[str, Any]]:
        self.correlation_lookup_calls.append(
            {
                "ctx": ctx,
                "correlation_id": correlation_id,
                "limit": limit,
            }
        )
        return list(self.inbox_rows)

    async def list_inbox_by_agent_turn_id(
        self,
        *,
        ctx: CGContext,
        agent_turn_id: str,
        limit: int = 1,
    ) -> List[Dict[str, Any]]:
        self.turn_lookup_calls.append(
            {
                "ctx": ctx,
                "agent_turn_id": agent_turn_id,
                "limit": limit,
            }
        )
        return list(self.inbox_rows)


class _DummyStateStore:
    def __init__(self, *, head: Any = None) -> None:
        self.head = head
        self.fetch_calls: List[CGContext] = []

    async def fetch(self, ctx: CGContext, conn: Any = None) -> Any:
        _ = conn
        self.fetch_calls.append(ctx)
        return self.head


def _parent_ctx() -> CGContext:
    return CGContext(
        project_id="proj_1",
        channel_id="public",
        agent_id="agent_parent",
        agent_turn_id="turn_parent",
        turn_epoch=1,
        step_id="step_parent",
        trace_id="1" * 32,
        recursion_depth=1,
        headers={},
    )


def _pending_row() -> Dict[str, Any]:
    parent_ctx = _parent_ctx()
    return {
        "batch_task_id": "bt_1",
        "batch_id": "batch_1",
        "project_id": "proj_1",
        "agent_id": "agent_child",
        "profile_box_id": "profile_1",
        "context_box_id": "context_1",
        "output_box_id": "output_box_1",
        "attempt_count": 0,
        "task_metadata": {
            "task_args": {"label": "Child 1"},
            "runtime_config": {"mode": "test"},
        },
        "batch_metadata": {
            "parent_ctx": BatchManager._ctx_snapshot(parent_ctx),
        },
        "deadline_at": None,
    }


def _stuck_row(*, task_metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "batch_task_id": "bt_1",
        "batch_id": "batch_1",
        "project_id": "proj_1",
        "agent_id": "agent_child",
        "current_agent_turn_id": "turn_child_l0",
        "current_turn_epoch": None,
        "attempt_count": 1,
        "task_metadata": dict(task_metadata or {}),
        "deadline_at": None,
        "fail_fast": False,
    }


def _manager(
    *,
    batch_store: _DummyBatchStore,
    execution_store: Optional[_DummyExecutionStore] = None,
    state_store: Optional[_DummyStateStore] = None,
) -> BatchManager:
    return BatchManager(
        resource_store=object(),  # type: ignore[arg-type]
        state_store=state_store or _DummyStateStore(),  # type: ignore[arg-type]
        batch_store=batch_store,  # type: ignore[arg-type]
        cardbox=_DummyCardBox(),  # type: ignore[arg-type]
        nats=object(),  # type: ignore[arg-type]
        execution_store=execution_store or _DummyExecutionStore(),  # type: ignore[arg-type]
        identity_store=object(),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_batch_dispatch_uses_intent_only_request(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: Dict[str, Any] = {}
    batch_store = _DummyBatchStore(pending_rows=[_pending_row()])
    manager = _manager(batch_store=batch_store)

    class _FakeDispatcher:
        def __init__(self, **kwargs: Any) -> None:
            captured["dispatcher_init"] = dict(kwargs)

        async def dispatch(self, req: Any) -> DispatchResult:
            captured["request"] = req
            return DispatchResult(
                status="accepted",
                agent_id=req.target_agent_id,
                agent_turn_id="turn_child_l0",
                turn_epoch=4,
                recursion_depth=2,
                output_box_id="output_box_1",
                trace_id="2" * 32,
            )

    monkeypatch.setattr(batch_manager_module, "AgentDispatcher", _FakeDispatcher)
    monkeypatch.setattr(batch_manager_module, "build_traceparent", lambda _arg: ("00-" + "2" * 32 + "-" + "3" * 16 + "-01", "2" * 32))
    monkeypatch.setattr(batch_manager_module, "link_from_traceparent", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(batch_manager_module, "current_traceparent", lambda: None)
    monkeypatch.setattr(batch_manager_module, "extract_context_from_headers", lambda _headers: None)
    monkeypatch.setattr(batch_manager_module, "start_span", _dummy_start_span)

    await manager.dispatch_pending_tasks(limit=1)

    req = captured["request"]
    assert req.target_agent_id == "agent_child"
    assert req.target_channel_id == "public"
    assert req.correlation_id == "bt_1"
    assert req.profile_box_id == "profile_1"
    assert req.context_box_id == "context_1"
    assert req.output_box_id == "output_box_1"
    assert not hasattr(req, "target_ctx")

    assert batch_store.claim_calls[0]["agent_turn_id"] is None
    assert batch_store.patch_calls[0]["metadata_patch"]["dispatch_channel_id"] == "public"
    assert batch_store.identity_calls == [
        {
            "batch_task_id": "bt_1",
            "project_id": "proj_1",
            "agent_turn_id": "turn_child_l0",
            "turn_epoch": 4,
        }
    ]


@pytest.mark.asyncio
async def test_batch_reconcile_wakeup_uses_dispatch_channel_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    batch_store = _DummyBatchStore(
        stuck_sequences=[
            [_stuck_row(task_metadata={"dispatch_channel_id": "private"})],
            [],
        ]
    )
    execution_store = _DummyExecutionStore(inbox_rows=[{"status": "queued"}])
    manager = _manager(batch_store=batch_store, execution_store=execution_store)
    wakeup_calls: List[Dict[str, Any]] = []

    async def _fake_wakeup(*, target_ctx: CGContext, mode: str, reason: str, metadata: Dict[str, Any]) -> Any:
        wakeup_calls.append(
            {
                "target_ctx": target_ctx,
                "mode": mode,
                "reason": reason,
                "metadata": metadata,
            }
        )
        return SimpleNamespace(ack_status="accepted")

    monkeypatch.setattr(manager.l0, "wakeup", _fake_wakeup)

    await manager.reconcile_dispatched_tasks(
        reconcile_after_seconds=0.0,
        revert_after_seconds=60.0,
        limit=10,
    )

    assert wakeup_calls[0]["target_ctx"].channel_id == "private"
    assert wakeup_calls[0]["target_ctx"].agent_id == "agent_child"
    assert execution_store.turn_lookup_calls == [
        {
            "ctx": CGContext(project_id="proj_1", agent_id="agent_child"),
            "agent_turn_id": "turn_child_l0",
            "limit": 1,
        }
    ]
    assert execution_store.correlation_lookup_calls == []
    assert batch_store.patch_calls[0]["metadata_patch"]["dispatch_wakeup_ack_status"] == "accepted"


@pytest.mark.asyncio
async def test_batch_reconcile_does_not_backfill_epoch_from_inbox() -> None:
    batch_store = _DummyBatchStore(
        stuck_sequences=[
            [_stuck_row(task_metadata={"dispatch_channel_id": "public"})],
            [],
        ]
    )
    execution_store = _DummyExecutionStore(inbox_rows=[{"status": "running", "turn_epoch": 9}])
    manager = _manager(batch_store=batch_store, execution_store=execution_store)

    await manager.reconcile_dispatched_tasks(
        reconcile_after_seconds=0.0,
        revert_after_seconds=60.0,
        limit=10,
    )

    assert execution_store.turn_lookup_calls == [
        {
            "ctx": CGContext(project_id="proj_1", agent_id="agent_child"),
            "agent_turn_id": "turn_child_l0",
            "limit": 1,
        }
    ]
    assert execution_store.correlation_lookup_calls == []
    assert batch_store.patch_calls == []
    assert batch_store.terminal_calls == []


@pytest.mark.asyncio
async def test_batch_reconcile_prefers_state_head_for_active_turn_epoch() -> None:
    batch_store = _DummyBatchStore(
        stuck_sequences=[
            [_stuck_row(task_metadata={"dispatch_channel_id": "public"})],
            [],
        ]
    )
    execution_store = _DummyExecutionStore(inbox_rows=[{"status": "queued"}])
    state_store = _DummyStateStore(
        head=SimpleNamespace(
            active_agent_turn_id="turn_child_l0",
            turn_epoch=9,
            status="running",
        )
    )
    manager = _manager(
        batch_store=batch_store,
        execution_store=execution_store,
        state_store=state_store,
    )

    await manager.reconcile_dispatched_tasks(
        reconcile_after_seconds=0.0,
        revert_after_seconds=60.0,
        limit=10,
    )

    assert state_store.fetch_calls == [CGContext(project_id="proj_1", agent_id="agent_child")]
    assert execution_store.turn_lookup_calls == []
    assert batch_store.patch_calls == []
