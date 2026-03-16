from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from core.cg_context import CGContext
from core.subject import subject_pattern
from infra.l0_engine import (
    AddressIntent,
    CommandIntent,
    DepthPolicy,
    JoinIntent,
    L0Engine,
    ReportIntent,
    SpawnIntent,
    TurnRef,
)


class DummyNats:
    def __init__(self) -> None:
        self.publish_calls: List[Dict[str, Any]] = []

    def merge_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        return dict(headers or {})

    async def publish_event(self, subject: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> None:
        self.publish_calls.append(
            {
                "subject": subject,
                "payload": dict(payload or {}),
                "headers": dict(headers or {}),
            }
        )


class DummyExecutionStore:
    def __init__(
        self,
        *,
        turn_row: Optional[Dict[str, Any]] = None,
        turn_rows_by_correlation: Optional[Dict[str, Dict[str, Any]]] = None,
        request_depth: Optional[int] = None,
    ) -> None:
        self.turn_row = dict(turn_row or {}) if turn_row is not None else None
        self.turn_rows_by_correlation = {
            str(key): dict(value)
            for key, value in dict(turn_rows_by_correlation or {}).items()
        }
        self.request_depth = request_depth
        self.enqueues: List[Dict[str, Any]] = []
        self.edges: List[Dict[str, Any]] = []
        self.inboxes: List[Dict[str, Any]] = []
        self.fetch_one_calls: List[Dict[str, Any]] = []

    async def get_request_recursion_depth_with_conn(
        self,
        conn: Any,
        *,
        project_id: str,
        correlation_id: str,
    ) -> Optional[int]:
        _ = conn, project_id, correlation_id
        return self.request_depth

    async def enqueue_with_conn(self, conn: Any, **kwargs: Any) -> bool:
        _ = conn
        self.enqueues.append(dict(kwargs))
        return True

    async def insert_execution_edge_with_conn(self, conn: Any, **kwargs: Any) -> bool:
        _ = conn
        self.edges.append(dict(kwargs))
        return True

    async def insert_inbox_with_conn(self, conn: Any, **kwargs: Any) -> bool:
        _ = conn
        self.inboxes.append(dict(kwargs))
        return True

    async def fetch_one(
        self,
        sql: str,
        params: Any,
        *,
        conn: Any = None,
    ) -> Optional[Dict[str, Any]]:
        self.fetch_one_calls.append({"sql": sql, "params": params, "conn": conn})
        if self.turn_rows_by_correlation:
            correlation_id = ""
            if isinstance(params, (tuple, list)) and len(params) >= 7:
                correlation_id = str(params[5] or "")
            if correlation_id and correlation_id in self.turn_rows_by_correlation:
                return dict(self.turn_rows_by_correlation[correlation_id])
        if self.turn_row is None:
            return None
        return dict(self.turn_row)


class MessageTypeAwareExecutionStore(DummyExecutionStore):
    async def fetch_one(
        self,
        sql: str,
        params: Any,
        *,
        conn: Any = None,
    ) -> Optional[Dict[str, Any]]:
        self.fetch_one_calls.append({"sql": sql, "params": params, "conn": conn})
        if self.turn_row is None:
            return None
        row_message_type = str(self.turn_row.get("message_type") or "")
        if row_message_type and f"'{row_message_type}'" not in sql:
            return None
        return dict(self.turn_row)


class DuplicateCommandExecutionStore(DummyExecutionStore):
    async def insert_execution_edge_with_conn(self, conn: Any, **kwargs: Any) -> bool:
        _ = conn, kwargs
        return False


class DummyResourceStore:
    async def fetch_roster(self, ctx: CGContext, *, conn: Any = None) -> Optional[Dict[str, Any]]:
        _ = conn
        return {"agent_id": ctx.agent_id}


class DummyCardBox:
    def __init__(self, cards_by_id: Optional[Dict[str, Any]] = None) -> None:
        self.cards_by_id = dict(cards_by_id or {})

    async def get_cards(self, card_ids: List[str], *, project_id: str, conn: Any = None) -> List[Any]:
        _ = project_id, conn
        cards: List[Any] = []
        for card_id in card_ids:
            card = self.cards_by_id.get(card_id)
            if card is not None:
                cards.append(card)
        return cards


@dataclass
class DummyHead:
    active_agent_turn_id: str
    turn_epoch: int
    active_channel_id: str = "public"
    trace_id: Optional[str] = None
    active_recursion_depth: int = 0
    parent_step_id: Optional[str] = None


class DummyStateStore:
    def __init__(
        self,
        *,
        leased_turn_epoch: Optional[int] = 1,
        head: Optional[DummyHead] = None,
    ) -> None:
        self.leased_turn_epoch = leased_turn_epoch
        self.head = head
        self.lease_calls: List[Dict[str, Any]] = []

    async def lease_agent_turn_with_conn(self, conn: Any, **kwargs: Any) -> Optional[int]:
        _ = conn
        self.lease_calls.append(dict(kwargs))
        return self.leased_turn_epoch

    async def fetch(
        self,
        ctx: CGContext,
        *,
        conn: Any = None,
    ) -> Optional[DummyHead]:
        _ = ctx, conn
        return self.head


def _source_ctx(
    *,
    agent_id: str = "sys.pmo",
    agent_turn_id: str = "turn_parent",
    step_id: Optional[str] = "step_parent",
) -> CGContext:
    return CGContext(
        project_id="proj_test",
        channel_id="public",
        agent_id=agent_id,
        agent_turn_id=agent_turn_id,
        turn_epoch=7,
        step_id=step_id,
        trace_id="11111111111111111111111111111111",
        recursion_depth=2,
        headers={},
    )


@pytest.mark.asyncio
async def test_enqueue_intent_descend_derives_parent_and_depth() -> None:
    execution_store = DummyExecutionStore()
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(leased_turn_epoch=1),
    )

    result = await engine.enqueue_intent(
        source_ctx=_source_ctx(),
        intent=SpawnIntent(
            target_agent_id="agent_child",
            message_type="turn",
            payload={"profile_box_id": "p", "context_box_id": "c", "output_box_id": "o"},
            correlation_id="turn_child_1",
            depth_policy=DepthPolicy.DESCEND,
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert result.payload.get("agent_turn_id")
    assert result.payload.get("correlation_id") == "turn_child_1"
    assert len(execution_store.enqueues) == 1
    write_ctx = execution_store.enqueues[0]["ctx"]
    assert write_ctx.agent_id == "agent_child"
    assert write_ctx.agent_turn_id
    assert write_ctx.agent_turn_id != "turn_child_1"
    assert write_ctx.recursion_depth == 3
    assert write_ctx.parent_agent_id == "sys.pmo"
    assert write_ctx.parent_agent_turn_id == "turn_parent"
    assert write_ctx.parent_step_id == "step_parent"


@pytest.mark.asyncio
async def test_enqueue_intent_peer_keeps_depth() -> None:
    execution_store = DummyExecutionStore()
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(leased_turn_epoch=1),
    )

    result = await engine.enqueue_intent(
        source_ctx=_source_ctx(),
        intent=SpawnIntent(
            target_agent_id="agent_peer",
            message_type="turn",
            payload={"profile_box_id": "p", "context_box_id": "c", "output_box_id": "o"},
            correlation_id="corr_peer_1",
            depth_policy=DepthPolicy.PEER,
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert result.payload.get("correlation_id") == "corr_peer_1"
    assert len(execution_store.enqueues) == 1
    assert execution_store.enqueues[0]["ctx"].recursion_depth == 2


@pytest.mark.asyncio
async def test_enqueue_intent_root_is_caller_declared() -> None:
    execution_store = DummyExecutionStore()
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(leased_turn_epoch=1),
    )

    result = await engine.enqueue_intent(
        source_ctx=_source_ctx(agent_id="agent.user"),
        intent=SpawnIntent(
            target_agent_id="agent_root",
            message_type="turn",
            payload={"profile_box_id": "p", "context_box_id": "c", "output_box_id": "o"},
            depth_policy=DepthPolicy.ROOT,
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert result.error_code is None
    assert len(execution_store.enqueues) == 1
    write_ctx = execution_store.enqueues[0]["ctx"]
    assert write_ctx.recursion_depth == 0
    assert write_ctx.parent_agent_id is None
    assert write_ctx.parent_agent_turn_id is None
    assert write_ctx.parent_step_id is None


@pytest.mark.asyncio
async def test_enqueue_intent_root_keeps_declared_semantics_for_service_source() -> None:
    execution_store = DummyExecutionStore()
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(leased_turn_epoch=1),
    )

    result = await engine.enqueue_intent(
        source_ctx=_source_ctx(agent_id="sys.ui_worker"),
        intent=SpawnIntent(
            target_agent_id="agent_root",
            message_type="turn",
            payload={"profile_box_id": "p", "context_box_id": "c", "output_box_id": "o"},
            depth_policy=DepthPolicy.ROOT,
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert result.error_code is None
    assert len(execution_store.enqueues) == 1
    write_ctx = execution_store.enqueues[0]["ctx"]
    assert write_ctx.recursion_depth == 0
    assert write_ctx.parent_agent_id is None
    assert write_ctx.parent_agent_turn_id is None
    assert write_ctx.parent_step_id is None


@pytest.mark.asyncio
async def test_enqueue_intent_address_uses_turn_ref_and_keeps_depth() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "5",
                "CG-Step-Id": "step_target_1",
                "CG-Parent-Agent-Id": "sys.pmo",
                "CG-Parent-Turn-Id": "turn_parent",
                "CG-Parent-Step-Id": "step_parent",
            },
            "step_id": "step_target_1",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 5,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": "step_parent",
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.enqueue_intent(
        source_ctx=_source_ctx(),
        intent=AddressIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="stop",
            payload={"reason": "manual_stop"},
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert len(execution_store.enqueues) == 1
    enqueue_req = execution_store.enqueues[0]
    assert enqueue_req["message_type"] == "stop"
    assert enqueue_req["ctx"].agent_id == "agent_target"
    assert enqueue_req["ctx"].agent_turn_id == "turn_target_1"
    assert enqueue_req["ctx"].turn_epoch == 4
    assert enqueue_req["ctx"].recursion_depth == 5


@pytest.mark.asyncio
async def test_enqueue_intent_address_resolves_ui_action_turn_ref_without_correlation() -> None:
    execution_store = MessageTypeAwareExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "message_type": "ui_action",
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "5",
            },
        }
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.enqueue_intent(
        source_ctx=_source_ctx(),
        intent=AddressIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="stop",
            payload={"reason": "manual_stop"},
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert result.error_code is None
    assert len(execution_store.enqueues) == 1
    fetch_call = execution_store.fetch_one_calls[0]
    assert "'ui_action'" in fetch_call["sql"]


@pytest.mark.asyncio
async def test_report_intent_uses_turn_ref_and_enqueues_report() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_target_1",
                "CG-Parent-Agent-Id": "sys.pmo",
                "CG-Parent-Turn-Id": "turn_parent",
                "CG-Parent-Step-Id": "step_parent",
            },
            "step_id": "step_target_1",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": "step_parent",
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_1",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert len(execution_store.enqueues) == 1
    enqueue_req = execution_store.enqueues[0]
    assert enqueue_req["message_type"] == "tool_result"
    assert enqueue_req["ctx"].agent_id == "agent_target"
    assert enqueue_req["ctx"].agent_turn_id == "turn_target_1"
    assert enqueue_req["ctx"].turn_epoch == 4


@pytest.mark.asyncio
async def test_report_intent_overrides_stale_tool_call_id_with_correlation() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "1",
                "CG-Tool-Call-Id": "call_b",
            },
            "step_id": "step_target_1",
            "tool_call_id": "call_b",
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": "step_parent",
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_a",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert len(execution_store.enqueues) == 1
    enqueue_req = execution_store.enqueues[0]
    assert enqueue_req["ctx"].tool_call_id == "call_a"


@pytest.mark.asyncio
async def test_report_intent_resolves_turn_ref_with_intent_correlation_id() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "1",
            },
            "step_id": "step_target_1",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": "step_parent",
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_a",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert len(execution_store.fetch_one_calls) == 1
    fetch_call = execution_store.fetch_one_calls[0]
    assert "message_type='command_anchor'" in fetch_call["sql"]
    assert isinstance(fetch_call["params"], tuple)
    assert fetch_call["params"][5] == "call_a"
    assert fetch_call["params"][6] == "call_a"


@pytest.mark.asyncio
async def test_report_intent_prefers_matching_anchor_over_neighbor_anchor() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_neighbor_b",
                "CG-Tool-Call-Id": "call_b",
            },
            "step_id": "step_neighbor_b",
            "tool_call_id": "call_b",
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "cccccccccccccccccccccccccccccccc",
            "parent_step_id": "step_parent",
        },
        turn_rows_by_correlation={
            "call_a": {
                "project_id": "proj_test",
                "agent_id": "agent_target",
                "channel_id": "public",
                "agent_turn_id": "turn_target_1",
                "turn_epoch": 4,
                "context_headers": {
                    "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                    "CG-Recursion-Depth": "1",
                    "CG-Step-Id": "step_anchor_a",
                    "CG-Tool-Call-Id": "call_a",
                },
                "step_id": "step_anchor_a",
                "tool_call_id": "call_a",
                "parent_agent_id": "sys.pmo",
                "parent_agent_turn_id": "turn_parent",
                "recursion_depth": 1,
                "parent_traceparent": None,
                "traceparent": None,
                "tracestate": None,
                "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "parent_step_id": "step_parent",
            }
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_a",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert len(execution_store.enqueues) == 1
    enqueue_req = execution_store.enqueues[0]
    assert enqueue_req["ctx"].step_id == "step_anchor_a"
    assert enqueue_req["ctx"].tool_call_id == "call_a"


@pytest.mark.asyncio
async def test_report_intent_falls_back_to_turn_when_anchor_missing() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-ffffffffffffffff-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_turn_root",
            },
            "step_id": "step_turn_root",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "parent_step_id": "step_parent",
        },
        turn_rows_by_correlation={
            "call_b": {
                "project_id": "proj_test",
                "agent_id": "agent_target",
                "channel_id": "public",
                "agent_turn_id": "turn_target_1",
                "turn_epoch": 4,
                "context_headers": {
                    "traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
                    "CG-Recursion-Depth": "1",
                    "CG-Step-Id": "step_neighbor_b",
                    "CG-Tool-Call-Id": "call_b",
                },
                "step_id": "step_neighbor_b",
                "tool_call_id": "call_b",
                "parent_agent_id": "sys.pmo",
                "parent_agent_turn_id": "turn_parent",
                "recursion_depth": 1,
                "parent_traceparent": None,
                "traceparent": None,
                "tracestate": None,
                "trace_id": "cccccccccccccccccccccccccccccccc",
                "parent_step_id": "step_parent",
            }
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_a",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert len(execution_store.enqueues) == 1
    enqueue_req = execution_store.enqueues[0]
    assert enqueue_req["ctx"].step_id == "step_turn_root"
    assert enqueue_req["ctx"].tool_call_id == "call_a"


@pytest.mark.asyncio
async def test_report_intent_rejects_non_response_message_type() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "0",
            },
            "step_id": None,
            "tool_call_id": None,
            "parent_agent_id": None,
            "parent_agent_turn_id": None,
            "recursion_depth": 0,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": None,
        }
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="stop",
            payload={"reason": "manual_stop"},
            correlation_id="turn_target_1",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "rejected"
    assert result.error_code == "protocol_violation"
    assert execution_store.enqueues == []


@pytest.mark.asyncio
async def test_report_intent_rejects_epoch_mismatch() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "0",
            },
            "step_id": None,
            "tool_call_id": None,
            "parent_agent_id": None,
            "parent_agent_turn_id": None,
            "recursion_depth": 0,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": None,
        }
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=5,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_1",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "rejected"
    assert result.error_code == "protocol_violation"
    assert execution_store.enqueues == []


@pytest.mark.asyncio
async def test_report_intent_rejects_when_turn_ref_not_found_in_inbox() -> None:
    execution_store = DummyExecutionStore(turn_row=None)
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_missing",
                expected_turn_epoch=1,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_1",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "rejected"
    assert result.error_code == "protocol_violation"
    assert execution_store.enqueues == []


@pytest.mark.asyncio
async def test_report_intent_rejects_author_mismatch_against_tool_result_card() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_target_1",
                "CG-Parent-Agent-Id": "sys.pmo",
                "CG-Parent-Turn-Id": "turn_parent",
                "CG-Parent-Step-Id": "step_parent",
            },
            "step_id": "step_target_1",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": "step_parent",
        },
        request_depth=1,
    )
    cardbox = DummyCardBox(
        {
            "card_1": SimpleNamespace(
                card_id="card_1",
                type="tool.result",
                tool_call_id="call_1",
                author_id="tool.other",
                metadata={"tool_call_id": "call_1", "function_name": "skills.run_cmd"},
            )
        }
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
        cardbox=cardbox,
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(agent_id="tool.skills"),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="tool_result",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_1",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "rejected"
    assert result.error_code == "protocol_violation"
    assert execution_store.enqueues == []


@pytest.mark.asyncio
async def test_report_intent_accepts_watchdog_author_alias_mapping() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_target_1",
                "CG-Parent-Agent-Id": "sys.pmo",
                "CG-Parent-Turn-Id": "turn_parent",
                "CG-Parent-Step-Id": "step_parent",
            },
            "step_id": "step_target_1",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": "step_parent",
        },
        request_depth=1,
    )
    cardbox = DummyCardBox(
        {
            "card_1": SimpleNamespace(
                card_id="card_1",
                type="tool.result",
                tool_call_id="call_1",
                author_id="worker.watchdog",
                metadata={"tool_call_id": "call_1", "function_name": "skills.run_cmd"},
            )
        }
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
        cardbox=cardbox,
    )

    result = await engine.report_intent(
        source_ctx=_source_ctx(agent_id="sys.agent_worker.watchdog"),
        intent=ReportIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            message_type="timeout",
            payload={"tool_result_card_id": "card_1"},
            correlation_id="call_1",
        ),
        conn=object(),
        wakeup=False,
    )

    assert result.status == "accepted"
    assert len(execution_store.enqueues) == 1


@pytest.mark.asyncio
async def test_join_intent_writes_join_edge() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_target_1",
                "CG-Parent-Agent-Id": "sys.pmo",
                "CG-Parent-Turn-Id": "turn_parent",
                "CG-Parent-Step-Id": "step_parent",
            },
            "step_id": "step_target_1",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "parent_step_id": "step_parent",
        }
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.join_intent(
        source_ctx=_source_ctx(),
        intent=JoinIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            phase="request",
            correlation_id="batch_1",
            metadata={"batch_id": "batch_1"},
        ),
        conn=object(),
    )

    assert result.status == "accepted"
    assert len(execution_store.edges) == 1
    edge_req = execution_store.edges[0]
    assert edge_req["primitive"] == "join"
    assert edge_req["edge_phase"] == "request"
    assert edge_req["correlation_id"] == "batch_1"


@pytest.mark.asyncio
async def test_join_intent_prefers_matching_anchor_over_neighbor_anchor() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_neighbor_b",
                "CG-Tool-Call-Id": "call_b",
            },
            "step_id": "step_neighbor_b",
            "tool_call_id": "call_b",
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "cccccccccccccccccccccccccccccccc",
            "parent_step_id": "step_parent",
        },
        turn_rows_by_correlation={
            "call_a": {
                "project_id": "proj_test",
                "agent_id": "agent_target",
                "channel_id": "public",
                "agent_turn_id": "turn_target_1",
                "turn_epoch": 4,
                "context_headers": {
                    "traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
                    "CG-Recursion-Depth": "1",
                    "CG-Step-Id": "step_anchor_a",
                    "CG-Tool-Call-Id": "call_a",
                },
                "step_id": "step_anchor_a",
                "tool_call_id": "call_a",
                "parent_agent_id": "sys.pmo",
                "parent_agent_turn_id": "turn_parent",
                "recursion_depth": 1,
                "parent_traceparent": None,
                "traceparent": None,
                "tracestate": None,
                "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "parent_step_id": "step_parent",
            }
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.join_intent(
        source_ctx=_source_ctx(),
        intent=JoinIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            phase="request",
            correlation_id="call_a",
            metadata={"batch_id": "batch_a"},
        ),
        conn=object(),
    )

    assert result.status == "accepted"
    assert len(execution_store.edges) == 1
    edge_req = execution_store.edges[0]
    assert edge_req["ctx"].step_id == "step_anchor_a"
    assert edge_req["ctx"].tool_call_id == "call_a"


@pytest.mark.asyncio
async def test_join_intent_falls_back_to_turn_when_anchor_missing() -> None:
    execution_store = DummyExecutionStore(
        turn_row={
            "project_id": "proj_test",
            "agent_id": "agent_target",
            "channel_id": "public",
            "agent_turn_id": "turn_target_1",
            "turn_epoch": 4,
            "context_headers": {
                "traceparent": "00-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-ffffffffffffffff-01",
                "CG-Recursion-Depth": "1",
                "CG-Step-Id": "step_turn_root",
            },
            "step_id": "step_turn_root",
            "tool_call_id": None,
            "parent_agent_id": "sys.pmo",
            "parent_agent_turn_id": "turn_parent",
            "recursion_depth": 1,
            "parent_traceparent": None,
            "traceparent": None,
            "tracestate": None,
            "trace_id": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "parent_step_id": "step_parent",
        },
        turn_rows_by_correlation={
            "call_b": {
                "project_id": "proj_test",
                "agent_id": "agent_target",
                "channel_id": "public",
                "agent_turn_id": "turn_target_1",
                "turn_epoch": 4,
                "context_headers": {
                    "traceparent": "00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01",
                    "CG-Recursion-Depth": "1",
                    "CG-Step-Id": "step_neighbor_b",
                    "CG-Tool-Call-Id": "call_b",
                },
                "step_id": "step_neighbor_b",
                "tool_call_id": "call_b",
                "parent_agent_id": "sys.pmo",
                "parent_agent_turn_id": "turn_parent",
                "recursion_depth": 1,
                "parent_traceparent": None,
                "traceparent": None,
                "tracestate": None,
                "trace_id": "cccccccccccccccccccccccccccccccc",
                "parent_step_id": "step_parent",
            }
        },
        request_depth=1,
    )
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    result = await engine.join_intent(
        source_ctx=_source_ctx(),
        intent=JoinIntent(
            target=TurnRef(
                project_id="proj_test",
                agent_id="agent_target",
                agent_turn_id="turn_target_1",
                expected_turn_epoch=4,
            ),
            phase="request",
            correlation_id="call_a",
            metadata={"batch_id": "batch_a"},
        ),
        conn=object(),
    )

    assert result.status == "accepted"
    assert len(execution_store.edges) == 1
    edge_req = execution_store.edges[0]
    assert edge_req["ctx"].step_id == "step_turn_root"
    assert edge_req["ctx"].tool_call_id is None


@pytest.mark.asyncio
async def test_command_intent_records_edge_and_emits_signal() -> None:
    nats = DummyNats()
    execution_store = DummyExecutionStore()
    engine = L0Engine(
        nats=nats,
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )
    subject = subject_pattern(
        project_id="proj_test",
        channel_id="public",
        category="cmd",
        component="tool",
        target="search",
        suffix="call",
    )

    source_ctx = _source_ctx().with_tool_call("call_1")
    result = await engine.command_intent(
        source_ctx=source_ctx,
        intent=CommandIntent(
            subject=subject,
            payload={
                "tool_name": "search",
                "after_execution": "suspend",
                "tool_call_card_id": "card_1",
            },
            correlation_id="call_1",
        ),
        conn=object(),
    )

    assert result.status == "accepted"
    assert result.payload["idempotent_hit"] is False
    assert len(execution_store.edges) == 1
    edge_req = execution_store.edges[0]
    assert edge_req["primitive"] == "tool_call"
    assert edge_req["edge_phase"] == "request"
    assert edge_req["correlation_id"] == "call_1"
    assert edge_req["metadata"]["subject"] == subject
    assert len(result.command_signals) == 1
    signal = result.command_signals[0]
    assert signal.subject == subject
    assert signal.payload["tool_name"] == "search"
    assert "tool_call_id" not in signal.payload
    assert signal.headers.get("CG-Step-Id") == "step_parent"
    assert signal.headers.get("CG-Tool-Call-Id") == "call_1"

    await engine.publish_command_signals(list(result.command_signals))
    assert len(nats.publish_calls) == 1
    assert nats.publish_calls[0]["subject"] == subject
    assert nats.publish_calls[0]["payload"]["tool_name"] == "search"
    assert "tool_call_id" not in nats.publish_calls[0]["payload"]


@pytest.mark.asyncio
async def test_command_intent_duplicate_correlation_is_idempotent_without_signal() -> None:
    nats = DummyNats()
    execution_store = DuplicateCommandExecutionStore()
    engine = L0Engine(
        nats=nats,
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )
    subject = subject_pattern(
        project_id="proj_test",
        channel_id="public",
        category="cmd",
        component="tool",
        target="search",
        suffix="call",
    )

    result = await engine.command_intent(
        source_ctx=_source_ctx().with_tool_call("call_dup"),
        intent=CommandIntent(
            subject=subject,
            payload={"tool_name": "search", "after_execution": "suspend"},
            correlation_id="call_dup",
        ),
        conn=object(),
    )

    assert result.status == "accepted"
    assert result.payload["idempotent_hit"] is True
    assert result.command_signals == ()
    assert execution_store.inboxes == []

    await engine.publish_command_signals(list(result.command_signals))
    assert nats.publish_calls == []


@pytest.mark.asyncio
async def test_command_intent_external_source_inserts_consumed_anchor_inbox() -> None:
    execution_store = DummyExecutionStore()
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )
    subject = subject_pattern(
        project_id="proj_test",
        channel_id="public",
        category="cmd",
        component="tool",
        target="search",
        suffix="call",
    )
    source_ctx = _source_ctx(agent_id="agent.external", step_id="step_external").with_tool_call("call_ext_1")
    result = await engine.command_intent(
        source_ctx=source_ctx,
        intent=CommandIntent(
            subject=subject,
            payload={"tool_name": "search"},
            correlation_id="call_ext_1",
        ),
        conn=object(),
    )

    assert result.status == "accepted"
    assert len(execution_store.inboxes) == 1
    anchor = execution_store.inboxes[0]
    assert anchor["message_type"] == "command_anchor"
    assert anchor["status"] == "consumed"
    assert anchor["correlation_id"] == "call_ext_1"
    assert (anchor_ctx := anchor.get("ctx"))
    assert anchor_ctx.agent_turn_id == "turn_parent"


@pytest.mark.asyncio
async def test_command_intent_rejects_invalid_subjects() -> None:
    execution_store = DummyExecutionStore()
    engine = L0Engine(
        nats=DummyNats(),
        execution_store=execution_store,
        resource_store=DummyResourceStore(),
        state_store=DummyStateStore(),
    )

    bad_evt_subject = subject_pattern(
        project_id="proj_test",
        channel_id="public",
        category="evt",
        component="tool",
        target="search",
        suffix="call",
    )
    result_evt = await engine.command_intent(
        source_ctx=_source_ctx(),
        intent=CommandIntent(
            subject=bad_evt_subject,
            payload={},
            correlation_id="call_1",
        ),
        conn=object(),
    )
    assert result_evt.status == "rejected"
    assert result_evt.error_code == "protocol_violation"

    wakeup_subject = subject_pattern(
        project_id="proj_test",
        channel_id="public",
        category="cmd",
        component="agent",
        target="worker",
        suffix="wakeup",
    )
    result_wakeup = await engine.command_intent(
        source_ctx=_source_ctx(),
        intent=CommandIntent(
            subject=wakeup_subject,
            payload={"agent_id": "agent_1"},
            correlation_id="wake_1",
        ),
        conn=object(),
    )
    assert result_wakeup.status == "rejected"
    assert result_wakeup.error_code == "protocol_violation"

    mismatch_subject = subject_pattern(
        project_id="proj_other",
        channel_id="public",
        category="cmd",
        component="tool",
        target="search",
        suffix="call",
    )
    result_mismatch = await engine.command_intent(
        source_ctx=_source_ctx(),
        intent=CommandIntent(
            subject=mismatch_subject,
            payload={},
            correlation_id="call_1",
        ),
        conn=object(),
    )
    assert result_mismatch.status == "rejected"
    assert result_mismatch.error_code == "protocol_violation"
