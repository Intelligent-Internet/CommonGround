from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pytest
from pydantic import ValidationError

from infra.tool_executor import ToolCallContext
from infra.idempotency import IdempotencyOutcome
from services.tools.tool_runner import ToolPayloadValidation, ToolRunner
from core.utp_protocol import ToolCallContent


@dataclass
class _DummyNats:
    def merge_headers(self, headers: Dict[str, str], extras: Dict[str, str] | None = None) -> Dict[str, str]:
        merged = dict(headers or {})
        if extras:
            merged.update(extras)
        return merged


@dataclass
class _DummyCardBox:
    saved_cards: List[Any]
    cards: Dict[str, Any]

    def __post_init__(self) -> None:
        if not self.cards:
            self.cards = {
                "tc_1": type(
                    "DummyCard",
                    (),
                    {
                        "card_id": "tc_1",
                        "type": "tool.call",
                        "tool_call_id": "call_1",
                        "content": ToolCallContent(
                            tool_name="demo",
                            arguments={"foo": "bar"},
                            status="called",
                            target_subject=None,
                        ),
                        "metadata": {"step_id": "step_1"},
                    },
                )()
            }

    async def save_card(self, card: Any, conn: Any = None) -> None:
        _ = conn
        self.saved_cards.append(card)

    async def get_cards(
        self, card_ids: List[str], project_id: str | None = None, *, conn: Any = None
    ) -> List[Any]:
        _ = conn
        _ = project_id
        return [self.cards[cid] for cid in card_ids if cid in self.cards]


@dataclass
class _DummyExecutionStore:
    request_depth: int | None = None
    inserted_edges: List[Dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        if self.inserted_edges is None:
            self.inserted_edges = []

    async def get_request_recursion_depth(self, *, project_id: str, correlation_id: str) -> int | None:
        _ = project_id
        _ = correlation_id
        return self.request_depth

    async def insert_execution_edge(self, **kwargs: Any) -> None:
        self.inserted_edges.append(dict(kwargs))
        depth = kwargs.get("recursion_depth")
        if depth is not None:
            self.request_depth = int(depth)


class _CapturePublisher:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    async def __call__(self, parts, headers: Dict[str, str], payload: Dict[str, Any], source_agent_id: str) -> None:
        self.calls.append(
            {
                "parts": parts,
                "headers": dict(headers),
                "payload": dict(payload),
                "source_agent_id": source_agent_id,
            }
        )


class _IdempLeader:
    async def execute(self, key: str, leader_fn):  # noqa: ANN001
        payload = await leader_fn()
        return IdempotencyOutcome(is_leader=True, payload=payload)


class _IdempTimeout:
    async def execute(self, key: str, leader_fn):  # noqa: ANN001
        raise TimeoutError(f"timeout for {key}")


def _valid_data() -> Dict[str, Any]:
    return {
        "tool_call_id": "call_1",
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "agent_id": "agent_1",
        "tool_name": "demo",
        "tool_call_card_id": "tc_1",
        "after_execution": "suspend",
    }


@pytest.mark.asyncio
async def test_tool_runner_validation_failure_publishes_failed_result() -> None:
    publish_capture = _CapturePublisher()
    cardbox = _DummyCardBox(saved_cards=[], cards={})
    runner = ToolRunner(
        target="demo",
        source_agent_id="tool.demo",
        nats=_DummyNats(),
        cardbox=cardbox,
        execution_store=_DummyExecutionStore(),
        idempotent_executor=_IdempLeader(),
        payload_validation=ToolPayloadValidation(require_after_execution=True),
        result_author_id="tool.demo",
        publish_result=publish_capture,
    )

    data = _valid_data()
    data.pop("after_execution")

    async def _execute(_: ToolCallContext):
        raise AssertionError("execute should not run on validation failure")

    result = await runner.run(
        subject="cg.v1r3.proj_1.public.cmd.tool.demo.execute",
        data=data,
        headers={},
        execute=_execute,
    )

    assert result.handled is True
    assert len(cardbox.saved_cards) == 1
    assert len(publish_capture.calls) == 1
    published = publish_capture.calls[0]["payload"]
    assert published["status"] == "failed"
    assert published["tool_call_id"] == "call_1"


@pytest.mark.asyncio
async def test_tool_runner_validation_failure_without_min_fields_raises() -> None:
    publish_capture = _CapturePublisher()
    cardbox = _DummyCardBox(saved_cards=[], cards={})
    runner = ToolRunner(
        target="demo",
        source_agent_id="tool.demo",
        nats=_DummyNats(),
        cardbox=cardbox,
        execution_store=_DummyExecutionStore(),
        idempotent_executor=_IdempLeader(),
        payload_validation=ToolPayloadValidation(require_after_execution=True),
        result_author_id="tool.demo",
        publish_result=publish_capture,
    )

    bad_data = {
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "agent_id": "agent_1",
    }

    async def _execute(_: ToolCallContext):
        raise AssertionError("execute should not run on validation failure")

    with pytest.raises(ValidationError):
        await runner.run(
            subject="cg.v1r3.proj_1.public.cmd.tool.demo.execute",
            data=bad_data,
            headers={},
            execute=_execute,
        )

    assert cardbox.saved_cards == []
    assert publish_capture.calls == []


@pytest.mark.asyncio
async def test_tool_runner_execute_timeout_is_raised() -> None:
    runner = ToolRunner(
        target="demo",
        source_agent_id="tool.demo",
        nats=_DummyNats(),
        cardbox=_DummyCardBox(saved_cards=[], cards={}),
        execution_store=_DummyExecutionStore(),
        idempotent_executor=_IdempTimeout(),
        payload_validation=ToolPayloadValidation(),
        result_author_id="tool.demo",
        publish_result=_CapturePublisher(),
    )

    async def _execute(_: ToolCallContext):
        return {"ok": True}, {"card": "ignored"}

    with pytest.raises(TimeoutError):
        await runner.run(
            subject="cg.v1r3.proj_1.public.cmd.tool.demo.execute",
            data=_valid_data(),
            headers={},
            execute=_execute,
        )


@pytest.mark.asyncio
async def test_tool_runner_success_saves_card_and_publishes() -> None:
    publish_capture = _CapturePublisher()
    cardbox = _DummyCardBox(saved_cards=[], cards={})
    runner = ToolRunner(
        target="demo",
        source_agent_id="tool.demo",
        nats=_DummyNats(),
        cardbox=cardbox,
        execution_store=_DummyExecutionStore(),
        idempotent_executor=_IdempLeader(),
        payload_validation=ToolPayloadValidation(),
        result_author_id="tool.demo",
        publish_result=publish_capture,
    )

    expected_payload = {
        "tool_call_id": "call_1",
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "after_execution": "suspend",
        "status": "success",
        "tool_result_card_id": "card_1",
        "agent_id": "agent_1",
        "step_id": "step_1",
    }
    expected_card = {"card_id": "card_1"}

    async def _execute(_: ToolCallContext):
        return expected_payload, expected_card

    result = await runner.run(
        subject="cg.v1r3.proj_1.public.cmd.tool.demo.execute",
        data=_valid_data(),
        headers={},
        execute=_execute,
    )

    assert result.handled is True
    assert result.is_leader is True
    assert result.payload == expected_payload
    assert cardbox.saved_cards == [expected_card]
    assert len(publish_capture.calls) == 1
    assert publish_capture.calls[0]["payload"] == expected_payload


@pytest.mark.asyncio
async def test_tool_runner_backfills_missing_request_edge_before_publish() -> None:
    publish_capture = _CapturePublisher()
    cardbox = _DummyCardBox(saved_cards=[], cards={})
    execution_store = _DummyExecutionStore(request_depth=None)
    runner = ToolRunner(
        target="demo",
        source_agent_id="tool.demo",
        nats=_DummyNats(),
        cardbox=cardbox,
        execution_store=execution_store,
        idempotent_executor=_IdempLeader(),
        payload_validation=ToolPayloadValidation(),
        result_author_id="tool.demo",
        publish_result=publish_capture,
    )

    payload = {
        "tool_call_id": "call_1",
        "agent_turn_id": "turn_1",
        "turn_epoch": 1,
        "after_execution": "suspend",
        "status": "success",
        "tool_result_card_id": "card_1",
        "agent_id": "agent_1",
        "step_id": "step_1",
    }

    async def _execute(_: ToolCallContext):
        return payload, {"card_id": "card_1"}

    result = await runner.run(
        subject="cg.v1r3.proj_1.public.cmd.tool.demo.execute",
        data=_valid_data(),
        headers={"CG-Recursion-Depth": "3"},
        execute=_execute,
    )

    assert result.handled is True
    assert len(execution_store.inserted_edges) == 1
    edge = execution_store.inserted_edges[0]
    assert edge["primitive"] == "tool_call"
    assert edge["edge_phase"] == "request"
    assert edge["correlation_id"] == "call_1"
    assert edge["recursion_depth"] == 3
