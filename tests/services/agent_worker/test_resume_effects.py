from types import SimpleNamespace

import pytest

from core.cg_context import CGContext
from core.status import STATUS_FAILED
from services.agent_worker.resume_effects import (
    collect_output_box_tool_call_ids,
    ensure_output_box_and_append_tool_result,
    ensure_terminated_deliverable,
)


class _DummyStateStore:
    def __init__(self):
        self.update_calls = []

    async def update(self, **kwargs):
        self.update_calls.append(kwargs)
        return True


class _DummyCardBox:
    def __init__(self):
        self.ensure_value = "box_new"
        self.append_calls = []
        self.saved_cards = []
        self.box = None
        self.cards = []

    async def ensure_box_id(self, *, project_id, box_id, conn=None):
        _ = conn
        return self.ensure_value

    async def append_to_box(self, box_id, card_ids, *, project_id, conn=None):
        _ = conn
        self.append_calls.append((box_id, list(card_ids), project_id))
        return box_id

    async def get_box(self, box_id, *, project_id, conn=None):
        _ = conn
        return self.box

    async def get_cards(self, card_ids, *, project_id, conn=None):
        _ = conn
        return list(self.cards)

    async def save_card(self, card, conn=None):
        _ = conn
        self.saved_cards.append(card)


@pytest.mark.asyncio
async def test_ensure_output_box_and_append_tool_result_updates_state_on_box_change() -> None:
    cardbox = _DummyCardBox()
    state_store = _DummyStateStore()
    output_box_id = await ensure_output_box_and_append_tool_result(
        cardbox=cardbox,
        state_store=state_store,
        project_id="proj_1",
        agent_id="agent_1",
        expect_turn_epoch=3,
        expect_agent_turn_id="turn_1",
        expect_status="running",
        current_output_box_id="box_old",
        tool_result_card_id="tr_1",
    )
    assert output_box_id == "box_new"
    assert len(state_store.update_calls) == 1
    assert state_store.update_calls[0]["output_box_id"] == "box_new"
    assert cardbox.append_calls == [("box_new", ["tr_1"], "proj_1")]


@pytest.mark.asyncio
async def test_collect_output_box_tool_call_ids_filters_by_type_call_and_step() -> None:
    cardbox = _DummyCardBox()
    cardbox.box = SimpleNamespace(card_ids=["1", "2", "3", "4"])
    cardbox.cards = [
        SimpleNamespace(type="tool.result", tool_call_id="call_1", metadata={"step_id": "step_a"}),
        SimpleNamespace(type="tool.result", tool_call_id="call_x", metadata={"step_id": "step_a"}),
        SimpleNamespace(type="task.deliverable", tool_call_id="call_2", metadata={"step_id": "step_a"}),
        SimpleNamespace(type="tool.result", tool_call_id="call_2", metadata={"step_id": "step_b"}),
    ]
    actual_ids = await collect_output_box_tool_call_ids(
        cardbox=cardbox,
        project_id="proj_1",
        output_box_id="box_1",
        expected_ids=["call_1", "call_2"],
        dispatch_step_id="step_a",
    )
    assert actual_ids == {"call_1"}


@pytest.mark.asyncio
async def test_ensure_terminated_deliverable_reuses_existing_card() -> None:
    cardbox = _DummyCardBox()
    cardbox.box = SimpleNamespace(card_ids=["c1"])
    cardbox.cards = [SimpleNamespace(type="task.deliverable", card_id="deliver_1")]
    ctx = CGContext(
        project_id="proj_1",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        step_id="step_1",
    )
    output_box_id, deliverable_card_id, created_card_id = await ensure_terminated_deliverable(
        cardbox=cardbox,
        ctx=ctx,
        output_box_id="box_1",
        result_payload={"ok": True},
        tool_call_id="call_1",
        resume_status="success",
        task_status="success",
    )
    assert output_box_id == "box_1"
    assert deliverable_card_id == "deliver_1"
    assert created_card_id is None
    assert cardbox.saved_cards == []


@pytest.mark.asyncio
async def test_ensure_terminated_deliverable_creates_new_card_when_missing() -> None:
    cardbox = _DummyCardBox()
    cardbox.box = SimpleNamespace(card_ids=["c1"])
    cardbox.cards = [SimpleNamespace(type="tool.result", card_id="r1")]
    ctx = CGContext(
        project_id="proj_1",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        trace_id="trace_1",
        step_id="step_1",
        parent_step_id="parent_1",
    )
    output_box_id, deliverable_card_id, created_card_id = await ensure_terminated_deliverable(
        cardbox=cardbox,
        ctx=ctx,
        output_box_id="box_1",
        result_payload={"error": {"message": "boom"}},
        tool_call_id="call_1",
        resume_status="failed",
        task_status=STATUS_FAILED,
    )
    assert output_box_id == "box_1"
    assert deliverable_card_id is not None
    assert created_card_id == deliverable_card_id
    assert len(cardbox.saved_cards) == 1
    saved = cardbox.saved_cards[0]
    assert saved.type == "task.deliverable"
    assert saved.metadata["partial"] is True
    payload = getattr(saved.content, "data", {})
    assert isinstance(payload, dict)
    assert payload.get("kind") == "fallback_deliverable"
    assert payload.get("partial") is True
    diagnostics = payload.get("diagnostics") or {}
    assert diagnostics.get("tool_call_id") == "call_1"
    assert diagnostics.get("resume_status") == "failed"
    assert cardbox.append_calls[-1][1] == [deliverable_card_id]
