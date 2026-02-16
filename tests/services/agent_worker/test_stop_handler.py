from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest

from core.status import STATUS_FAILED
from services.agent_worker.models import AgentTurnWorkItem
from services.agent_worker.stop_handler import StopHandler

class _DummyStateStore:
    def __init__(self, state=None):
        self.state = state
        self.finish_calls = []

    async def fetch(self, project_id, agent_id):
        return self.state

    async def finish_turn_idle(self, **kwargs):
        self.finish_calls.append(kwargs)
        return True

    def transaction(self):
        class Tx:
            conn = "conn"
            async def __aenter__(self):
                return self
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        return Tx()

class _DummyCardBox:
    def __init__(self):
        self.ensure_value = "box_1"
        self.append_calls = []
        self.saved_cards = []
        self.box = None
        self.cards = []

    async def ensure_box_id(self, *, project_id, box_id, conn=None):
        return self.ensure_value

    async def append_to_box(self, box_id, card_ids, *, project_id, conn=None):
        self.append_calls.append((box_id, list(card_ids), project_id))
        return box_id

    async def get_box(self, box_id, *, project_id, conn=None):
        return self.box

    async def get_cards(self, card_ids, *, project_id, conn=None):
        return list(self.cards)

    async def save_card(self, card, conn=None):
        self.saved_cards.append(card)

class _DummyNATS:
    def __init__(self):
        self.published = []

    async def publish(self, subject, payload, headers=None):
        self.published.append((subject, payload, headers))

    async def publish_event(self, subject, payload, headers=None):
        self.published.append((subject, payload, headers))


class _DummyStepStore:
    def __init__(self):
        self.insert_calls = []
        self.update_calls = []

    async def insert_step(self, **kwargs):
        self.insert_calls.append(kwargs)

    async def update_step(self, **kwargs):
        self.update_calls.append(kwargs)

@pytest.mark.asyncio
async def test_stop_handler_creates_fallback_deliverable_when_missing(monkeypatch) -> None:
    cardbox = _DummyCardBox()
    cardbox.box = SimpleNamespace(card_ids=["c1", "c2"])
    cardbox.cards = [
        SimpleNamespace(type="agent.thought", content=SimpleNamespace(text="last thought dummy")),
        SimpleNamespace(type="tool.result"),
    ]
    state_store = _DummyStateStore(
        state=SimpleNamespace(
            active_agent_turn_id="turn_1",
            turn_epoch=2,
            output_box_id="box_1",
            parent_step_id="step_1",
            trace_id="trace_1",
            status="running", # required by TurnGuard
        )
    )
    nats = _DummyNATS()

    import services.agent_worker.stop_handler as stop_handler
    async def _noop(*args, **kwargs):
        pass
    monkeypatch.setattr(stop_handler, "emit_agent_state", _noop)
    monkeypatch.setattr(stop_handler, "emit_agent_task", _noop)
    monkeypatch.setattr(stop_handler, "publish_idle_wakeup", _noop)

    handler = StopHandler(
        state_store=state_store,
        resource_store=SimpleNamespace(),
        cardbox=cardbox,
        nats=nats,
        step_store=_DummyStepStore(),
    )
    item = AgentTurnWorkItem(
        project_id="proj_1",
        channel_id="ch_1",
        agent_id="agent_1",
        agent_turn_id="turn_1",
        turn_epoch=2,
        output_box_id="box_1",
        profile_box_id="profile_1",
        context_box_id="ctx_1",
        headers={},
        stop_data={"reason": "user_cancelled"}
    )
    
    await handler.handle(item)

    assert len(cardbox.saved_cards) == 2  
    
    deliverable = [c for c in cardbox.saved_cards if c.type == "task.deliverable"][0]
    error_card = [c for c in cardbox.saved_cards if c.type == "agent.error"][0]
    
    assert deliverable.metadata["stop_reason"] == "user_cancelled"
    
    payload = getattr(deliverable.content, "data", {})
    assert payload.get("kind") == "fallback_deliverable"
    assert payload.get("partial") is True
    
    diag = payload.get("diagnostics") or {}
    assert diag.get("tool_result_count") == 1
    assert "last thought dummy" in diag.get("last_agent_thought", "")
    assert diag.get("source") == "agent_worker.stop_handler"

    assert getattr(error_card.content, "data", {})["error"] == "user_cancelled"
