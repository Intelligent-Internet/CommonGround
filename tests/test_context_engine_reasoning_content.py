from __future__ import annotations

import pytest

from card_box_core import Card, CardBox, ContextEngine, TextContent


class _MemoryStorage:
    def __init__(self) -> None:
        self._cards = {}

    def add_card(self, card, tenant_id):
        self._cards[(tenant_id, card.card_id)] = card

    def get_card(self, card_id, tenant_id):
        return self._cards.get((tenant_id, card_id))

    def add_sync_task(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_to_api_replays_reasoning_content_from_card_metadata() -> None:
    storage = _MemoryStorage()
    engine = ContextEngine(
        trace_id="trace_1",
        tenant_id="tenant_1",
        storage_adapter=storage,
        history_level="none",
    )

    thought_card = Card(
        card_id="assistant_thought_1",
        content=TextContent(text="normal content"),
        metadata={"role": "assistant", "reasoning_content": "previous reasoning"},
    )
    engine.card_store.add(thought_card)

    box = CardBox()
    box.add(thought_card.card_id)

    api_request, _ = await engine.to_api(box)

    messages = api_request.get("messages") or []
    assert len(messages) == 1
    assert messages[0].get("role") == "assistant"
    assert messages[0].get("content") == "normal content"
    assert messages[0].get("reasoning_content") == "previous reasoning"
