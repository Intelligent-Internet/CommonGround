import asyncio
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from core.utp_protocol import Card, TextContent
from services.api.ground_control_models import BatchCardsRequest
from services.api.main import batch_get_cards


class FakeCardbox:
    def __init__(self, cards):
        self._cards = list(cards)

    async def get_cards_batch(self, card_ids, project_id=None, *, conn=None):
        _ = conn
        wanted = set(card_ids)
        return [card for card in self._cards if card.card_id in wanted]


def _make_card(card_id: str, project_id: str = "proj") -> Card:
    return Card(
        card_id=card_id,
        project_id=project_id,
        type="task.instruction",
        content=TextContent(text="hi"),
        created_at=datetime.now(timezone.utc),
        author_id="user",
        metadata={},
    )


def test_batch_get_cards_dedup_and_missing():
    cardbox = FakeCardbox([_make_card("c1"), _make_card("c2")])
    payload = BatchCardsRequest(card_ids=["c1", "c1", "c2", "missing", "missing"])
    resp = asyncio.run(batch_get_cards("proj", payload, cardbox))

    found_ids = {card.card_id for card in resp.cards}
    assert found_ids == {"c1", "c2"}
    assert resp.missing_ids == ["missing"]


def test_batch_get_cards_empty_list():
    cardbox = FakeCardbox([])
    payload = BatchCardsRequest(card_ids=[])
    resp = asyncio.run(batch_get_cards("proj", payload, cardbox))
    assert resp.cards == []
    assert resp.missing_ids == []


def test_batch_get_cards_limit_exceeded():
    cardbox = FakeCardbox([])
    payload = BatchCardsRequest(card_ids=[f"c{i}" for i in range(101)])
    with pytest.raises(HTTPException) as exc:
        asyncio.run(batch_get_cards("proj", payload, cardbox))
    assert exc.value.status_code == 400


def test_batch_get_cards_boundary_100():
    cards = [_make_card(f"c{i}") for i in range(100)]
    cardbox = FakeCardbox(cards)
    payload = BatchCardsRequest(card_ids=[f"c{i}" for i in range(100)])
    resp = asyncio.run(batch_get_cards("proj", payload, cardbox))
    found_ids = {card.card_id for card in resp.cards}
    assert found_ids == {f"c{i}" for i in range(100)}
    assert resp.missing_ids == []
