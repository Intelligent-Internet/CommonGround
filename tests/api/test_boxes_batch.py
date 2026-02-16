import asyncio
from datetime import datetime, timezone

from core.utp_protocol import Card, TextContent
from services.api.ground_control_models import BatchBoxesRequest
from services.api.main import batch_get_boxes


class FakeBox:
    def __init__(self, box_id, card_ids):
        self.box_id = box_id
        self.card_ids = list(card_ids)


class FakeCardbox:
    def __init__(self, boxes, cards):
        self._boxes = {box.box_id: box for box in boxes}
        self._cards = {card.card_id: card for card in cards}

    async def get_boxes_batch(self, box_ids, project_id=None, *, conn=None):
        _ = conn
        return [self._boxes.get(str(bid)) for bid in box_ids]

    async def get_cards_batch(self, card_ids, project_id=None, *, conn=None):
        _ = conn
        return [self._cards[cid] for cid in card_ids if cid in self._cards]


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


def test_batch_get_boxes_no_hydrate():
    box1 = FakeBox("box1", ["c1", "c2", "c3"])
    cardbox = FakeCardbox([box1], [])
    payload = BatchBoxesRequest(box_ids=["box1", "box1", "missing"], hydrate=False)
    resp = asyncio.run(batch_get_boxes("proj", payload, cardbox))

    assert len(resp.boxes) == 1
    assert resp.boxes[0].box_id == "box1"
    assert resp.boxes[0].card_ids == ["c1", "c2", "c3"]
    assert resp.boxes[0].hydrated_cards is None
    assert resp.missing_box_ids == ["missing"]
    assert resp.missing_card_ids == []


def test_batch_get_boxes_hydrate_limit_order_and_missing():
    box1 = FakeBox("box1", ["c1", "c2", "c3", "c4"])
    box2 = FakeBox("box2", ["c3", "missing", "c5"])
    cards = [_make_card("c3"), _make_card("c4"), _make_card("c5")]
    cardbox = FakeCardbox([box1, box2], cards)

    payload = BatchBoxesRequest(box_ids=["box1", "box2"], hydrate=True, limit=2)
    resp = asyncio.run(batch_get_boxes("proj", payload, cardbox))

    boxes_by_id = {box.box_id: box for box in resp.boxes}
    assert boxes_by_id["box1"].card_ids == ["c1", "c2", "c3", "c4"]
    assert [c.card_id for c in boxes_by_id["box1"].hydrated_cards] == ["c3", "c4"]

    assert boxes_by_id["box2"].card_ids == ["c3", "missing", "c5"]
    assert [c.card_id for c in boxes_by_id["box2"].hydrated_cards] == ["c5"]
    assert resp.missing_card_ids == ["missing"]
