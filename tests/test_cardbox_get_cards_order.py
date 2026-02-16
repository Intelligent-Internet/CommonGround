from __future__ import annotations

from typing import Optional

import pytest

from infra.cardbox_client import CardBoxClient


class _DummyCard:
    def __init__(self, card_id: str) -> None:
        self.card_id = card_id


class _DummyClient(CardBoxClient):
    def __init__(self) -> None:  # noqa: D401 - test stub
        # Skip CardBoxClient.__init__ (no DB / no card_box_core configure).
        self.tenant_id = "public"
        self._last_batch_ids: Optional[list[str]] = None
        self._last_conn = None

    async def get_cards_batch(self, card_ids, project_id=None, *, conn=None):  # type: ignore[override]
        self._last_batch_ids = list(card_ids)
        self._last_conn = conn
        # Return out-of-order on purpose to validate re-ordering.
        out = []
        if "c" in card_ids:
            out.append(_DummyCard("c"))
        if "a" in card_ids:
            out.append(_DummyCard("a"))
        if "b" in card_ids:
            out.append(_DummyCard("b"))
        return out


@pytest.mark.asyncio
async def test_get_cards_preserves_input_order_and_duplicates() -> None:
    client = _DummyClient()
    cards = await client.get_cards(["a", "b", "a", "c"], project_id="proj_x", conn="tx_conn")
    assert [c.card_id for c in cards] == ["a", "b", "a", "c"]
    assert client._last_batch_ids == ["a", "b", "c"]
    assert client._last_conn == "tx_conn"


@pytest.mark.asyncio
async def test_get_cards_skips_missing_ids() -> None:
    client = _DummyClient()
    cards = await client.get_cards(["missing", "a", "missing2", "c"], project_id="proj_x")
    assert [c.card_id for c in cards] == ["a", "c"]
    assert client._last_batch_ids == ["missing", "a", "missing2", "c"]
