from __future__ import annotations

import asyncio
import math
from threading import RLock
from typing import Any, Mapping
from uuid import NAMESPACE_URL, uuid5

from cardbox.adapters.async_storage import AsyncPostgresStorageAdapter
from cardbox.config import PostgresStorageAdapterSettings
from cardbox.structures import Card, CardBox, JsonContent, TextContent

from CommonGround.contracts import CardBoxPort, CardBoxRef, ConflictError, HydratedCardBox


_WRITE_KEY_NAMESPACE = uuid5(NAMESPACE_URL, "commonground-kernel/cardbox-write-key")


class PostgresCardBoxService(CardBoxPort):
    def __init__(self, pg_dsn: str) -> None:
        self._config = PostgresStorageAdapterSettings(dsn=pg_dsn)
        self._lock = RLock()

    def create_payload_box(
        self,
        project_id: str,
        payload: Any,
        *,
        write_key: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> CardBoxRef:
        metadata_dict = {"cg_source": "CommonGround"}
        if metadata is not None:
            metadata_dict.update(dict(metadata))
        return self._run(lambda adapter: self._create_payload_box_async(adapter, project_id, payload, write_key, metadata_dict))

    def box_exists(self, ref: CardBoxRef) -> bool:
        return self._run(lambda adapter: self._box_exists_async(adapter, ref))

    def load_box(self, ref: CardBoxRef) -> CardBox | None:
        return self._run(lambda adapter: adapter.load_card_box(ref.cardbox_id, ref.project_id))

    def hydrate_box(self, ref: CardBoxRef) -> HydratedCardBox | None:
        return self._run(lambda adapter: self._hydrate_box_async(adapter, ref))

    def _run(self, operation):
        with self._lock:
            async def _wrapped():
                adapter = AsyncPostgresStorageAdapter(config=self._config)
                await adapter.open()
                try:
                    return await operation(adapter)
                finally:
                    await adapter.close()

            return asyncio.run(_wrapped())

    async def _create_payload_box_async(
        self,
        adapter: AsyncPostgresStorageAdapter,
        project_id: str,
        payload: Any,
        write_key: str | None,
        metadata: dict[str, Any],
    ) -> CardBoxRef:
        encoded_payload = self._encode_payload(payload)
        if write_key is not None:
            ref = CardBoxRef(project_id=project_id, cardbox_id=str(uuid5(_WRITE_KEY_NAMESPACE, f"{project_id}:{write_key}")))
            existing_box = await adapter.load_card_box(ref.cardbox_id, ref.project_id)
            if existing_box is not None:
                hydrated = await self._hydrate_box_async(adapter, ref)
                if hydrated is None:
                    raise ConflictError(f"cardbox write_key conflict: {write_key}")
                if self._hydrated_matches(hydrated, payload, metadata):
                    return ref
                raise ConflictError(f"cardbox write_key conflict: {write_key}")
            box = CardBox(box_id=ref.cardbox_id)
        else:
            box = CardBox()

        card = Card(content=encoded_payload, metadata=metadata)
        await adapter.add_card(card, project_id)
        box.card_ids = [card.card_id]
        box_id = await adapter.save_card_box(box, project_id)
        return CardBoxRef(project_id=project_id, cardbox_id=str(box_id))

    async def _box_exists_async(self, adapter: AsyncPostgresStorageAdapter, ref: CardBoxRef) -> bool:
        box = await adapter.load_card_box(ref.cardbox_id, ref.project_id)
        return box is not None

    async def _hydrate_box_async(
        self,
        adapter: AsyncPostgresStorageAdapter,
        ref: CardBoxRef,
    ) -> HydratedCardBox | None:
        box = await adapter.load_card_box(ref.cardbox_id, ref.project_id)
        if box is None:
            return None
        cards: list[Card] = []
        for card_id in box.card_ids:
            card = await adapter.get_card(card_id, ref.project_id)
            if card is None:
                return None
            cards.append(card)
        return HydratedCardBox(ref=ref, box=box, cards=tuple(cards))

    @staticmethod
    def _hydrated_matches(
        hydrated: HydratedCardBox,
        payload: Any,
        metadata: Mapping[str, Any],
    ) -> bool:
        try:
            existing_payload = PostgresCardBoxService._extract_single_payload(hydrated)
        except (TypeError, ValueError):
            return False
        return existing_payload == payload and dict(hydrated.cards[0].metadata) == dict(metadata)

    @staticmethod
    def _encode_payload(payload: Any):
        if isinstance(payload, str):
            return TextContent(text=payload)
        if isinstance(payload, float) and not math.isfinite(payload):
            raise TypeError("CommonGround payloads must be standard JSON values")
        if payload is None or isinstance(payload, (dict, list, int, float, bool)):
            return JsonContent(data=payload)
        raise TypeError("CommonGround payloads must be valid JSON values")

    @staticmethod
    def _decode_card_payload(card: Card) -> Any:
        content = card.content
        if isinstance(content, TextContent):
            return content.text
        if isinstance(content, JsonContent):
            return content.data
        raise TypeError(f"unsupported card content for payload extraction: {type(content).__name__}")

    @staticmethod
    def _extract_single_payload(hydrated: HydratedCardBox) -> Any:
        if len(hydrated.cards) != 1:
            raise ValueError("CommonGround payload boxes must contain exactly one card")
        return PostgresCardBoxService._decode_card_payload(hydrated.cards[0])
