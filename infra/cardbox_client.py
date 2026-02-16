from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from psycopg_pool import AsyncConnectionPool

from card_box_core import Card
from card_box_core.adapters.async_storage import AsyncPostgresStorageAdapter
from card_box_core.config import PostgresStorageAdapterSettings, configure
from card_box_core.structures import CardBox

from core.config_defaults import (
    DEFAULT_CARDBOX_DSN,
    DEFAULT_CARDBOX_MAX_SIZE,
    DEFAULT_CARDBOX_MIN_SIZE,
    DEFAULT_TENANT_ID,
)
from core.utp_protocol import Card as SchemaCard
from core.utp_protocol import from_core_card, to_core_card


class CardBoxClient:
    """Async CardBox client backed by AsyncPostgresStorageAdapter."""

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        pool: AsyncConnectionPool | None = None,
    ) -> None:
        cfg = config or {}
        self.tenant_id = cfg.get("tenant_id", DEFAULT_TENANT_ID)

        pg_conf = {
            "dsn": cfg.get("postgres_dsn", DEFAULT_CARDBOX_DSN),
            "min_size": cfg.get("postgres_min_size", DEFAULT_CARDBOX_MIN_SIZE),
            "max_size": cfg.get("postgres_max_size", DEFAULT_CARDBOX_MAX_SIZE),
            "auto_bootstrap": False,
        }

        # Keep card_box_core runtime settings aligned for call-sites that still read it.
        configure(
            {
                "STORAGE_BACKEND": "postgres",
                "POSTGRES_STORAGE_ADAPTER": pg_conf,
            }
        )

        self.adapter = AsyncPostgresStorageAdapter(
            PostgresStorageAdapterSettings(**pg_conf),
            pool=pool,
        )
        self.pool = self.adapter.pool

    async def init(self) -> None:
        await self.adapter.open()

    async def close(self, *, timeout: float = 30.0) -> None:
        await self.adapter.close(timeout=timeout)

    async def save_card(self, card: SchemaCard, *, conn: Any = None) -> None:
        core_card = to_core_card(card)
        tenant_id = core_card.metadata.get("project_id", self.tenant_id)
        await self.adapter.add_card(core_card, tenant_id, conn=conn)

    async def get_cards(
        self,
        card_ids: List[str],
        project_id: Optional[str] = None,
        *,
        conn: Any = None,
    ) -> List[SchemaCard]:
        """Fetch cards in input order (missing ids skipped)."""
        if not card_ids:
            return []

        normalized: list[str] = []
        for cid in card_ids:
            if cid is None:
                continue
            text = str(cid)
            if not text:
                continue
            normalized.append(text)
        if not normalized:
            return []

        unique_ids = list(dict.fromkeys(normalized))
        fetched = await self.get_cards_batch(unique_ids, project_id=project_id, conn=conn)
        if not fetched:
            return []

        by_id: dict[str, SchemaCard] = {}
        for card in fetched:
            cid = getattr(card, "card_id", None)
            if isinstance(cid, str) and cid:
                by_id[cid] = card

        ordered: list[SchemaCard] = []
        for cid in normalized:
            card = by_id.get(cid)
            if card is not None:
                ordered.append(card)
        return ordered

    async def get_cards_batch(
        self,
        card_ids: List[str],
        project_id: Optional[str] = None,
        *,
        conn: Any = None,
    ) -> List[SchemaCard]:
        """Batch fetch cards; order is not guaranteed."""
        if not card_ids:
            return []

        tenant_id = project_id or self.tenant_id
        core_cards = await self.adapter.list_cards_by_ids(
            tenant_id,
            card_ids=list(card_ids),
            conn=conn,
        )
        return [from_core_card(c, project_id=tenant_id) for c in (core_cards or [])]

    async def get_box(
        self,
        box_id: Union[str, UUID],
        project_id: Optional[str] = None,
        *,
        conn: Any = None,
    ) -> Optional[CardBox]:
        """Load CardBox by box_id, returns None if missing."""
        tenant_id = project_id or self.tenant_id
        return await self.adapter.load_card_box(str(box_id), tenant_id, conn=conn)

    async def get_boxes_batch(
        self,
        box_ids: List[Union[str, UUID]],
        project_id: Optional[str] = None,
        *,
        conn: Any = None,
    ) -> List[Optional[CardBox]]:
        """Batch fetch card boxes in input order."""
        if not box_ids:
            return []
        if conn is not None:
            return [
                await self.get_box(box_id, project_id=project_id, conn=conn)
                for box_id in box_ids
            ]

        return await asyncio.gather(
            *[self.get_box(box_id, project_id=project_id) for box_id in box_ids]
        )

    async def save_box(
        self,
        card_ids: List[str],
        project_id: Optional[str] = None,
        box_id: Optional[Union[str, UUID]] = None,
        *,
        conn: Any = None,
    ) -> str:
        """Create or update a CardBox, returning box_id."""
        tenant_id = project_id or self.tenant_id
        box = CardBox(box_id=str(box_id) if box_id else None)
        box.card_ids = list(card_ids)
        return await self.adapter.save_card_box(box, tenant_id, conn=conn)

    async def ensure_box_id(
        self,
        *,
        project_id: Optional[str] = None,
        box_id: Optional[Union[str, UUID]] = None,
        conn: Any = None,
    ) -> str:
        """Ensure a CardBox exists and return its box_id."""
        if box_id:
            box = await self.get_box(box_id, project_id=project_id, conn=conn)
            if box:
                return str(box_id)
        return str(await self.save_box([], project_id=project_id, conn=conn))

    async def append_to_box(
        self,
        box_id: Union[str, UUID],
        card_ids: List[str],
        project_id: Optional[str] = None,
        *,
        conn: Any = None,
    ) -> str:
        """Append cards to a specific box_id, returning the box_id."""
        tenant_id = project_id or self.tenant_id
        return await self.adapter.append_to_box(
            str(box_id),
            tenant_id,
            list(card_ids),
            conn=conn,
        )

    async def get_latest_card_by_step_type(
        self,
        *,
        project_id: str,
        step_id: str,
        card_type: str,
        conn: Any = None,
    ) -> Optional[SchemaCard]:
        """Fetch the most recent card for a given step_id + type."""
        if not step_id or not card_type:
            return None
        tenant_id = project_id or self.tenant_id
        core_cards = await self.adapter.list_cards(
            tenant_id,
            metadata_filters={"step_id": str(step_id), "type": str(card_type)},
            limit=1,
            conn=conn,
        )
        if not core_cards:
            return None
        return from_core_card(core_cards[0], project_id=tenant_id)

    async def get_tool_results_by_step_and_call_ids(
        self,
        *,
        project_id: str,
        step_id: str,
        tool_call_ids: List[str],
        conn: Any = None,
    ) -> List[SchemaCard]:
        """Fetch tool.result cards by dispatch step_id + tool_call_id list."""
        if not step_id or not tool_call_ids:
            return []
        tenant_id = project_id or self.tenant_id
        core_cards = await self.adapter.list_cards_by_tool_call_ids(
            tenant_id,
            tool_call_ids=list(tool_call_ids),
            metadata_filters={"step_id": str(step_id), "type": "tool.result"},
            conn=conn,
        )
        return [from_core_card(c, project_id=tenant_id) for c in core_cards]

    async def get_cards_by_tool_call_ids(
        self,
        *,
        project_id: str,
        tool_call_ids: List[str],
        conn: Any = None,
    ) -> List[SchemaCard]:
        if not tool_call_ids:
            return []
        tenant_id = project_id or self.tenant_id
        core_cards = await self.adapter.list_cards_by_tool_call_ids(
            tenant_id,
            tool_call_ids=list(tool_call_ids),
            conn=conn,
        )
        return [from_core_card(c, project_id=tenant_id) for c in core_cards]

    async def add_core_card(self, card: Card, *, tenant_id: str, conn: Any = None) -> None:
        """Persist a raw card_box_core Card (for internal callers)."""
        await self.adapter.add_card(card, tenant_id, conn=conn)
