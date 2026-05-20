from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence


class CardContentLike(Protocol):
    pass


class CardLike(Protocol):
    content: CardContentLike
    metadata: Mapping[str, Any]


class CardBoxLike(Protocol):
    card_ids: Sequence[str]
