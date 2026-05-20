from __future__ import annotations

from typing import Any, Callable

from CommonGround.contracts import AgentRef, TurnRef


DEFAULT_FEED_PAGE_LIMIT = 500


def fetch_agent_feed_items_since(
    client,
    agent: AgentRef,
    *,
    after_ledger_seq: int = 0,
    limit: int = DEFAULT_FEED_PAGE_LIMIT,
) -> tuple[tuple[Any, ...], int]:
    return _fetch_feed_items(
        lambda *, after_ledger_seq, limit: client.fetch_agent_feed(
            agent,
            after_ledger_seq=after_ledger_seq,
            limit=limit,
        ),
        after_ledger_seq=after_ledger_seq,
        limit=limit,
    )


def fetch_turn_feed_items(
    client,
    turn: TurnRef,
    *,
    after_ledger_seq: int = 0,
    limit: int = DEFAULT_FEED_PAGE_LIMIT,
) -> tuple[Any, ...]:
    items, _ = _fetch_feed_items(
        lambda *, after_ledger_seq, limit: client.fetch_turn_feed(
            turn,
            after_ledger_seq=after_ledger_seq,
            limit=limit,
        ),
        after_ledger_seq=after_ledger_seq,
        limit=limit,
    )
    return items


def _fetch_feed_items(
    fetch_page: Callable[..., Any],
    *,
    after_ledger_seq: int,
    limit: int,
) -> tuple[tuple[Any, ...], int]:
    items: list[Any] = []
    next_after = after_ledger_seq
    while True:
        page = fetch_page(after_ledger_seq=next_after, limit=limit)
        page_items = tuple(page.items)
        if not page_items:
            break
        items.extend(page_items)
        page_next_after = page.next_after_ledger_seq
        if page_next_after <= next_after:
            break
        next_after = page_next_after
    return tuple(items), next_after
