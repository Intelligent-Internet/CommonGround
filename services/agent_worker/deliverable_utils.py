from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable, Dict, Optional

import uuid6

from core.status import STATUS_FAILED, STATUS_SUCCESS
from core.utp_protocol import Card, JsonContent, extract_card_content_text

from .tx_utils import call_with_optional_conn


async def ensure_terminal_deliverable(
    *,
    cardbox: Any,
    item: Any,
    step_id: str,
    terminal_status: str,
    reason: str,
    source: str = "agent_worker.react_step.ensure_terminal_deliverable",
    card_metadata_builder: Callable[..., Dict[str, Any]],
    logger: Any,
    conn: Any = None,
) -> Optional[str]:
    """Best-effort: ensure output_box contains a task.deliverable and return its card_id."""
    if not item.output_box_id:
        return None

    try:
        box = await call_with_optional_conn(
            cardbox.get_box,
            str(item.output_box_id),
            project_id=item.project_id,
            conn=conn,
        )
        if not box:
            return None

        card_ids = list(box.card_ids or [])
        cards = (
            await call_with_optional_conn(
                cardbox.get_cards,
                card_ids,
                project_id=item.project_id,
                conn=conn,
            )
            if card_ids
            else []
        )

        for c in reversed(cards):
            if getattr(c, "type", "") == "task.deliverable":
                return getattr(c, "card_id", None)

        last_thought = ""
        tool_results = 0
        for c in reversed(cards):
            if getattr(c, "type", "") == "tool.result":
                tool_results += 1
            if not last_thought and getattr(c, "type", "") == "agent.thought":
                thought_text = extract_card_content_text(c).strip()
                if thought_text:
                    last_thought = thought_text

        from core.deliverable_fallback import build_fallback_deliverable_data

        is_partial = terminal_status != STATUS_SUCCESS
        fallback_data = build_fallback_deliverable_data(
            reason=reason,
            source=source,
            last_thought=last_thought,
            tool_result_count=tool_results,
            terminal_status=terminal_status,
        )

        deliverable_card_id = uuid6.uuid7().hex
        deliverable = Card(
            card_id=deliverable_card_id,
            project_id=item.project_id,
            type="task.deliverable",
            content=JsonContent(data=fallback_data),
            created_at=datetime.now(UTC),
            author_id=item.agent_id,
            metadata=card_metadata_builder(
                item,
                step_id=step_id,
                role="assistant",
                extra={"partial": is_partial, "terminal_reason": reason},
            ),
        )
        await call_with_optional_conn(cardbox.save_card, deliverable, conn=conn)
        item.output_box_id = await call_with_optional_conn(
            cardbox.append_to_box,
            str(item.output_box_id),
            [deliverable.card_id],
            project_id=item.project_id,
            conn=conn,
        )
        return deliverable.card_id
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to ensure terminal deliverable: %s", exc)
        return None


async def append_partial_deliverable(
    *,
    cardbox: Any,
    item: Any,
    step_id: str,
    reason: str,
    card_metadata_builder: Callable[..., Dict[str, Any]],
    logger: Any,
) -> Optional[str]:
    """Best-effort deliverable to help parent orchestrators continue even on failed turns."""
    if not item.output_box_id:
        return None
    try:
        box = await cardbox.get_box(item.output_box_id, project_id=item.project_id)
        if not box or not box.card_ids:
            return None
        cards = await cardbox.get_cards(list(box.card_ids), project_id=item.project_id)
        if not cards:
            return None

        # Avoid duplicating if a deliverable already exists.
        for c in cards:
            if getattr(c, "type", "") == "task.deliverable":
                return getattr(c, "card_id", None)

        last_thought = ""
        tool_results = 0
        for c in reversed(cards):
            if getattr(c, "type", "") == "tool.result":
                tool_results += 1
            if not last_thought and getattr(c, "type", "") == "agent.thought":
                thought_text = extract_card_content_text(c).strip()
                if thought_text:
                    last_thought = thought_text

        from core.deliverable_fallback import build_fallback_deliverable_data

        fallback_data = build_fallback_deliverable_data(
            reason=reason,
            source="agent_worker.react_step.append_partial_deliverable",
            last_thought=last_thought,
            tool_result_count=tool_results,
            terminal_status=STATUS_FAILED,
        )

        deliverable_card_id = uuid6.uuid7().hex
        deliverable = Card(
            card_id=deliverable_card_id,
            project_id=item.project_id,
            type="task.deliverable",
            content=JsonContent(data=fallback_data),
            created_at=datetime.now(UTC),
            author_id=item.agent_id,
            metadata=card_metadata_builder(item, step_id=step_id, role="assistant", extra={"partial": True}),
        )
        await cardbox.save_card(deliverable)
        item.output_box_id = await cardbox.append_to_box(
            item.output_box_id, [deliverable.card_id], project_id=item.project_id
        )
        return deliverable_card_id
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to append partial deliverable: %s", exc)
        return None
