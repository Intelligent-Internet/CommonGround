from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Awaitable, Callable, Dict, Iterable, Optional

from core.errors import ProtocolViolationError

from .headers import normalize_headers
from .wakeup import build_wakeup_headers_from_inbox_row


@dataclass(frozen=True)
class InboxRowContext:
    row: Dict[str, Any]
    inbox_id: str
    message_type: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    trace_id: Optional[str]

    inbox_created_at: Any = None
    inbox_processed_at: Any = None
    recursion_depth: int = 0


@dataclass(frozen=True)
class InboxRowResult:
    # If status is None, leave the inbox row in "processing" (caller may consume later).
    status: Optional[str]


async def consume_inbox_rows(
    *,
    nats: Any,
    execution_store: Any,
    project_id: str,
    rows: Iterable[Dict[str, Any]],
    handler: Callable[[InboxRowContext], Awaitable[InboxRowResult]],
    logger: logging.Logger,
    expected_status: str = "processing",
    require_depth: bool = True,
    status_update_hook: Optional[Callable[[Dict[str, Any], str], Awaitable[None]]] = None,
) -> int:
    """Consume already-claimed inbox rows using a shared loop template.

    This helper centralizes:
    - payload type validation
    - wakeup headers building (+ normalization)
    - try/except -> inbox status update
    - consistent update_inbox_status behavior
    """
    processed = 0
    for row in rows or []:
        inbox_id = row.get("inbox_id")
        if not inbox_id:
            continue
        message_type = str(row.get("message_type") or "")
        payload = row.get("payload") or {}
        if not isinstance(payload, dict):
            updated = await execution_store.update_inbox_status(
                inbox_id=inbox_id,
                project_id=project_id,
                status="error",
                expected_status=expected_status,
            )
            if updated and status_update_hook:
                await status_update_hook(row, "error")
            processed += 1
            continue

        wakeup_headers, row_trace_id = build_wakeup_headers_from_inbox_row(row=row, logger=logger)
        try:
            wakeup_headers, normalized_trace_id, _ = normalize_headers(
                nats=nats,
                headers=wakeup_headers,
                trace_id=row_trace_id,
                require_depth=require_depth,
            )
        except ProtocolViolationError as exc:
            logger.warning("Inbox row invalid headers inbox_id=%s: %s", inbox_id, exc)
            updated = await execution_store.update_inbox_status(
                inbox_id=inbox_id,
                project_id=project_id,
                status="error",
                expected_status=expected_status,
            )
            if updated and status_update_hook:
                await status_update_hook(row, "error")
            processed += 1
            continue

        ctx = InboxRowContext(
            row=row,
            inbox_id=str(inbox_id),
            message_type=message_type,
            payload=payload,
            headers=wakeup_headers,
            trace_id=normalized_trace_id,
            inbox_created_at=row.get("created_at"),
            inbox_processed_at=row.get("processed_at"),
            recursion_depth=int(row.get("recursion_depth") or 0),
        )

        try:
            result = await handler(ctx)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Inbox row handler failed inbox_id=%s: %s", inbox_id, exc, exc_info=True)
            result = InboxRowResult(status="error")

        if result.status is not None:
            updated = await execution_store.update_inbox_status(
                inbox_id=inbox_id,
                project_id=project_id,
                status=result.status,
                expected_status=expected_status,
            )
            if updated and status_update_hook:
                await status_update_hook(row, result.status)
        processed += 1

    return processed
