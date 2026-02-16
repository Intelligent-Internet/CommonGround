from __future__ import annotations

from datetime import UTC, datetime
from typing import Optional


def ensure_utc(value: datetime) -> datetime:
    """Normalize datetime to UTC.

    NOTE: Naive datetimes are assumed to already be UTC.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return ensure_utc(value).isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    return to_iso(utc_now()) or ""

