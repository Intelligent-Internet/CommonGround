from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime:
        ...


@dataclass(slots=True)
class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@dataclass(slots=True)
class ManualClock:
    current: datetime = field(default_factory=lambda: datetime.now(UTC))

    def now(self) -> datetime:
        return self.current

    def set(self, current: datetime) -> None:
        self.current = current

    def advance(self, *, seconds: int = 0, delta: timedelta | None = None) -> datetime:
        if delta is None:
            delta = timedelta(seconds=seconds)
        self.current = self.current + delta
        return self.current
