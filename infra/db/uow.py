from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from psycopg_pool import AsyncConnectionPool


@dataclass(frozen=True)
class DbTransaction:
    conn: Any


class UnitOfWork:
    """Thin unit-of-work wrapper over a shared AsyncConnectionPool."""

    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[DbTransaction]:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                yield DbTransaction(conn=conn)
