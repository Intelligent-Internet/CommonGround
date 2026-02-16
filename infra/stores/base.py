from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional, Sequence, TypeVar

from psycopg_pool import AsyncConnectionPool

from .pool import create_pool

T = TypeVar("T")


class BaseStore:
    def __init__(
        self,
        dsn: Optional[str] = None,
        *,
        pool: Optional[AsyncConnectionPool] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> None:
        if pool is not None:
            self.pool = pool
            self._owns_pool = False
            return
        if not dsn:
            raise ValueError("dsn is required when pool is not provided")
        self.pool = create_pool(str(dsn), min_size=min_size, max_size=max_size)
        self._owns_pool = True

    async def open(self) -> None:
        if self._owns_pool:
            await self.pool.open()

    async def close(self, *, timeout: Optional[float] = None) -> None:
        if not self._owns_pool:
            return
        if timeout is None:
            await self.pool.close()
        else:
            await self.pool.close(timeout=timeout)

    async def _run(self, fn: Callable[[Any], Awaitable[T]], *, conn: Any = None) -> T:
        if conn is not None:
            return await fn(conn)
        async with self.pool.connection() as own_conn:
            return await fn(own_conn)

    async def fetch_one(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        conn: Any = None,
    ) -> Any:
        query_params = tuple(params or ())

        async def _execute(c: Any) -> Any:
            res = await c.execute(sql, query_params)
            return await res.fetchone()

        return await self._run(_execute, conn=conn)

    async def fetch_all(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        conn: Any = None,
    ) -> list[Any]:
        query_params = tuple(params or ())

        async def _execute(c: Any) -> list[Any]:
            res = await c.execute(sql, query_params)
            return await res.fetchall()

        return await self._run(_execute, conn=conn)
