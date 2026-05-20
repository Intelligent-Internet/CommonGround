from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from threading import Lock
from typing import Any, ContextManager, Protocol

import psycopg
from psycopg_pool import ConnectionPool


class SyncPostgresConnectionProvider(Protocol):
    def connection(self) -> ContextManager[Any]:
        ...


ConnectionFactory = Callable[[], Any]


class PostgresConnectionPool:
    """Small sync psycopg3 pool wrapper owned by the service app lifespan."""

    def __init__(
        self,
        pg_dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        timeout: float = 30.0,
    ) -> None:
        if not pg_dsn:
            raise ValueError("pg_dsn is required")
        self._pg_dsn = pg_dsn
        self._min_size = min_size
        self._max_size = max_size
        self._timeout = timeout
        self._pool = self._new_pool()
        self._lock = Lock()
        self._opened = False

    @property
    def pg_dsn(self) -> str:
        return self._pg_dsn

    def open(self) -> None:
        with self._lock:
            if self._opened:
                return
            try:
                self._pool.open(wait=True, timeout=self._timeout)
            except Exception:
                self._pool.close()
                self._pool = self._new_pool()
                raise
            else:
                self._opened = True

    def close(self) -> None:
        with self._lock:
            if not self._opened:
                return
            self._pool.close()
            self._pool = self._new_pool()
            self._opened = False

    @contextmanager
    def connection(self):
        self.open()
        with self._pool.connection() as conn:
            yield conn

    def _new_pool(self) -> ConnectionPool:
        return ConnectionPool(
            conninfo=self._pg_dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            timeout=self._timeout,
            open=False,
        )

def postgres_connection(
    pg_dsn: str | None,
    *,
    connection_provider: SyncPostgresConnectionProvider | None = None,
    connection_factory: ConnectionFactory | None = None,
):
    if connection_provider is not None:
        return connection_provider.connection()
    if connection_factory is not None:
        return connection_factory()
    if not pg_dsn:
        raise ValueError("pg_dsn is required")
    return psycopg.connect(pg_dsn)


__all__ = [
    "ConnectionFactory",
    "PostgresConnectionPool",
    "SyncPostgresConnectionProvider",
    "postgres_connection",
]
