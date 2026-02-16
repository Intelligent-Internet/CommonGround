from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from infra.db import DbTransaction, UnitOfWork


@asynccontextmanager
async def state_store_transaction(state_store: Any) -> AsyncIterator[DbTransaction]:
    """Use DB transaction when store exposes `pool`; otherwise provide no-op tx."""
    pool = getattr(state_store, "pool", None)
    if pool is None:
        yield DbTransaction(conn=None)
        return
    uow = UnitOfWork(pool)
    async with uow.transaction() as tx:
        yield tx


async def call_with_optional_conn(func: Any, *args: Any, conn: Any = None, **kwargs: Any) -> Any:
    """Call async function and pass conn only when available."""
    if conn is not None:
        kwargs["conn"] = conn
    return await func(*args, **kwargs)
