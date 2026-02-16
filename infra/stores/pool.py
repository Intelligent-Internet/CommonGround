from __future__ import annotations

from typing import Any, Dict, Optional

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row


def create_pool(
    dsn: str,
    *,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
) -> AsyncConnectionPool:
    pool_kwargs: Dict[str, Any] = {"open": False, "kwargs": {"row_factory": dict_row}}
    if min_size is not None:
        pool_kwargs["min_size"] = int(min_size)
    if max_size is not None:
        pool_kwargs["max_size"] = int(max_size)
    return AsyncConnectionPool(dsn, **pool_kwargs)
