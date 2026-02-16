import argparse
import asyncio
import os
import sys

from psycopg_pool import AsyncConnectionPool

from core.utils import set_loop_policy
from scripts.utils.config import load_toml_config, resolve_cardbox_dsn

set_loop_policy()


def _load_dsn() -> str:
    cfg = load_toml_config()
    dsn, source = resolve_cardbox_dsn(cfg)
    if source == "env":
        print("Using DSN from Environment (PG_DSN)")
        return dsn
    if source == "missing":
        print("❌ config.toml not found! Set PG_DSN or create config.toml")
        sys.exit(1)
    if not dsn:
        print("❌ No DSN found. Set PG_DSN or configure [cardbox].postgres_dsn in config.toml")
        sys.exit(1)
    print("Using DSN from config.toml")
    return dsn


async def _cleanup_inbox(*, dsn: str, days: int, limit: int, execute: bool) -> None:
    pool = AsyncConnectionPool(dsn, open=False)
    await pool.open()
    async with pool.connection() as conn:
        count_sql = """
            SELECT COUNT(*) AS cnt
            FROM state.agent_inbox
            WHERE archived_at IS NOT NULL
              AND archived_at < NOW() - make_interval(days => %s)
        """
        res = await conn.execute(count_sql, (int(days),))
        row = await res.fetchone()
        total = int(row["cnt"]) if row and row.get("cnt") is not None else 0
        print(f"Eligible archived rows: {total} (older than {days}d)")
        if not execute:
            await pool.close()
            return

        delete_sql = """
            WITH doomed AS (
                SELECT inbox_id
                FROM state.agent_inbox
                WHERE archived_at IS NOT NULL
                  AND archived_at < NOW() - make_interval(days => %s)
                ORDER BY archived_at ASC
                LIMIT %s
            )
            DELETE FROM state.agent_inbox
            WHERE inbox_id IN (SELECT inbox_id FROM doomed)
            RETURNING inbox_id
        """
        res = await conn.execute(delete_sql, (int(days), int(limit)))
        deleted = res.rowcount
        print(f"Deleted rows: {deleted}")
    await pool.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanup archived inbox rows.")
    parser.add_argument("--days", type=int, default=7, help="Delete rows archived before N days ago.")
    parser.add_argument("--limit", type=int, default=5000, help="Max rows to delete per run.")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete rows (default is dry-run).",
    )
    args = parser.parse_args()

    dsn = _load_dsn()
    await _cleanup_inbox(dsn=dsn, days=max(0, int(args.days)), limit=max(1, int(args.limit)), execute=args.execute)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
