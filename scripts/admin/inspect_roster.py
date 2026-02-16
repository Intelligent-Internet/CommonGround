import argparse
import asyncio
import json
from typing import Any, Dict, List, Optional

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

from core.utils import set_loop_policy
from scripts.utils.config import load_toml_config, require_cardbox_dsn

set_loop_policy()


async def _fetch_roster(
    *,
    pool: AsyncConnectionPool,
    project_id: str,
    owner_agent_id: Optional[str],
    worker_target: Optional[str],
    tag: Optional[str],
    batch_id: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT project_id, agent_id, worker_target, tags, owner_agent_id, profile_box_id, display_name, status, metadata, created_at, last_seen_at
        FROM resource.project_agents
        WHERE project_id=%s
    """
    args: List[Any] = [project_id]
    if owner_agent_id is not None:
        sql += " AND owner_agent_id=%s"
        args.append(owner_agent_id)
    if worker_target is not None:
        sql += " AND worker_target=%s"
        args.append(worker_target)
    if tag is not None:
        sql += " AND tags @> %s::jsonb"
        args.append(json.dumps([tag]))
    if batch_id is not None:
        sql += " AND (metadata->>'batch_id')=%s"
        args.append(batch_id)
    sql += " ORDER BY created_at DESC LIMIT %s"
    args.append(int(limit))

    async with pool.connection() as conn:
        res = await conn.execute(sql, args)
        rows = await res.fetchall()
        return rows or []


async def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect roster entries in resource.project_agents.")
    parser.add_argument("--project", required=True, help="project_id")
    parser.add_argument("--owner", default=None, help="filter by owner_agent_id")
    parser.add_argument("--worker-target", dest="worker_target", default=None, help="filter by worker_target")
    parser.add_argument("--tag", default=None, help="filter by tag")
    parser.add_argument("--batch", default=None, help="filter by metadata.batch_id")
    parser.add_argument("--limit", type=int, default=50, help="max rows")
    parser.add_argument("--json", action="store_true", help="print as json")
    args = parser.parse_args()

    cfg = load_toml_config()
    if not cfg:
        raise RuntimeError("config.toml not found")
    dsn = require_cardbox_dsn(cfg, error="cardbox.postgres_dsn missing in config.toml")
    pool = AsyncConnectionPool(dsn, open=False, kwargs={"row_factory": dict_row})
    await pool.open()
    try:
        rows = await _fetch_roster(
            pool=pool,
            project_id=str(args.project),
            owner_agent_id=str(args.owner) if args.owner is not None else None,
            worker_target=str(args.worker_target) if args.worker_target is not None else None,
            tag=str(args.tag) if args.tag is not None else None,
            batch_id=str(args.batch) if args.batch is not None else None,
            limit=int(args.limit),
        )
    finally:
        await pool.close()

    if args.json:
        print(json.dumps(rows, ensure_ascii=False, default=str, indent=2))
        return

    for r in rows:
        created_at = r.get("created_at")
        agent_id = r.get("agent_id")
        worker_target = r.get("worker_target")
        tags = r.get("tags") or []
        owner = r.get("owner_agent_id")
        profile_box_id = r.get("profile_box_id")
        spawned_by = None
        meta = r.get("metadata") or {}
        if isinstance(meta, dict):
            spawned_by = meta.get("spawned_by")
        print(
            f"{created_at} agent={agent_id} worker_target={worker_target} tags={tags} "
            f"owner={owner} profile={profile_box_id} spawned_by={spawned_by}"
        )


if __name__ == "__main__":
    asyncio.run(main())
