import argparse
import asyncio
import os
from pathlib import Path
from typing import Any, Dict

import httpx
from psycopg_pool import AsyncConnectionPool

from core.utils import set_loop_policy, REPO_ROOT
from scripts.utils.config import api_base_url, load_toml_config, resolve_cardbox_dsn

set_loop_policy()


async def _upload_profile_yaml(*, api_url: str, project_id: str, path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"profile yaml not found: {path}")
    async with httpx.AsyncClient(base_url=api_url, timeout=30.0) as client:
        with open(path, "rb") as f:
            files = {"file": (path.name, f, "text/yaml")}
            resp = await client.post(f"/projects/{project_id}/profiles", files=files)
        resp.raise_for_status()
        print(f"[seed] profile uploaded: {path.name}")


async def main_async(*, project_id: str, dsn: str, ensure_schema_flag: bool) -> None:
    if not os.environ.get("CG_CONFIG_TOML"):
        cfg_path = REPO_ROOT / "config.toml"
        if cfg_path.exists():
            os.environ["CG_CONFIG_TOML"] = str(cfg_path)

    from infra.project_bootstrap import ProjectBootstrapper, ensure_project, ensure_schema

    pool = AsyncConnectionPool(dsn, open=False)
    await pool.open()
    try:
        if ensure_schema_flag:
            await ensure_schema(pool)
            print("[seed] schema ensured")

        await ensure_project(pool, project_id=project_id, title="Sample Test Project", owner_id="user_seed")
        print(f"[seed] project ensured: {project_id}")

        bootstrapper = ProjectBootstrapper(pool=pool)
        await bootstrapper.bootstrap(project_id)
        print(f"[seed] all done project={project_id}")
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="One-click seed: schema + tools + profiles + agents.")
    parser.add_argument("--project", default=os.environ.get("PROJECT_ID", "proj_mvp_001"))
    parser.add_argument("--dsn", default=os.environ.get("PG_DSN"))
    parser.add_argument("--api-url", default=os.environ.get("API_URL"))
    parser.add_argument("--profile-yaml", action="append", default=[], help="Upload profile YAML via API (repeatable)")
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Run init_*.sql to create schemas/tables. Default: off (assume reset_db already ran).",
    )
    args = parser.parse_args()

    if args.dsn:
        dsn = args.dsn
    else:
        cfg = load_toml_config()
        dsn, _ = resolve_cardbox_dsn(cfg)
        if not dsn:
            raise SystemExit(
                "No DSN found. Set PG_DSN env var or configure [cardbox].postgres_dsn in config.toml"
            )

    asyncio.run(main_async(project_id=args.project, dsn=dsn, ensure_schema_flag=args.ensure_schema))

    profile_paths = [Path(p) for p in (args.profile_yaml or []) if str(p).strip()]
    if profile_paths:
        cfg = load_toml_config()
        api_url = str(args.api_url or api_base_url(cfg))
        async def _run_uploads() -> None:
            await asyncio.gather(
                *[
                    _upload_profile_yaml(api_url=api_url, project_id=args.project, path=p)
                    for p in profile_paths
                ]
            )

        asyncio.run(_run_uploads())


if __name__ == "__main__":
    main()
