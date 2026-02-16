"""
E2B Sandbox Reaper

Periodically deletes expired sandboxes from the registry.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, Optional

import toml

from core.utils import REPO_ROOT, set_loop_policy, set_python_path

set_loop_policy()
set_python_path()

from infra.stores import SandboxStore  # noqa: E402


def _configure_logging() -> None:
    level_name = str(os.getenv("CG_LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


_configure_logging()
logger = logging.getLogger("SandboxReaper")


def _load_config() -> Dict[str, Any]:
    cfg_path = REPO_ROOT / "config.toml"
    if not cfg_path.exists():
        return {}
    return toml.load(cfg_path)


class SandboxReaper:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
        dsn = cardbox_cfg.get("postgres_dsn", "postgresql://postgres:postgres@localhost:5433/cardbox")
        self.sandbox_store = SandboxStore(dsn)

        tool_cfg = (cfg.get("tools") or {}).get("e2b") if isinstance(cfg.get("tools"), dict) else {}
        tool_cfg = tool_cfg if isinstance(tool_cfg, dict) else {}
        self.interval_sec = int(tool_cfg.get("reaper_interval_sec", 60))
        self.batch_size = int(tool_cfg.get("reaper_batch_size", 50))
        domain_val = tool_cfg.get("domain") or os.getenv("E2B_DOMAIN")
        self.domain = domain_val.strip() if isinstance(domain_val, str) and domain_val.strip() else None

    async def start(self) -> None:
        await self.sandbox_store.open()
        logger.info("Sandbox reaper started (interval=%ss batch=%s)", self.interval_sec, self.batch_size)
        while True:
            try:
                await self._reap_once()
            except Exception as exc:  # noqa: BLE001
                logger.error("Reaper loop error: %s", exc)
            await asyncio.sleep(self.interval_sec)

    async def _reap_once(self) -> None:
        rows = await self.sandbox_store.list_expired(limit=self.batch_size)
        if not rows:
            return
        for row in rows:
            project_id = str(row.get("project_id"))
            agent_id = str(row.get("agent_id"))
            sandbox_id = str(row.get("sandbox_id"))
            domain = row.get("domain") or self.domain
            await self.sandbox_store.mark_status(
                project_id=project_id,
                agent_id=agent_id,
                status="deleting",
                metadata={"last_reap_attempt": "delete"},
            )
            try:
                await asyncio.to_thread(self._kill_sandbox, sandbox_id, domain)
                await self.sandbox_store.delete_sandbox(project_id=project_id, agent_id=agent_id)
                logger.info("Deleted sandbox %s (project=%s agent=%s)", sandbox_id, project_id, agent_id)
            except Exception as exc:  # noqa: BLE001
                await self.sandbox_store.mark_status(
                    project_id=project_id,
                    agent_id=agent_id,
                    status="error",
                    metadata={"last_error": str(exc)},
                )
                logger.warning(
                    "Failed to delete sandbox %s (project=%s agent=%s): %s",
                    sandbox_id,
                    project_id,
                    agent_id,
                    exc,
                )

    def _kill_sandbox(self, sandbox_id: str, domain: Optional[str]) -> None:
        try:
            from e2b_code_interpreter import Sandbox  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"E2B SDK unavailable: {exc}") from exc
        opts: Dict[str, Any] = {}
        if domain:
            opts["domain"] = domain
        sandbox = Sandbox._cls_connect(sandbox_id, **opts)
        sandbox.kill()


def main() -> None:
    cfg = _load_config()
    reaper = SandboxReaper(cfg)
    asyncio.run(reaper.start())


if __name__ == "__main__":
    main()
