from __future__ import annotations

import os
import sys
from pathlib import Path


def resolve_nanobot_repo_root(repo_root: str | Path | None = None) -> Path | None:
    if repo_root is not None:
        return Path(repo_root).expanduser().resolve()
    configured = os.environ.get("NANOBOT_REPO_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    repo_root = Path(__file__).resolve().parents[3]
    candidate = repo_root.parent / "nanobot"
    return candidate if candidate.exists() else None


def configure_nanobot_logging(*, level: str | None = None) -> None:
    configured_level = (level or os.environ.get("CG_NANOBOT_LOG_LEVEL", "INFO")).upper()
    try:
        from loguru import logger
    except ImportError:
        return
    logger.remove()
    logger.add(
        sys.stderr,
        level=configured_level,
        backtrace=True,
        diagnose=True,
        enqueue=False,
    )
    logger.enable("nanobot")


def build_agent_loop(*, config_path: str | Path | None = None, workspace: str | Path | None = None, repo_root: str | Path | None = None):
    nanobot_root = resolve_nanobot_repo_root(repo_root)
    if nanobot_root is not None and not nanobot_root.exists():
        raise FileNotFoundError(f"nanobot repo root not found: {nanobot_root}")
    if nanobot_root is not None and str(nanobot_root) not in sys.path:
        sys.path.insert(0, str(nanobot_root))
    configure_nanobot_logging()

    from nanobot.nanobot import Nanobot

    bot = Nanobot.from_config(config_path, workspace=workspace)
    loop = getattr(bot, "_loop", None)
    if loop is None:
        raise RuntimeError("Nanobot.from_config() did not expose an AgentLoop")
    return loop
