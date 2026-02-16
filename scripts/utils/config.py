from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import toml

from core.utils import REPO_ROOT

DEFAULT_CARDBOX_DSN = "postgresql://postgres:postgres@localhost:5433/cardbox"


def load_toml_config(path: Optional[Path] = None) -> Dict[str, Any]:
    cfg_path = path or (REPO_ROOT / "config.toml")
    if not cfg_path.exists():
        return {}
    return toml.load(cfg_path)


def api_base_url(cfg: Dict[str, Any]) -> str:
    api_cfg = cfg.get("api", {}) if isinstance(cfg, dict) else {}
    host = str(api_cfg.get("listen_host") or "127.0.0.1")
    port = int(api_cfg.get("port") or 8099)
    if host in ("0.0.0.0", "::"):
        host = "127.0.0.1"
    return f"http://{host}:{port}"


def resolve_cardbox_dsn(
    cfg: Dict[str, Any],
    *,
    override: Optional[str] = None,
    env_var: str = "PG_DSN",
    default: Optional[str] = None,
) -> Tuple[str, str]:
    if override:
        return str(override), "override"
    env_dsn = os.environ.get(env_var)
    if env_dsn:
        return env_dsn, "env"
    cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
    dsn = str(cardbox_cfg.get("postgres_dsn") or "")
    if dsn:
        return dsn, "config"
    if default is not None:
        return str(default), "default"
    if isinstance(cfg, dict) and cfg:
        return "", "config"
    return "", "missing"


def require_cardbox_dsn(
    cfg: Dict[str, Any],
    *,
    override: Optional[str] = None,
    env_var: str = "PG_DSN",
    default: Optional[str] = None,
    error: str = "cardbox.postgres_dsn missing in config.toml",
) -> str:
    dsn, _source = resolve_cardbox_dsn(
        cfg,
        override=override,
        env_var=env_var,
        default=default,
    )
    if not dsn:
        raise RuntimeError(error)
    return dsn
