from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from core.config_defaults import default_config
from core.utils import REPO_ROOT


def _load_toml(path: Path) -> Dict[str, Any]:
    try:
        import tomllib  # py3.11+
    except ModuleNotFoundError:  # pragma: no cover
        import toml as tomllib  # type: ignore
    with open(path, "rb") as f:
        return tomllib.load(f)


def _parse_env_value(raw: str) -> Any:
    value = (raw or "").strip()
    if value == "":
        return ""
    try:
        return json.loads(value)
    except Exception:
        return value


def apply_env_overrides(
    cfg: Dict[str, Any],
    *,
    prefix: str = "CG__",
    separator: str = "__",
) -> Dict[str, Any]:
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(prefix):
            continue
        path = env_key[len(prefix) :].split(separator)
        path = [p.strip().lower() for p in path if p.strip()]
        if not path:
            continue
        cursor = cfg
        for segment in path[:-1]:
            node = cursor.get(segment)
            if not isinstance(node, dict):
                node = {}
                cursor[segment] = node
            cursor = node
        cursor[path[-1]] = _parse_env_value(env_val)
    return cfg


def apply_legacy_env_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    pg_dsn = os.environ.get("PG_DSN")
    if pg_dsn:
        cfg.setdefault("cardbox", {})["postgres_dsn"] = pg_dsn
    nats_servers = os.environ.get("NATS_SERVERS")
    if nats_servers:
        servers = [s.strip() for s in nats_servers.split(",") if s.strip()]
        if servers:
            cfg.setdefault("nats", {})["servers"] = servers

    return cfg


def apply_defaults(cfg: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    def _deep_apply(target: Dict[str, Any], src: Dict[str, Any]) -> None:
        for key, value in (src or {}).items():
            if key not in target or target[key] is None:
                if isinstance(value, dict):
                    target[key] = {}
                    _deep_apply(target[key], value)
                else:
                    target[key] = value
                continue
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                _deep_apply(target[key], value)

    _deep_apply(cfg, defaults or {})
    return cfg


def _load_raw_config(
    path: Optional[Path] = None,
    *,
    env_prefix: str = "CG__",
    env_separator: str = "__",
) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    cfg_path = path or Path(os.environ.get("CG_CONFIG_TOML") or (REPO_ROOT / "config.toml"))
    if cfg_path.exists():
        cfg = _load_toml(cfg_path)

    apply_legacy_env_overrides(cfg)
    apply_env_overrides(cfg, prefix=env_prefix, separator=env_separator)
    apply_defaults(cfg, default_config())
    return cfg
