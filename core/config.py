"""Runtime configuration loaded from config.toml (if present), with defaults + env overrides.

Configuration is assembled via AppConfig defaults, then `config.toml`, then env
overrides (e.g. `CG__SECTION__KEY`). Use `CG_CONFIG_TOML` to point elsewhere.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict

from core.app_config import load_app_config, config_to_dict


@lru_cache(maxsize=1)
def _cfg() -> Dict[str, Any]:
    return config_to_dict(load_app_config())


def _require_str(section: str, key: str) -> str:
    cfg = _cfg()
    val = (cfg.get(section) or {}).get(key)
    if not isinstance(val, str) or not val:
        raise RuntimeError(f"Missing required config: [{section}].{key}")
    return val


def _require_int(section: str, key: str) -> int:
    cfg = _cfg()
    val = (cfg.get(section) or {}).get(key)
    if val is None:
        raise RuntimeError(f"Missing required config: [{section}].{key}")
    try:
        return int(val)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Invalid config type: [{section}].{key} must be int") from exc


def _require_float(section: str, key: str) -> float:
    cfg = _cfg()
    val = (cfg.get(section) or {}).get(key)
    if val is None:
        raise RuntimeError(f"Missing required config: [{section}].{key}")
    try:
        return float(val)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Invalid config type: [{section}].{key} must be float") from exc


# Core protocol
PROTOCOL_VERSION = _require_str("protocol", "version")

# Worker runtime knobs
MAX_STEPS = _require_int("worker", "max_steps")
DEFAULT_TIMEOUT = _require_float("worker", "default_timeout_seconds")
MAX_RECURSION_DEPTH = int((_cfg().get("worker") or {}).get("max_recursion_depth", 20))
