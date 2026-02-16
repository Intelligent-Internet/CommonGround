from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


TESTS_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LLM_PATH = TESTS_ROOT / "default_llm.yaml"


def load_default_llm_config() -> Dict[str, Any]:
    if not DEFAULT_LLM_PATH.exists():
        return {}
    payload = yaml.safe_load(DEFAULT_LLM_PATH.read_text(encoding="utf-8")) or {}
    llm_cfg = payload.get("llm_config")
    return llm_cfg if isinstance(llm_cfg, dict) else {}


def apply_default_llm_config(profile_data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(profile_data, dict):
        return profile_data
    if not profile_data.get("llm_config"):
        profile_data["llm_config"] = load_default_llm_config()
    return profile_data
