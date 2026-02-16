from __future__ import annotations

from pathlib import Path
from typing import Optional


def _normalize_profile_name(name: str) -> str:
    normalized = []
    for ch in name.lower():
        normalized.append(ch if ch.isalnum() else "_")
    return "".join(normalized).strip("_")


def resolve_profile_yaml_from_examples(
    profile_name: str,
    *,
    examples_dir: Optional[Path] = None,
) -> Optional[Path]:
    root = examples_dir or (Path(__file__).resolve().parents[2] / "examples" / "profiles")
    if not root.exists():
        return None
    normalized = _normalize_profile_name(profile_name)
    for path in sorted(root.glob("*.yaml")):
        stem = path.stem
        if stem.lower() == profile_name.lower():
            return path
        if _normalize_profile_name(stem) == normalized:
            return path
    return None
