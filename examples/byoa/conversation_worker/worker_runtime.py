#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def _latest_record_role(context: dict) -> str | None:
    items = context.get("semantic_items")
    if not isinstance(items, list) or not items:
        return None
    record = items[-1].get("record") if isinstance(items[-1], dict) else None
    role = record.get("record_role") if isinstance(record, dict) else None
    return role if isinstance(role, str) else None


def main() -> None:
    context_path = Path(_required_env("CG_CONTEXT_FILE"))
    final_path = Path(_required_env("CG_FINAL_FILE"))
    context = json.loads(context_path.read_text(encoding="utf-8"))

    turn = context.get("turn", {}) if isinstance(context, dict) else {}
    turn_ref = turn.get("turn", {}) if isinstance(turn, dict) else {}
    items = context.get("semantic_items", []) if isinstance(context, dict) else []
    payload = {
        "summary": "Minimal BYOA worker completed the turn.",
        "turn_id": turn_ref.get("turn_id"),
        "agent_id": os.environ.get("CG_AGENT_ID"),
        "semantic_item_count": len(items) if isinstance(items, list) else 0,
        "latest_record_role": _latest_record_role(context) if isinstance(context, dict) else None,
    }

    final_path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
