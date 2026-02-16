from __future__ import annotations

import json
from typing import Any, Dict, Optional


def normalize_tool_args(tool_call: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fn = tool_call.get("function") or {}
    args = fn.get("arguments")
    if isinstance(args, dict):
        return args
    if isinstance(args, str):
        try:
            parsed = json.loads(args)
        except Exception:  # noqa: BLE001
            return None
        if isinstance(parsed, dict):
            fn["arguments"] = parsed
            return parsed
    return None


def sanitize_provider_options(provider_options: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not provider_options:
        return None
    cleaned: Dict[str, Any] = {}
    for key, value in provider_options.items():
        if isinstance(key, str) and key.lower() in {"api_key", "apikey", "auth_token", "token"}:
            continue
        cleaned[key] = value
    if not cleaned:
        return None
    return cleaned


def build_stream_start_metadata(*, item: Any, profile: Any) -> Dict[str, Any]:
    llm_cfg = profile.llm_config.model_dump(exclude_none=True)
    llm_cfg.pop("api_key", None)
    provider_options = sanitize_provider_options(llm_cfg.get("provider_options"))
    if provider_options is not None:
        llm_cfg["provider_options"] = provider_options
    else:
        llm_cfg.pop("provider_options", None)
    return {
        "agent_id": item.agent_id,
        "profile_id": profile.card_id,
        "profile_name": profile.name,
        "display_name": profile.display_name,
        "tags": list(profile.tags or []),
        "worker_target": profile.worker_target,
        "llm_config": llm_cfg,
    }


def build_card_metadata(
    *,
    item: Any,
    step_id: str,
    role: str,
    tool_call_id: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    meta: Dict[str, Any] = {
        "agent_turn_id": item.agent_turn_id,
        "step_id": step_id,
        "role": role,
    }
    if tool_call_id:
        meta["tool_call_id"] = tool_call_id
    if item.trace_id:
        meta["trace_id"] = item.trace_id
    if item.parent_step_id:
        meta["parent_step_id"] = item.parent_step_id
    if extra:
        meta.update(extra)
    return meta
