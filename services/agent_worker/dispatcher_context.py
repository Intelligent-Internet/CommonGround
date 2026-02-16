from __future__ import annotations

import json
from typing import Any, Dict, Optional


def render_template(value: Any, context: Dict[str, Any]) -> Any:
    if not isinstance(value, str):
        return value
    rendered = value
    for key, ctx_val in context.items():
        if ctx_val is None:
            continue
        rendered = rendered.replace(f"{{{key}}}", str(ctx_val))
    return rendered


def apply_tool_arg_options(
    *,
    args: Dict[str, Any],
    tool_options: Any,
    context: Dict[str, Any],
    logger: Any,
) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    args_map = dict(args) if isinstance(args, dict) else {}
    options_map = tool_options if isinstance(tool_options, dict) else {}

    args_opts = options_map.get("args") if isinstance(options_map.get("args"), dict) else {}
    defaults = args_opts.get("defaults") if isinstance(args_opts.get("defaults"), dict) else {}
    fixed = args_opts.get("fixed") if isinstance(args_opts.get("fixed"), dict) else {}
    envs = options_map.get("envs") if isinstance(options_map.get("envs"), dict) else {}

    applied_defaults: Dict[str, Any] = {}
    applied_fixed: Dict[str, Any] = {}
    envs_clean: Dict[str, str] = {}

    if isinstance(envs, dict):
        for key, env_name in envs.items():
            if not isinstance(key, str):
                continue
            if not isinstance(env_name, str) or not env_name.strip():
                continue
            envs_clean[key] = env_name.strip()
            if key in args_map:
                logger.warning(
                    "LLM-provided argument '%s' was removed because it is configured as an env-injected parameter '%s'. "
                    "This may indicate a tool spec configuration issue.",
                    key,
                    env_name.strip(),
                )
                logger.warning(
                    "LLM-provided arg '%s' removed due to env-injected parameter '%s'.",
                    key,
                    env_name.strip(),
                )
                args_map.pop(key, None)

    if isinstance(defaults, dict):
        for key, value in defaults.items():
            if not isinstance(key, str):
                logger.warning("Non-string key in tool args defaults ignored: %r", key)
                continue
            if key in args_map or key in envs_clean:
                continue
            rendered = render_template(value, context)
            args_map[key] = rendered
            applied_defaults[key] = rendered

    if isinstance(fixed, dict):
        for key, value in fixed.items():
            if not isinstance(key, str) or key in envs_clean:
                continue
            rendered = render_template(value, context)
            args_map[key] = rendered
            applied_fixed[key] = rendered

    return args_map, applied_defaults, {"fixed": applied_fixed, "envs": envs_clean}


def parse_tool_args(tool_call: Dict[str, Any]) -> tuple[Dict[str, Any], Optional[str]]:
    raw_args = (tool_call.get("function") or {}).get("arguments")
    if raw_args is None:
        return {}, None
    if isinstance(raw_args, dict):
        return raw_args, None
    if isinstance(raw_args, str):
        try:
            parsed = json.loads(raw_args)
        except Exception as exc:
            return {"_raw": raw_args}, f"Invalid arguments JSON: {exc}"
        if isinstance(parsed, dict):
            return parsed, None
        return {"_raw": parsed}, None
    return {"_raw": raw_args}, None
