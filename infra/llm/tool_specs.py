from __future__ import annotations

import copy
from typing import Any, Dict, List, Tuple


def _extract_tool_def(tool_def: Any) -> Tuple[str | None, str | None, str, Dict[str, Any], Dict[str, Any]]:
    if isinstance(tool_def, dict):
        target_subject = tool_def.get("target_subject")
        tool_name = tool_def.get("tool_name")
        description = tool_def.get("description") or ""
        params = tool_def.get("parameters") or {"type": "object", "properties": {}}
        options = tool_def.get("options") or {}
    else:
        target_subject = getattr(tool_def, "target_subject", None)
        tool_name = getattr(tool_def, "tool_name", None)
        description = getattr(tool_def, "description", None) or ""
        params = getattr(tool_def, "parameters", None) or {"type": "object", "properties": {}}
        options = getattr(tool_def, "options", None) or {}
    return target_subject, tool_name, description, params, options


def _apply_arg_options(params: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
    params_copy = copy.deepcopy(params) if isinstance(params, dict) else {"type": "object", "properties": {}}
    options_map = options if isinstance(options, dict) else {}
    args_opts = options_map.get("args") if isinstance(options_map.get("args"), dict) else {}
    defaults = args_opts.get("defaults") if isinstance(args_opts.get("defaults"), dict) else {}
    fixed = args_opts.get("fixed") if isinstance(args_opts.get("fixed"), dict) else {}
    envs = options_map.get("envs") if isinstance(options_map.get("envs"), dict) else {}

    hidden_keys = set()
    hidden_keys.update(k for k in fixed.keys() if isinstance(k, str))
    hidden_keys.update(k for k in envs.keys() if isinstance(k, str))

    if isinstance(params_copy, dict):
        props = params_copy.get("properties")
        if isinstance(props, dict) and hidden_keys:
            for key in hidden_keys:
                props.pop(key, None)

        required = params_copy.get("required")
        if isinstance(required, list) and hidden_keys:
            params_copy["required"] = [r for r in required if r not in hidden_keys]

        if isinstance(props, dict) and defaults:
            for key, value in defaults.items():
                if key in hidden_keys:
                    continue
                prop = props.get(key)
                if isinstance(prop, dict) and "default" not in prop:
                    prop["default"] = value

    return params_copy


def build_tool_specs(tool_defs: List[Any]) -> List[Dict[str, Any]]:
    """Build OpenAI-style tool specs from DB definitions."""
    specs: List[Dict[str, Any]] = []
    for tool_def in tool_defs or []:
        target_subject, tool_name, description, params, options = _extract_tool_def(tool_def)
        if not target_subject:
            continue

        params_copy = _apply_arg_options(params, options)
        specs.append(
            {
                "type": "function",
                "function": {
                    "name": tool_name,
                    "description": description,
                    "parameters": params_copy,
                },
            }
        )
    return specs
