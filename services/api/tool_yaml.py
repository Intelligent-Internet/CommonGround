from __future__ import annotations

from typing import Any, Dict

from services.api.tool_models import ToolYaml
from core.json_schema import validate_json_schema, validate_json_schema_requires_array_items
from core.errors import BadRequestError


def normalize_tool_yaml(data: Any) -> ToolYaml:
    if not isinstance(data, dict):
        raise BadRequestError("YAML root must be a mapping")

    tool_name = data.get("tool_name") or data.get("name")
    if not isinstance(tool_name, str) or not tool_name.strip():
        raise BadRequestError("tool_name is required")

    normalized: Dict[str, Any] = dict(data)
    normalized["tool_name"] = tool_name.strip()
    tool = ToolYaml.model_validate(normalized)
    try:
        validate_json_schema_requires_array_items(
            tool.parameters, context=f"tool_yaml[{tool.tool_name}].parameters"
        )
        validate_json_schema(tool.parameters, context=f"tool_yaml[{tool.tool_name}].parameters")
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc
    return tool
