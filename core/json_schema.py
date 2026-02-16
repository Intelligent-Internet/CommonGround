from __future__ import annotations

from typing import Any

from jsonschema import validators as _jsonschema_validators


def validate_json_schema_requires_array_items(schema: Any, *, context: str = "schema") -> None:
    """
    Validate a JSONSchema fragment with the constraint:
    - if {"type": "array"} appears anywhere, it must include a non-empty object `items`.

    This matches Vertex/Gemini function-declaration requirements and prevents
    late 400 INVALID_ARGUMENT failures during LLM calls.
    """

    def _walk(node: Any, path: str) -> None:
        if isinstance(node, list):
            for i, child in enumerate(node):
                _walk(child, f"{path}[{i}]")
            return
        if not isinstance(node, dict):
            return

        node_type = node.get("type")
        if node_type == "array":
            items = node.get("items")
            if not isinstance(items, dict) or not items:
                raise ValueError(f"{context}: {path} is array but missing `items` object")
            _walk(items, f"{path}.items")
            return

        props = node.get("properties")
        if isinstance(props, dict):
            for key, child in props.items():
                _walk(child, f"{path}.properties.{key}")

        for key in ("anyOf", "oneOf", "allOf"):
            if isinstance(node.get(key), list):
                _walk(node[key], f"{path}.{key}")

        for key in ("$defs", "definitions"):
            defs = node.get(key)
            if isinstance(defs, dict):
                for def_name, child in defs.items():
                    _walk(child, f"{path}.{key}.{def_name}")

    _walk(schema, "$")


def validate_json_schema(schema: Any, *, context: str = "schema") -> None:
    """
    Validate that a JSON Schema itself is well-formed.
    Raises ValueError on invalid schema.
    """
    if not isinstance(schema, dict):
        raise ValueError(f"{context}: schema must be an object")
    try:
        validator_cls = _jsonschema_validators.validator_for(schema)
        validator_cls.check_schema(schema)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"{context}: invalid JSON Schema ({exc})") from exc
