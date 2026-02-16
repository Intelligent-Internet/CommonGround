from __future__ import annotations

from typing import Any, List, Optional

from core.errors import ProtocolViolationError
from core.utp_protocol import FieldSchema, FieldsSchemaContent


def extract_fields_schema(card: Any) -> List[FieldSchema]:
    content = getattr(card, "content", None)
    if not isinstance(content, FieldsSchemaContent):
        raise ProtocolViolationError("task.result_fields content must be FieldsSchemaContent")
    return list(content.fields or [])


def normalize_fields_schema(
    value: Any, *, default: Optional[List[FieldSchema]] = None
) -> List[FieldSchema]:
    if value is None:
        return list(default or [])
    if isinstance(value, FieldsSchemaContent):
        return list(value.fields or [])
    if not isinstance(value, list):
        raise ProtocolViolationError("result_fields must be a list of field objects")
    fields: List[FieldSchema] = []
    for item in value:
        if isinstance(item, FieldSchema):
            fields.append(item)
        elif isinstance(item, dict):
            fields.append(FieldSchema.model_validate(item))
        else:
            raise ProtocolViolationError("result_fields must contain field objects")
    return fields
