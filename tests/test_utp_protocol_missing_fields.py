from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from core.utp_protocol import ToolCommandPayload


class _RequiredPayload(BaseModel):
    tool_name: str
    after_execution: str


def test_missing_fields_from_error_extracts_required_field_names() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _RequiredPayload.model_validate({})

    assert ToolCommandPayload.missing_fields_from_error(exc_info.value) == [
        "tool_name",
        "after_execution",
    ]
