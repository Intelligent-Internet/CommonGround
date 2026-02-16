from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class Tool(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    tool_name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    target_subject: Optional[str] = None
    after_execution: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)


class ToolYaml(BaseModel):
    """Tool definition YAML for resource.tools registration (external tools only)."""

    model_config = ConfigDict(extra="allow")

    tool_name: str = Field(min_length=1)
    description: Optional[str] = None
    target_subject: str = Field(min_length=1)
    after_execution: str = Field(min_length=1)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    options: Dict[str, Any] = Field(default_factory=dict)
