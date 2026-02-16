from __future__ import annotations

from typing import Any, Dict, Optional, List

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ToolDefinition(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    project_id: str
    tool_name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)

    target_subject: Optional[str] = None
    after_execution: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("parameters", "options", mode="before")
    @classmethod
    def _none_to_dict(cls, v: Any) -> Any:
        if v is None:
            return {}
        return v


class ProjectAgent(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    project_id: str
    agent_id: str
    profile_box_id: str
    worker_target: str
    tags: List[str] = Field(default_factory=list)
    display_name: Optional[str] = None
    owner_agent_id: Optional[str] = None
    status: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
