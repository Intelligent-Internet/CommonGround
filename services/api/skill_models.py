from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field


class SkillVersion(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    skill_name: str
    version_hash: str
    storage_uri: str
    description: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Skill(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    skill_name: str
    description: Optional[str] = None
    enabled: bool = True
    active_version_hash: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SkillUploadResponse(BaseModel):
    skill: Skill
    version: SkillVersion


class SkillPatch(BaseModel):
    enabled: Optional[bool] = None
