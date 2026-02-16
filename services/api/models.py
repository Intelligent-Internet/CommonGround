from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class ProjectCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: Optional[str] = None
    title: str = Field(min_length=1)
    owner_id: str = Field(min_length=1)
    bootstrap: bool = True


class ProjectUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: Optional[str] = Field(default=None, min_length=1)
    status: Optional[str] = None


class Project(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    title: Optional[str] = None
    status: Optional[str] = None
    owner_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
