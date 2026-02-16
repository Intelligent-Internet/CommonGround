from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class DelegationPolicyYaml(BaseModel):
    """Delegation/spawn policy for sub-agents by profile name."""

    model_config = ConfigDict(extra="ignore")

    target_tags: List[str] = Field(default_factory=list)
    # null => allow all profiles (subject to target_tags filter)
    target_profiles: Optional[List[str]] = None


class Profile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    project_id: str
    profile_box_id: str
    name: str
    worker_target: str
    tags: List[str] = Field(default_factory=list)
    system_prompt_compiled: Optional[str] = None
    updated_at: Optional[datetime] = None


class ProfileYaml(BaseModel):
    """Normalized YAML content used to build sys.profile cards."""

    model_config = ConfigDict(extra="allow")

    name: str = Field(min_length=1)
    worker_target: str = Field(min_length=1)
    tags: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    system_prompt_template: str = ""
    # Preferred delegation configuration.
    delegation_policy: Optional[DelegationPolicyYaml] = None
    allowed_tools: List[str] = Field(default_factory=list)
    allowed_internal_tools: List[str] = Field(default_factory=list)
    must_end_with: List[str] = Field(default_factory=list)
    llm_config: Dict[str, Any] = Field(default_factory=dict)
