from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class ArtifactSignedUrlResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    artifact_id: str
    project_id: str
    filename: Optional[str] = None
    mime: Optional[str] = None
    size: Optional[int] = None
    sha256: Optional[str] = None
    storage_uri: Optional[str] = None
    signed_url: str
    expires_at: datetime


class ArtifactUploadResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    artifact_id: str
    project_id: str
    filename: Optional[str] = None
    mime: Optional[str] = None
    size: Optional[int] = None
