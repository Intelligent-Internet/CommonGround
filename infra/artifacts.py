from __future__ import annotations

import hashlib
from typing import Any, Dict, Optional

import uuid6

from core.errors import BadRequestError, NotFoundError
from infra.gcs_client import GcsClient
from infra.stores import ArtifactStore


def _sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise BadRequestError("artifact storage_uri must be gs://")
    raw = uri[5:]
    if "/" not in raw:
        return raw, ""
    bucket, path = raw.split("/", 1)
    return bucket, path


class ArtifactManager:
    def __init__(self, *, store: ArtifactStore, gcs: GcsClient):
        self.store = store
        self.gcs = gcs

    async def save_bytes(
        self,
        *,
        project_id: str,
        filename: str,
        data: bytes,
        mime: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not isinstance(data, (bytes, bytearray)):
            raise BadRequestError("artifact data must be bytes")
        artifact_id = uuid6.uuid7().hex
        size = len(data)
        sha256 = _sha256_bytes(data)
        path = f"artifacts/{project_id}/{artifact_id}/{filename}"
        storage_uri = f"gs://{self.gcs.bucket_name}/{self.gcs._with_prefix(path)}"
        self.gcs.upload_bytes(path, bytes(data), content_type=mime)
        return await self.store.create_artifact(
            project_id=project_id,
            artifact_id=artifact_id,
            filename=filename,
            mime=mime,
            size=size,
            sha256=sha256,
            storage_uri=storage_uri,
            metadata=metadata or {},
        )

    async def read_bytes(self, *, project_id: str, artifact_id: str) -> tuple[bytes, Dict[str, Any]]:
        row = await self.store.get_artifact(project_id=project_id, artifact_id=artifact_id)
        if not row:
            raise NotFoundError(f"artifact not found: {artifact_id}")
        storage_uri = row.get("storage_uri")
        if not isinstance(storage_uri, str) or not storage_uri:
            raise BadRequestError(f"artifact storage_uri missing: {artifact_id}")
        bucket, path = _parse_gcs_uri(storage_uri)
        if bucket != self.gcs.bucket_name:
            raise BadRequestError(f"artifact bucket mismatch: {bucket}")
        data = self.gcs.download_bytes_raw(path)
        return data, row
