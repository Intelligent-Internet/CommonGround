from __future__ import annotations

import hashlib
import io
import zipfile
from typing import Any, Dict, Optional

import yaml

from core.errors import BadRequestError, NotFoundError
from infra.gcs_client import GcsClient, GcsConfig
from infra.skills import _safe_relpath
from infra.stores import ProjectStore, SkillStore
from services.api.skill_models import Skill, SkillPatch, SkillUploadResponse, SkillVersion


def _sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def _parse_skill_front_matter(text: str) -> Dict[str, Any]:
    raw = text.lstrip()
    if not raw.startswith("---"):
        raise BadRequestError("SKILL.md missing YAML front matter")
    end = raw.find("\n---", 3)
    if end == -1:
        raise BadRequestError("SKILL.md front matter not terminated")
    yaml_block = raw[3:end]
    data = yaml.safe_load(yaml_block) or {}
    if not isinstance(data, dict):
        raise BadRequestError("SKILL.md front matter must be a YAML object")
    name = data.get("name")
    desc = data.get("description")
    if not isinstance(name, str) or not name.strip():
        raise BadRequestError("SKILL.md front matter missing name")
    if not isinstance(desc, str) or not desc.strip():
        raise BadRequestError("SKILL.md front matter missing description")
    return {"name": name.strip(), "description": desc.strip()}


def _inspect_zip(zip_bytes: bytes) -> Dict[str, Any]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
        names = [n for n in zf.namelist() if n and not n.endswith("/")]
        if not names:
            raise BadRequestError("skill zip is empty")
        root = names[0].split("/")[0]
        if not root:
            raise BadRequestError("skill zip missing root dir")
        for name in names:
            parts = name.split("/")
            if not parts or parts[0] != root:
                raise BadRequestError("skill zip must contain a single root folder")
            _safe_relpath(name)
        skill_md = f"{root}/SKILL.md"
        if skill_md not in names:
            raise BadRequestError("skill zip missing SKILL.md")
        with zf.open(skill_md) as f:
            text = f.read().decode("utf-8")
        meta = _parse_skill_front_matter(text)
        if meta["name"] != root:
            raise BadRequestError("skill name must match root folder")
        return {"root": root, "meta": meta}


class SkillService:
    def __init__(
        self,
        *,
        project_store: ProjectStore,
        skill_store: SkillStore,
        gcs_cfg: GcsConfig,
    ):
        self.project_store = project_store
        self.skill_store = skill_store
        self.gcs = GcsClient(gcs_cfg)

    async def upload_skill_zip(
        self,
        *,
        project_id: str,
        zip_bytes: bytes,
    ) -> SkillUploadResponse:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        if not zip_bytes:
            raise BadRequestError("zip is empty")
        info = _inspect_zip(zip_bytes)
        meta = info["meta"]
        skill_name = meta["name"]
        description = meta["description"]
        version_hash = _sha256_bytes(zip_bytes)
        storage_path = f"skills/{project_id}/{skill_name}/{version_hash}/skill.zip"
        storage_uri = f"gs://{self.gcs.bucket_name}/{self.gcs._with_prefix(storage_path)}"
        self.gcs.upload_bytes(storage_path, zip_bytes, content_type="application/zip")
        version_row = await self.skill_store.add_skill_version(
            project_id=project_id,
            skill_name=skill_name,
            version_hash=version_hash,
            storage_uri=storage_uri,
            description=description,
            metadata={"root": info["root"]},
        )
        skill_row = await self.skill_store.upsert_skill(
            project_id=project_id,
            skill_name=skill_name,
            description=description,
            enabled=True,
            allow_scripts=True,
            active_version_hash=version_hash,
            metadata={},
        )
        await self.skill_store.set_active_version(
            project_id=project_id,
            skill_name=skill_name,
            version_hash=version_hash,
        )
        return SkillUploadResponse(
            skill=Skill.model_validate(skill_row),
            version=SkillVersion.model_validate(version_row),
        )

    async def list_skills(self, *, project_id: str, limit: int = 200, offset: int = 0) -> list[Skill]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        rows = await self.skill_store.list_skills(project_id=project_id, limit=limit, offset=offset)
        return [Skill.model_validate(r) for r in rows or []]

    async def get_skill(self, *, project_id: str, skill_name: str) -> Optional[Skill]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        row = await self.skill_store.get_skill(project_id=project_id, skill_name=skill_name)
        return Skill.model_validate(row) if row else None

    async def patch_skill(
        self,
        *,
        project_id: str,
        skill_name: str,
        patch: SkillPatch,
    ) -> Optional[Skill]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        row = await self.skill_store.get_skill(project_id=project_id, skill_name=skill_name)
        if not row:
            return None
        if patch.enabled is not None:
            row = await self.skill_store.set_enabled(
                project_id=project_id,
                skill_name=skill_name,
                enabled=patch.enabled,
            )
        return Skill.model_validate(row) if row else None

    async def activate_skill_version(
        self,
        *,
        project_id: str,
        skill_name: str,
        version_hash: str,
    ) -> Optional[Skill]:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        existing = await self.skill_store.get_skill_version(
            project_id=project_id,
            skill_name=skill_name,
            version_hash=version_hash,
        )
        if not existing:
            return None
        row = await self.skill_store.set_active_version(
            project_id=project_id,
            skill_name=skill_name,
            version_hash=version_hash,
        )
        return Skill.model_validate(row) if row else None

    async def delete_skill(self, *, project_id: str, skill_name: str) -> bool:
        if not await self.project_store.get_project(project_id):
            raise NotFoundError(f"project not found: {project_id}")
        return await self.skill_store.delete_skill(project_id=project_id, skill_name=skill_name)
