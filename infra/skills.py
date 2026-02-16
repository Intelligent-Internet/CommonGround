from __future__ import annotations

import hashlib
import os
import shutil
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

from core.errors import BadRequestError, NotFoundError
from infra.gcs_client import GcsClient
from infra.stores import SkillStore


def _sha256_text(data: str) -> str:
    digest = hashlib.sha256()
    digest.update(data.encode("utf-8"))
    return digest.hexdigest()


def _mock_enabled() -> bool:
    val = str(os.getenv("SKILLS_TEST_ALLOW_MOCK", "")).strip().lower()
    return val in ("1", "true", "yes", "on")


def _mock_skill_content(skill_name: str) -> str:
    return f"name: {skill_name}\ndescription: test mock skill\n"


def _safe_relpath(path: str) -> Path:
    raw = (path or "").strip().lstrip("/")
    if not raw:
        raise BadRequestError("path is empty")
    if ".." in Path(raw).parts:
        raise BadRequestError("path traversal detected")
    return Path(raw)


def _safe_extract_zip(zip_path: Path, dest_dir: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            name = member.filename
            if not name or name.endswith("/"):
                continue
            rel = _safe_relpath(name)
            target = (dest_dir / rel).resolve()
            if not str(target).startswith(str(dest_dir.resolve())):
                raise BadRequestError("zip entry escapes destination")
        zf.extractall(dest_dir)


class SkillManager:
    def __init__(
        self,
        *,
        store: SkillStore,
        gcs: GcsClient,
        cache_root: str,
        max_file_bytes: int = 200 * 1024,
    ):
        self.store = store
        self.gcs = gcs
        self.cache_root = Path(cache_root)
        self.max_file_bytes = int(max_file_bytes)

    async def ensure_skill_local(self, *, project_id: str, skill_name: str) -> Dict[str, Any]:
        normalized_name = str(skill_name or "").strip()
        if _mock_enabled() and normalized_name == "mock-test-only":
            content = _mock_skill_content(normalized_name)
            version_hash = _sha256_text(content)
            local_dir = self.cache_root / project_id / normalized_name / version_hash
            if not local_dir.exists():
                local_dir.mkdir(parents=True, exist_ok=True)
                (local_dir / "SKILL.md").write_text(content, encoding="utf-8")
            active_link = self.cache_root / project_id / normalized_name / "ACTIVE"
            active_link.parent.mkdir(parents=True, exist_ok=True)
            if active_link.exists() or active_link.is_symlink():
                active_link.unlink()
            os.symlink(local_dir, active_link)
            return {
                "skill": {
                    "skill_name": normalized_name,
                    "enabled": True,
                    "active_version_hash": version_hash,
                    "description": "test mock skill",
                },
                "version_hash": version_hash,
                "local_dir": str(local_dir),
                "active_path": str(active_link),
            }
        skill = await self.store.get_skill(project_id=project_id, skill_name=skill_name)
        if not skill:
            raise NotFoundError(f"skill not found: {skill_name}")
        if not skill.get("enabled", True):
            raise BadRequestError(f"skill disabled: {skill_name}")
        version_hash = skill.get("active_version_hash")
        if not isinstance(version_hash, str) or not version_hash.strip():
            raise BadRequestError(f"skill has no active version: {skill_name}")
        version_hash = version_hash.strip()
        local_dir = self.cache_root / project_id / skill_name / version_hash
        if not local_dir.exists():
            version_row = await self.store.get_skill_version(
                project_id=project_id, skill_name=skill_name, version_hash=version_hash
            )
            if not version_row:
                raise NotFoundError(f"skill version not found: {skill_name}@{version_hash}")
            storage_uri = version_row.get("storage_uri")
            if not isinstance(storage_uri, str) or not storage_uri.startswith("gs://"):
                raise BadRequestError("skill storage_uri invalid")
            bucket, path = storage_uri[5:].split("/", 1)
            if bucket != self.gcs.bucket_name:
                raise BadRequestError("skill bucket mismatch")
            local_dir.parent.mkdir(parents=True, exist_ok=True)
            tmp_zip = local_dir.parent / f"{version_hash}.zip"
            tmp_dir = local_dir.parent / f"{version_hash}.tmp"
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
            tmp_dir.mkdir(parents=True, exist_ok=True)
            self.gcs.download_to_file_raw(path, str(tmp_zip))
            _safe_extract_zip(tmp_zip, tmp_dir)
            tmp_zip.unlink(missing_ok=True)
            if local_dir.exists():
                shutil.rmtree(local_dir)
            tmp_dir.rename(local_dir)
        active_link = self.cache_root / project_id / skill_name / "ACTIVE"
        active_link.parent.mkdir(parents=True, exist_ok=True)
        if active_link.exists() or active_link.is_symlink():
            active_link.unlink()
        os.symlink(local_dir, active_link)
        return {
            "skill": skill,
            "version_hash": version_hash,
            "local_dir": str(local_dir),
            "active_path": str(active_link),
        }

    def read_skill_file(self, *, base_dir: str, rel_path: str) -> Dict[str, Any]:
        rel = _safe_relpath(rel_path)
        root = Path(base_dir).resolve()
        target = (root / rel).resolve()
        if not str(target).startswith(str(root)):
            raise BadRequestError("path escapes skill root")
        if not target.exists() or not target.is_file():
            raise NotFoundError(f"skill file not found: {rel_path}")
        size = target.stat().st_size
        if size > self.max_file_bytes:
            raise BadRequestError("skill file too large")
        content = target.read_text(encoding="utf-8")
        return {
            "content": content,
            "bytes": size,
            "sha256": _sha256_text(content),
        }
