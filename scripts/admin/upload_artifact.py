"""
Upload a local file as an artifact and print the artifact_id.

Usage:
  uv run -m scripts.upload_artifact --project proj_demo_01 --file /path/to/file.png
"""

from __future__ import annotations

import argparse
import asyncio
import mimetypes
from pathlib import Path
from typing import Any, Dict

import toml

from core.utils import REPO_ROOT
from infra.artifacts import ArtifactManager
from infra.gcs_client import GcsClient, GcsConfig
from infra.stores import ArtifactStore


def _load_config() -> Dict[str, Any]:
    cfg_path = REPO_ROOT / "config.toml"
    if not cfg_path.exists():
        return {}
    return toml.load(cfg_path)


async def _upload(*, project_id: str, file_path: Path, cfg: Dict[str, Any]) -> str:
    cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
    dsn = cardbox_cfg.get("postgres_dsn", "postgresql://postgres:postgres@localhost:5433/cardbox")

    gcs_cfg = cfg.get("gcs", {}) if isinstance(cfg, dict) else {}
    gcs = GcsClient(
        GcsConfig(
            bucket=str(gcs_cfg.get("bucket") or "").strip(),
            prefix=str(gcs_cfg.get("prefix") or "").strip(),
            credentials_path=str(gcs_cfg.get("credentials_path") or "").strip() or None,
        )
    )

    store = ArtifactStore(dsn)
    await store.open()
    try:
        manager = ArtifactManager(store=store, gcs=gcs)
        data = file_path.read_bytes()
        mime, _ = mimetypes.guess_type(str(file_path))
        artifact = await manager.save_bytes(
            project_id=project_id,
            filename=file_path.name,
            data=data,
            mime=mime,
        )
        return str(artifact.get("artifact_id"))
    finally:
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload file to artifact store")
    parser.add_argument("--project", required=True)
    parser.add_argument("--file", required=True, help="Local file path to upload")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        raise SystemExit(f"file not found: {path}")

    cfg = _load_config()
    artifact_id = asyncio.run(_upload(project_id=args.project, file_path=path, cfg=cfg))
    print(artifact_id)


if __name__ == "__main__":
    main()
