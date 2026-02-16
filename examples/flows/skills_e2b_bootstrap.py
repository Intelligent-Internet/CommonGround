#!/usr/bin/env python3
from __future__ import annotations

import io
import os
import subprocess
import zipfile
from pathlib import Path

import httpx


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _zip_skill(skill_dir: Path) -> bytes:
    parent = skill_dir.parent
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in skill_dir.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(parent)
            zf.writestr(rel.as_posix(), path.read_bytes())
    return buf.getvalue()


def _run_seed(project_id: str, profile_path: Path) -> None:
    repo_root = _repo_root()
    cmd = [
        "uv",
        "run",
        "-m",
        "scripts.setup.seed",
        "--ensure-schema",
        "--project",
        project_id,
        "--profile-yaml",
        str(profile_path),
    ]
    subprocess.run(cmd, check=True)


def main() -> None:
    project_id = os.getenv("PROJECT_ID", "proj_demo_01")
    api_url = os.getenv("API_URL", "http://127.0.0.1:8099")

    repo_root = _repo_root()
    profile_path = Path(
        os.getenv(
            "PROFILE_PATH",
            str(repo_root / "examples" / "profiles" / "skill_orchestrator_agent.yaml"),
        )
    )
    skill_dir = Path(
        os.getenv(
            "SKILL_DIR",
            str(repo_root / "examples" / "skills" / "web-dev-agent"),
        )
    )
    skill_name = os.getenv("SKILL_NAME", "web-dev-agent")

    if not profile_path.exists():
        raise FileNotFoundError(f"missing profile: {profile_path}")
    if not (skill_dir / "SKILL.md").exists():
        raise FileNotFoundError(f"missing skill: {skill_dir / 'SKILL.md'}")

    _run_seed(project_id, profile_path)

    skill_zip = _zip_skill(skill_dir)

    with httpx.Client(base_url=api_url, timeout=30.0) as client:
        resp = client.post(
            f"/projects/{project_id}/skills:upload",
            files={"file": ("skill.zip", skill_zip, "application/zip")},
        )
        resp.raise_for_status()

        resp = client.patch(
            f"/projects/{project_id}/skills/{skill_name}",
            json={"enabled": True},
        )
        resp.raise_for_status()

    print("[bootstrap] done")
    print("Next:")
    print("  Use the UI or your own end-to-end flow to test skills.")


if __name__ == "__main__":
    main()
