from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REFERENCE_SKILL = ROOT / "examples" / "skills" / "cg" / "SKILL.md"
WORK_MEMORY_SKILL = ROOT / "examples" / "skills" / "cg-work-memory" / "SKILL.md"
WORK_MEMORY_README = ROOT / "examples" / "skills" / "cg-work-memory" / "README.md"
REPO_LOCAL_CG_SKILL = ROOT / ".agents" / "skills" / "cg" / "SKILL.md"
REPO_LOCAL_WORKER_SKILL = ROOT / ".agents" / "skills" / "cg-worker" / "SKILL.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _tracked_skill_assets() -> list[str]:
    paths = {
        path.relative_to(ROOT).as_posix()
        for path in ROOT.glob(".agents/skills/*/SKILL.md")
    }
    paths.update(
        path.relative_to(ROOT).as_posix()
        for path in ROOT.glob("examples/**/skills/*/SKILL.md")
    )
    return sorted(paths)


def test_reference_skill_includes_current_allowed_and_forbidden_commands() -> None:
    body = _read(REFERENCE_SKILL)
    assert "- `cg profile ensure-agent`" in body
    assert "- `cg report work-memory`" in body
    assert "- `cg project agent list`" in body
    assert "- `cg project offer list`" in body
    assert "- `cg project offer get`" in body
    assert "- `cg setup project seed`" in body
    assert "- `cg setup project status`" in body
    assert "- `cg setup project client-config`" in body
    assert "- `cg kernel`" in body
    assert "- `cg worker claim run`" in body
    assert "direct database or `PG_DSN` setup commands" in body
    assert "reading credential token files directly" in body
    assert "- Do not call `cg setup` or `cg kernel`; project setup is an operator-only local action, not a prompt-level skill action." in body
    assert "Ask the user/operator for the non-secret client connection facts" in body
    assert "Never ask for a bearer token value in chat" in body
    assert "silently falling back to native subagents" in body
    assert "- Do not put claim tokens, registration credentials, or top-level `meta` into a work-memory manifest." in body
    assert "- Use a higher-level workflow skill such as `cg-work-memory` when you need stricter work-memory manifest guidance; this base skill only defines the CLI safety contract." in body
    allowed_section = body.split("## Allowed commands", 1)[1].split("## Forbidden commands", 1)[0]
    assert "cg setup" not in allowed_section
    assert "cg kernel" not in allowed_section


def test_tracked_skill_asset_inventory_is_intentional() -> None:
    assert _tracked_skill_assets() == [
        ".agents/skills/cg-worker/SKILL.md",
        ".agents/skills/cg/SKILL.md",
        "examples/skills/cg-work-memory/SKILL.md",
        "examples/skills/cg/SKILL.md",
    ]


def test_repo_local_cg_skill_matches_reference_skill() -> None:
    assert REPO_LOCAL_CG_SKILL.is_symlink()
    assert REPO_LOCAL_CG_SKILL.resolve() == REFERENCE_SKILL
    assert _read(REPO_LOCAL_CG_SKILL) == _read(REFERENCE_SKILL)


def test_repo_local_worker_skill_uses_current_worker_surface_only() -> None:
    body = _read(REPO_LOCAL_WORKER_SKILL)
    for command in (
        "cg worker claim next",
        "cg worker claim renew",
        "cg worker claim append",
        "cg worker claim finish",
        "cg worker claim suspend",
        "cg worker claim dispatch-child",
    ):
        assert command in body
    for stale_term in (
        "cg-worker claim next",
        "cg-worker turn append",
        "claim heartbeat",
        "register-with-credential",
        "commonground-v3-service",
    ):
        assert stale_term not in body


def test_work_memory_skill_uses_current_worker_surface_terms() -> None:
    body = _read(WORK_MEMORY_SKILL) + "\n" + _read(WORK_MEMORY_README)
    assert "raw `cg worker` lifecycle commands" in body
    assert "Do not call `cg setup ...`, `cg kernel ...`, direct database commands" in body
    assert "project_not_seeded" in body
    assert "non-secret client connection facts" in body
    assert "Never ask for a bearer token value in chat" in body
    assert "cg-demo/local-agent" in body
    assert "`cg-worker`" not in body
