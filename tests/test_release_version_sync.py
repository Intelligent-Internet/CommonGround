from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "release" / "sync_release_version.py"
_SPEC = importlib.util.spec_from_file_location("sync_release_version", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


def test_normalize_version_accepts_canonical_pep440_versions() -> None:
    assert _MODULE.normalize_version("3.1.0") == "3.1.0"
    assert _MODULE.normalize_version("v3.1.1rc1") == "3.1.1rc1"


@pytest.mark.parametrize("value", ["", "v3r1", "3.01.0"])
def test_normalize_version_rejects_noncanonical_release_versions(value: str) -> None:
    with pytest.raises(ValueError):
        _MODULE.normalize_version(value)


def test_update_pyproject_version_replaces_existing_version(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "commonground-kernel"\nversion = "3.1.0"\n', encoding="utf-8")

    changed, previous = _MODULE.update_pyproject_version(pyproject, "3.1.1", write=True)

    assert changed is True
    assert previous == "3.1.0"
    assert 'version = "3.1.1"' in pyproject.read_text(encoding="utf-8")


def test_update_uv_lock_self_package_inserts_missing_version_line(tmp_path: Path) -> None:
    lockfile = tmp_path / "uv.lock"
    lockfile.write_text(
        '[[package]]\nname = "commonground-kernel"\nsource = { editable = "." }\n',
        encoding="utf-8",
    )

    changed, previous = _MODULE.update_uv_lock_self_package(lockfile, "3.1.0", write=True)

    assert changed is True
    assert previous == ""
    assert 'name = "commonground-kernel"\nversion = "3.1.0"\nsource' in lockfile.read_text(encoding="utf-8")


def test_update_uv_lock_self_package_replaces_existing_version_line(tmp_path: Path) -> None:
    lockfile = tmp_path / "uv.lock"
    lockfile.write_text(
        '[[package]]\nname = "commonground-kernel"\nversion = "3.1.0"\nsource = { editable = "." }\n',
        encoding="utf-8",
    )

    changed, previous = _MODULE.update_uv_lock_self_package(lockfile, "3.1.2", write=True)

    assert changed is True
    assert previous == "3.1.0"
    assert 'version = "3.1.2"' in lockfile.read_text(encoding="utf-8")
