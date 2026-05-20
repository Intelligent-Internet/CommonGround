#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path

from packaging.version import InvalidVersion, Version

ROOT = Path(__file__).resolve().parents[2]
VERSION_FILE = ROOT / "VERSION"
PYPROJECT_FILE = ROOT / "pyproject.toml"
UV_LOCK_FILE = ROOT / "uv.lock"
PACKAGE_NAME = "commonground-kernel"


def normalize_version(raw: str) -> str:
    candidate = raw.removeprefix("v").strip()
    if not candidate:
        raise ValueError("Release version is empty.")
    try:
        normalized = str(Version(candidate))
    except InvalidVersion as exc:
        raise ValueError(f"Invalid release version: {raw}") from exc
    if normalized != candidate:
        raise ValueError(
            f"Release version {raw!r} is not canonical PEP 440. Use {normalized!r}."
        )
    return candidate


def read_version_file(path: Path = VERSION_FILE) -> str:
    raw = path.read_text(encoding="utf-8").strip()
    return raw.splitlines()[0].strip() if raw else ""


def write_version_file(version: str) -> tuple[bool, str]:
    previous = read_version_file()
    if previous == version:
        return False, previous
    VERSION_FILE.write_text(f"{version}\n", encoding="utf-8")
    return True, previous


def update_pyproject_version(path: Path, version: str, *, write: bool) -> tuple[bool, str]:
    text = path.read_text(encoding="utf-8")
    pattern = re.compile(r'(?m)^(version\s*=\s*")([^"]+)(")$')
    match = pattern.search(text)
    if not match:
        raise ValueError(f"No version field found in {path}")

    current = match.group(2)
    if current == version:
        return False, current

    if write:
        replaced = pattern.sub(f'\\g<1>{version}\\g<3>', text, count=1)
        path.write_text(replaced, encoding="utf-8")
    return True, current


def update_uv_lock_self_package(path: Path, version: str, *, write: bool) -> tuple[bool, str]:
    text = path.read_text(encoding="utf-8")
    version_pattern = re.compile(
        rf'(?ms)(\[\[package\]\]\nname = "{re.escape(PACKAGE_NAME)}"\nversion = ")([^"]+)(")'
    )
    match = version_pattern.search(text)
    if match:
        current = match.group(2)
        if current == version:
            return False, current
        if write:
            replaced = version_pattern.sub(f'\\g<1>{version}\\g<3>', text, count=1)
            path.write_text(replaced, encoding="utf-8")
        return True, current

    insert_pattern = re.compile(rf'(?m)^(\[\[package\]\]\nname = "{re.escape(PACKAGE_NAME)}"\n)')
    if not insert_pattern.search(text):
        raise ValueError(f'No editable "{PACKAGE_NAME}" package found in {path}')

    if write:
        replaced = insert_pattern.sub(f'\\1version = "{version}"\n', text, count=1)
        path.write_text(replaced, encoding="utf-8")
    return True, ""


def collect_updates(version: str) -> list[tuple[str, tuple[bool, str]]]:
    return [
        ("pyproject.toml", update_pyproject_version(PYPROJECT_FILE, version, write=False)),
        ("uv.lock", update_uv_lock_self_package(UV_LOCK_FILE, version, write=False)),
    ]


def apply_updates(version: str) -> list[tuple[str, tuple[bool, str]]]:
    return [
        ("pyproject.toml", update_pyproject_version(PYPROJECT_FILE, version, write=True)),
        ("uv.lock", update_uv_lock_self_package(UV_LOCK_FILE, version, write=True)),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synchronize repository release metadata for CommonGround Kernel."
    )
    parser.add_argument(
        "--version",
        default=None,
        help="Release version to sync; defaults to VERSION file.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only check that target files already match the version.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write synchronized version values into files.",
    )
    args = parser.parse_args()

    if args.check and args.write:
        raise ValueError("Use either --check or --write, not both.")
    if not VERSION_FILE.exists():
        raise ValueError(f"VERSION file is missing: {VERSION_FILE}")

    target_version = normalize_version(args.version or read_version_file())
    file_version = read_version_file()

    if args.write and target_version != file_version:
        was_updated, previous = write_version_file(target_version)
        if was_updated:
            print(f"{VERSION_FILE}: {previous} -> {target_version}")

    if not args.write and file_version != target_version:
        if args.check:
            print(f"VERSION file mismatch: {file_version} -> {target_version}")
            raise SystemExit(1)
        print(f"WARNING: VERSION file mismatch: {file_version} -> {target_version}")

    updates = apply_updates(target_version) if args.write else collect_updates(target_version)

    changed = False
    for path, (is_changed, previous) in updates:
        if is_changed:
            changed = True
            old = previous or "<missing>"
            print(f"{path}: {old} -> {target_version}")

    if args.check and changed:
        print(f"Version mismatch detected for target {target_version}")
        raise SystemExit(1)

    if args.write:
        print(f"Synchronized files to version {target_version}")
    else:
        print(f"Checked files against version {target_version}")


if __name__ == "__main__":
    main()
