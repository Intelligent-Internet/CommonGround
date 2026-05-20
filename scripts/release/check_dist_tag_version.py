#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from packaging.version import InvalidVersion, Version


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify that built wheel and sdist versions match one release tag."
    )
    parser.add_argument("--dist-dir", default="dist", help="Directory containing built artifacts.")
    parser.add_argument(
        "--package-prefix",
        default="commonground_kernel",
        help="Normalized package filename prefix. Default: commonground_kernel.",
    )
    parser.add_argument("--tag", required=True, help="Release tag, usually v<version>.")
    return parser.parse_args()


def _one(paths: list[Path], label: str) -> Path:
    if len(paths) != 1:
        raise SystemExit(f"Expected exactly one {label} artifact, found {len(paths)}")
    return paths[0]


def validate_release_tag(tag: str) -> str:
    if not tag.startswith("v"):
        raise SystemExit(f"Release tag {tag!r} must start with 'v'")

    raw_version = tag.removeprefix("v")
    try:
        normalized = str(Version(raw_version))
    except InvalidVersion as exc:
        raise SystemExit(
            f"Release tag {tag!r} is not a valid PEP 440 version tag. "
            "Use canonical tags such as v3.1.0 or v3.1.1rc1."
        ) from exc

    if normalized != raw_version:
        raise SystemExit(
            f"Release tag {tag!r} is not a canonical PEP 440 version tag. "
            f"The canonical PEP 440 form would be v{normalized!s}, but semantic release-line "
            "names such as v3-preview and route labels such as v3r1 must not be used as package release tags."
        )

    return raw_version


def main() -> int:
    args = _parse_args()
    dist_dir = Path(args.dist_dir)
    prefix = f"{args.package_prefix}-"
    expected = validate_release_tag(args.tag)

    wheel = _one(sorted(dist_dir.glob(f"{prefix}*.whl")), "wheel")
    sdist = _one(sorted(dist_dir.glob(f"{prefix}*.tar.gz")), "sdist")

    wheel_version = wheel.name.removeprefix(prefix).split("-py3", 1)[0]
    sdist_version = sdist.name.removeprefix(prefix).removesuffix(".tar.gz")

    if wheel_version != expected:
        raise SystemExit(f"Wheel version {wheel_version!r} does not match tag {expected!r}")
    if sdist_version != expected:
        raise SystemExit(f"sdist version {sdist_version!r} does not match tag {expected!r}")

    print(f"Validated release version: {expected}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
