from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "release" / "check_dist_tag_version.py"
_SPEC = importlib.util.spec_from_file_location("check_dist_tag_version", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
validate_release_tag = _MODULE.validate_release_tag


def test_validate_release_tag_accepts_canonical_pep440_tags() -> None:
    assert validate_release_tag("v3.1.0") == "3.1.0"
    assert validate_release_tag("v3.1.1rc1") == "3.1.1rc1"


@pytest.mark.parametrize(
    ("tag", "message"),
    [
        ("0.1.0", "must start with 'v'"),
        ("v3-preview", "canonical PEP 440"),
        ("v3r1", "canonical PEP 440"),
    ],
)
def test_validate_release_tag_rejects_noncanonical_or_non_package_tags(tag: str, message: str) -> None:
    with pytest.raises(SystemExit, match=message):
        validate_release_tag(tag)
