from __future__ import annotations

import io
from contextlib import redirect_stdout
from importlib.metadata import PackageNotFoundError

import pytest

from CommonGround.cli import build_parser
from CommonGround.version import UNKNOWN_VERSION, get_package_version


def test_get_package_version_returns_unknown_when_metadata_is_missing(monkeypatch) -> None:
    def raise_package_not_found(_: str) -> str:
        raise PackageNotFoundError

    monkeypatch.setattr("CommonGround.version.version", raise_package_not_found)

    assert get_package_version() == UNKNOWN_VERSION


def test_cg_version_flag_reports_package_version(monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli.get_package_version", lambda: "1.2.3")
    parser = build_parser()
    stdout = io.StringIO()

    with redirect_stdout(stdout), pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--version"])

    assert exc_info.value.code == 0
    assert stdout.getvalue().strip() == "cg 1.2.3"
