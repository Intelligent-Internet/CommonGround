from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


PACKAGE_NAME = "commonground-kernel"
UNKNOWN_VERSION = "0+unknown"


def get_package_version() -> str:
    try:
        return version(PACKAGE_NAME)
    except PackageNotFoundError:
        return UNKNOWN_VERSION
