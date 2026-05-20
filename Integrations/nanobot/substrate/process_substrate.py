from __future__ import annotations

from collections.abc import Mapping
import os
import subprocess
import sys
from pathlib import Path
from typing import Protocol

from ..adapter.provision_handler import ProvisionLaunchRequest, ProvisionLaunchResult


class PopenFactory(Protocol):
    def __call__(
        self,
        args: list[str],
        *,
        env: dict[str, str],
        cwd: str | None,
        start_new_session: bool,
    ):
        ...


class ProcessOpsSubstrate:
    def __init__(
        self,
        *,
        python_executable: str | None = None,
        cwd: str | Path | None = None,
        base_env: Mapping[str, str] | None = None,
        popen_factory: PopenFactory = subprocess.Popen,
    ) -> None:
        self._python_executable = python_executable or sys.executable
        self._cwd = None if cwd is None else str(cwd)
        self._base_env = base_env
        self._popen_factory = popen_factory

    def start_leaf_worker(self, request: ProvisionLaunchRequest) -> ProvisionLaunchResult:
        env = dict(os.environ if self._base_env is None else self._base_env)
        env.update(request.env)
        env = _without_cg_claim_tokens(env)
        command = [self._python_executable, "-m", "Integrations.nanobot.runtime.leaf_worker_main"]
        try:
            process = self._popen_factory(command, env=env, cwd=self._cwd, start_new_session=True)
        except OSError as exc:
            return ProvisionLaunchResult(started=False, note=str(exc))
        return ProvisionLaunchResult(started=True, handle=f"pid:{process.pid}")


def _without_cg_claim_tokens(env: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in env.items() if not _is_cg_claim_token_key(key)}


def _is_cg_claim_token_key(key: str) -> bool:
    upper_key = key.upper()
    return upper_key.startswith("CG_") and "CLAIM" in upper_key and "TOKEN" in upper_key
