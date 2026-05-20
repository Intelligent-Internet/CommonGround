from __future__ import annotations

import os

from Integrations.nanobot.provision_lifecycle import DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS
from Integrations.nanobot.runtime.provisioner_runner import run_provisioner_worker_forever
from Integrations.nanobot.substrate.process_substrate import ProcessOpsSubstrate

from .common_agent_demo import configure_demo_logging, load_demo_env


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return default if raw is None else float(raw)


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return default if raw is None else int(raw)


def main() -> None:
    configure_demo_logging()
    env = load_demo_env()
    run_provisioner_worker_forever(
        base_url=env.base_url,
        agent=env.nanobot_provisioner(),
        substrate=ProcessOpsSubstrate(),
        repo_root=env.nanobot_repo_root,
        config_path=env.nanobot_config_path,
        workspace_root=env.nanobot_leaf_workspace,
        provision_lifecycle_cleanup_interval_seconds=_float_env("CG_PROVISION_LIFECYCLE_CLEANUP_INTERVAL_SECONDS", 30.0),
        provision_lifecycle_ttl_seconds=_int_env(
            "CG_PROVISION_LIFECYCLE_TTL_SECONDS",
            DEFAULT_EPHEMERAL_LIFECYCLE_TTL_SECONDS,
        ),
    )


if __name__ == "__main__":
    main()
