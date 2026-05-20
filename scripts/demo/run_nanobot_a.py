from __future__ import annotations

from Integrations.nanobot.runtime.supervisor_runner import run_supervisor_worker_forever

from .common_agent_demo import configure_demo_logging, load_demo_env


def main() -> None:
    configure_demo_logging()
    env = load_demo_env()
    run_supervisor_worker_forever(
        base_url=env.base_url,
        agent=env.nanobot_a(),
        provisioner_agent=env.nanobot_provisioner(),
        config_path=env.nanobot_config_path,
        workspace=env.nanobot_a_workspace,
        repo_root=env.nanobot_repo_root,
    )


if __name__ == "__main__":
    main()
