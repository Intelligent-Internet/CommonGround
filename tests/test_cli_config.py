from __future__ import annotations

import argparse
import json

import pytest

from CommonGround.cli_config import resolve_cli_config


CLI_CONFIG_ENV_KEYS = (
    "CG_BASE_URL",
    "CG_AGENT_CREDENTIAL_TOKEN",
    "CG_AGENT_CREDENTIAL_TOKEN_FILE",
    "CG_ADMIN_BASE_URL",
    "CG_ADMIN_AUTH_TOKEN",
    "CG_ADMIN_AUTH_TOKEN_FILE",
    "CG_CALLER_PROJECT_ID",
    "CG_CALLER_AGENT_ID",
    "CG_CONFIG_PATH",
)


def _args(**kwargs):
    return argparse.Namespace(
        base_url=kwargs.get("base_url"),
        admin_base_url=kwargs.get("admin_base_url"),
        auth_token=kwargs.get("auth_token"),
        auth_token_file=kwargs.get("auth_token_file"),
        admin_auth_token=kwargs.get("admin_auth_token"),
        admin_auth_token_file=kwargs.get("admin_auth_token_file"),
        profile=kwargs.get("profile"),
        config=kwargs.get("config"),
    )


@pytest.fixture(autouse=True)
def isolated_cli_config_runtime(monkeypatch, tmp_path) -> None:
    for key in CLI_CONFIG_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr("CommonGround.cli_config.DEFAULT_CONFIG_PATH", tmp_path / "missing-config.json")


def test_cli_config_flag_overrides_env_and_file(monkeypatch, tmp_path) -> None:
    token_file = tmp_path / "token.txt"
    token_file.write_text("config-token\n", encoding="utf-8")
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"base_url": "http://config.example", "auth": {"token_file": str(token_file)}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("CG_BASE_URL", "http://env.example")
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", "env-token")

    resolved = resolve_cli_config(
        _args(base_url="http://flag.example", auth_token="flag-token", config=str(config_path)),
        default_base_url="http://default.example",
    )

    assert resolved.base_url == "http://flag.example"
    assert resolved.auth_token == "flag-token"


def test_cli_config_env_overrides_file(monkeypatch, tmp_path) -> None:
    token_file = tmp_path / "token.txt"
    token_file.write_text("config-token\n", encoding="utf-8")
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"base_url": "http://config.example", "auth": {"token_file": str(token_file)}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("CG_BASE_URL", "http://env.example")
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", "env-token")

    resolved = resolve_cli_config(
        _args(config=str(config_path)),
        default_base_url="http://default.example",
    )

    assert resolved.base_url == "http://env.example"
    assert resolved.auth_token == "env-token"


def test_cli_config_reads_token_file_from_env(monkeypatch, tmp_path) -> None:
    token_file = tmp_path / "token.txt"
    token_file.write_text("file-token\n", encoding="utf-8")
    monkeypatch.delenv("CG_AGENT_CREDENTIAL_TOKEN", raising=False)
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN_FILE", str(token_file))

    resolved = resolve_cli_config(_args(), default_base_url="http://default.example")

    assert resolved.base_url == "http://default.example"
    assert resolved.auth_token == "file-token"


def test_cli_config_can_ignore_caller_env_and_file(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"caller": {"project_id": "config-project", "agent_id": "config-agent"}}), encoding="utf-8")
    monkeypatch.setenv("CG_CALLER_PROJECT_ID", "env-project")
    monkeypatch.setenv("CG_CALLER_AGENT_ID", "env-agent")

    resolved = resolve_cli_config(
        _args(config=str(config_path)),
        default_base_url="http://default.example",
        resolve_caller=False,
    )

    assert resolved.caller_project_id is None
    assert resolved.caller_agent_id is None


def test_cli_config_can_ignore_profile_arg(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")

    resolved = resolve_cli_config(
        _args(config=str(config_path), profile="demo/agent-001"),
        default_base_url="http://default.example",
        resolve_profile=False,
    )

    assert resolved.profile_name is None


def test_cli_config_uses_default_path_when_present(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("CommonGround.cli_config.DEFAULT_CONFIG_PATH", tmp_path / "config.json")
    (tmp_path / "config.json").write_text(json.dumps({"base_url": "http://config.example"}), encoding="utf-8")

    resolved = resolve_cli_config(_args(), default_base_url="http://default.example")

    assert resolved.base_url == "http://config.example"


def test_cli_config_treats_missing_explicit_path_as_empty_write_target(tmp_path) -> None:
    config_path = tmp_path / "new-config.json"

    resolved = resolve_cli_config(_args(config=str(config_path)), default_base_url="http://default.example")

    assert resolved.base_url == "http://default.example"
    assert resolved.config_path == config_path
    assert resolved.write_config_path == config_path


def test_cli_config_rejects_non_object(monkeypatch, tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(["bad"]), encoding="utf-8")

    with pytest.raises(ValueError, match="CLI config file must contain a JSON object"):
        resolve_cli_config(_args(config=str(config_path)), default_base_url="http://default.example")


def test_cli_config_reads_admin_auth_and_profiles(tmp_path) -> None:
    admin_token_file = tmp_path / "admin.token"
    admin_token_file.write_text("admin-token\n", encoding="utf-8")
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "admin_base_url": "http://admin.example",
                "admin_auth": {"token_file": str(admin_token_file)},
                "profiles": {
                    "demo/agent-001": {
                        "project_id": "demo",
                        "agent_id": "agent-001",
                        "profile_kind": "byoa.work_memory_reporter.v1",
                        "runtime_kind": "codex.local.v1",
                        "display_name": "Agent 001",
                        "credential_id": "cred-001",
                        "token_file": "/tmp/agent.token",
                        "status": "ready",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    resolved = resolve_cli_config(
        _args(config=str(config_path), profile="demo/agent-001"),
        default_base_url="http://default.example",
    )

    assert resolved.admin_base_url == "http://admin.example"
    assert resolved.admin_auth_token == "admin-token"
    assert resolved.profile_name == "demo/agent-001"
    assert resolved.profiles["demo/agent-001"].credential_id == "cred-001"
