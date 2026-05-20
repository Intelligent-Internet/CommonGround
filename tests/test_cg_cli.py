from __future__ import annotations

import io
import json
import stat
from contextlib import redirect_stdout
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import psycopg
from fastapi.testclient import TestClient

from CommonGround.cli import build_parser, main
from CommonGround.agent_client import ClaimLeaseLostError
from CommonGround.agent_client import HttpAgentClient
from CommonGround.agent_client.types import ClaimTurnPartialFailure, ClaimedTurn
from CommonGround.agent_credentials import parse_agent_credential_token
from CommonGround.contracts import AgentRef, AgentSnapshot, CardBoxRef, CauseRef, ClaimToken, DispatchAuthority, DispatchAuthorityMode, TURN_KIND_CONVERSATION_V1, SemanticRecordRef, SemanticRecordSnapshot, TurnOutcome, TurnRef, TurnSnapshot, TurnState, WorkMemoryReportSubmissionResult
from CommonGround.infra import PostgresAgentCredentialStore
from CommonGround.service.serialization import parse_agent_snapshot, to_jsonable
from CommonGround.sdk import SemanticContextItem, TurnContext
from Integrations.admin_service import ADMIN_SERVICE_AGENT_ID, AdminServiceByoaFacade, ByoaRegistrationProcessor, ByoaWorkflowStore, bootstrap_project_admin_service_agent, create_agent_credential_token_request_app
from tests.auth_support import agent_token


NOW = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
PROJECT_ID = "demo"
TURN_ID = "T-1"
AGENT_ID = "agent-001"

pytestmark = pytest.mark.usefixtures("isolated_cli_runtime")


def _parser_help(command_path: tuple[str, ...]) -> str:
    parser = build_parser()
    stdout = io.StringIO()
    with redirect_stdout(stdout), pytest.raises(SystemExit) as exc_info:
        parser.parse_args([*command_path, "--help"])
    assert exc_info.value.code == 0
    return stdout.getvalue()


def test_cg_help_documents_contracts_and_defaults() -> None:
    top_help = _parser_help(())
    assert "CommonGround is an open-source ground layer" in top_help
    assert "For local first run:" in top_help
    assert "Choose one integration path:" in top_help
    assert "cg local -h" in top_help
    assert "--auth-token" not in top_help
    assert "--admin-auth-token" not in top_help
    assert "--caller-project-id" not in top_help

    dispatch_help = _parser_help(("dispatch",))
    dispatch_help_flat = " ".join(dispatch_help.split())
    assert "CG_BASE_URL" in dispatch_help
    assert "at least one of --request-id or --dispatch-key" in dispatch_help_flat
    assert "mirrors it to the other" in dispatch_help_flat
    assert "--payload-json" in dispatch_help_flat
    assert "--payload-stdin" in dispatch_help_flat
    assert "--admin-auth-token" not in dispatch_help_flat
    assert "--caller-project-id" not in dispatch_help_flat

    wait_help = _parser_help(("turn", "wait"))
    assert "Default: 60" in wait_help
    assert "Default: 500" in wait_help
    assert "--caller-project-id" in wait_help
    assert "--admin-auth-token" not in wait_help

    drain_help = _parser_help(("agent", "drain"))
    assert "defaults to --agent-id" in drain_help

    worker_run_help = _parser_help(("worker", "claim", "run"))
    assert "-- CHILD_CMD [ARGS ...]" in worker_run_help
    assert "--admin-auth-token" not in worker_run_help

    profile_ensure_help = _parser_help(("profile", "ensure-agent"))
    assert "local destination profile" in profile_ensure_help
    assert "--invitation-code-file" in profile_ensure_help
    assert "--invitation-code" in profile_ensure_help
    assert "--auth-token" not in profile_ensure_help
    assert "--caller-project-id" not in profile_ensure_help
    assert "cg worker claim run --project-id <project_id> --agent-id <agent_id> -- ./worker-bin --flag" in worker_run_help

    agent_join_help = _parser_help(("agent", "join"))
    assert "join code" in agent_join_help
    assert "profile" in agent_join_help
    assert "AgentCredential token file" in agent_join_help
    assert "--auth-token" not in agent_join_help
    assert "--profile" not in agent_join_help
    assert "--admin-auth-token" not in agent_join_help
    assert "--caller-project-id" not in agent_join_help

    invite_help = _parser_help(("admission", "invite", "create"))
    assert "Admin Service bearer auth" in invite_help
    assert "cg agent join" in invite_help
    assert "--admin-auth-token" in invite_help
    assert "--auth-token" not in invite_help
    assert "--profile PROFILE" not in invite_help
    assert "--caller-project-id" not in invite_help

    report_help = _parser_help(("report", "work-memory"))
    assert "JSON object" in report_help
    assert "Top-level meta is rejected" in report_help
    assert "--ensure-profile" not in report_help
    assert "--admin-auth-token" not in report_help
    assert "--caller-project-id" not in report_help

    worker_once_help = _parser_help(("worker", "once"))
    assert "CG_CONTEXT_FILE" in worker_once_help
    assert "CG_FINAL_FILE" in worker_once_help
    assert "CG_SUSPEND_FILE" in worker_once_help
    assert "CG_FAILURE_FILE" in worker_once_help
    worker_loop_help = _parser_help(("worker", "loop"))
    assert "--idle-sleep-seconds" in worker_loop_help
    assert "--max-iterations" in worker_loop_help

    for claim_command in ("renew", "append", "finish", "dispatch-child", "suspend"):
        claim_help = _parser_help(("worker", "claim", claim_command))
        assert "matching AgentCredential" in claim_help
        assert "--claim-file" in claim_help
        assert "--admin-auth-token" not in claim_help

    setup_status_help = _parser_help(("setup", "project", "status"))
    setup_status_help_flat = " ".join(setup_status_help.split())
    assert "setup artifacts" in setup_status_help
    assert "not a live CommonGround service health check" in setup_status_help_flat

    smoke_help = _parser_help(("smoke", "pair"))
    assert "discovering the target offer" in smoke_help
    assert "dispatching a turn" in smoke_help
    assert "returning final context" in smoke_help
    assert "default payload" in smoke_help.lower()

    project_offer_help = _parser_help(("project", "offer", "list"))
    assert "--caller-project-id" in project_offer_help
    assert "--admin-auth-token" not in project_offer_help


@pytest.mark.parametrize(
    "legacy_args",
    [
        ["--auth-token", "agent-token"],
        ["--auth-token-file", "agent.token"],
        ["--profile", "demo/agent"],
        ["--admin-auth-token", "admin-token"],
        ["--admin-auth-token-file", "admin.token"],
        ["--caller-project-id", PROJECT_ID],
        ["--caller-agent-id", AGENT_ID],
    ],
)
def test_cg_agent_join_rejects_auth_profile_admin_and_caller_flags(legacy_args: list[str]) -> None:
    stdout = io.StringIO()
    exit_code = main(
        ["agent", "join", *legacy_args, "http://127.0.0.1:8000", "cgjoin_secret_001"],
        stdout=stdout,
        admin_client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("admin client should not be constructed")),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invalid_arguments"


def test_cg_admission_invite_create_rejects_agent_profile_and_caller_flags() -> None:
    rejected = [
        ["--profile", f"{PROJECT_ID}/{AGENT_ID}"],
        ["--auth-token", "agent-token"],
        ["--caller-project-id", PROJECT_ID],
    ]
    for legacy_args in rejected:
        stdout = io.StringIO()
        exit_code = main(
            [
                "admission",
                "invite",
                "create",
                *legacy_args,
                "--admin-auth-token",
                "admin-token",
                "--project-id",
                PROJECT_ID,
                "--agent-id",
                AGENT_ID,
            ],
            stdout=stdout,
            admin_client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("admin client should not be constructed")),
        )

        body = json.loads(stdout.getvalue())
        assert exit_code == 1
        assert body["error"]["code"] == "invalid_arguments"


def test_cg_profile_ensure_agent_rejects_agent_auth_and_caller_flags() -> None:
    rejected = [
        ["--auth-token", "agent-token"],
        ["--caller-project-id", PROJECT_ID],
    ]
    for legacy_args in rejected:
        stdout = io.StringIO()
        exit_code = main(
            [
                "profile",
                "ensure-agent",
                *legacy_args,
                "--profile",
                f"{PROJECT_ID}/{AGENT_ID}",
                "--admin-auth-token",
                "admin-token",
                "--project-id",
                PROJECT_ID,
                "--requested-agent-id",
                AGENT_ID,
                "--runtime-kind",
                "codex.local.v1",
                "--display-name",
                "Agent 001",
            ],
            stdout=stdout,
            admin_client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("admin client should not be constructed")),
        )

        body = json.loads(stdout.getvalue())
        assert exit_code == 1
        assert body["error"]["code"] == "invalid_arguments"


def test_cg_service_run_uses_local_service_entrypoint_without_json_envelope(monkeypatch) -> None:
    calls: list[str] = []

    def fake_service_main() -> None:
        calls.append("run")

    monkeypatch.setattr("CommonGround.cli.service_main", fake_service_main)
    out = io.StringIO()
    exit_code = main(["service", "run"], stdout=out, client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")))

    assert exit_code == 0
    assert calls == ["run"]
    assert out.getvalue() == ""


def test_cg_service_run_does_not_wrap_local_failures_in_json(monkeypatch) -> None:
    def fake_service_main() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr("CommonGround.cli.service_main", fake_service_main)

    with pytest.raises(RuntimeError, match="boom"):
        main(["service", "run"], stdout=io.StringIO(), client_factory=lambda **_: None)


def test_cg_local_run_mounts_service_and_admin_router_without_json_envelope(monkeypatch) -> None:
    calls: list[tuple[str, Any]] = []

    class FakeApp:
        def mount(self, path, app):
            calls.append(("mount", path, app))

    def fake_create_service_app(*, config):
        calls.append(("service_config", config))
        return FakeApp()

    def fake_create_admission_app(settings, *, prefix):
        calls.append(("admission_settings", settings, prefix))
        return "admin-app"

    def fake_uvicorn_run(app, *, host, port, log_level):
        calls.append(("uvicorn", app, host, port, log_level))

    monkeypatch.setattr("CommonGround.service.http.create_service_app", fake_create_service_app)
    monkeypatch.setattr("Integrations.admin_service.admission_runner.create_local_admission_app", fake_create_admission_app)
    monkeypatch.setattr("uvicorn.run", fake_uvicorn_run)
    out = io.StringIO()
    err = io.StringIO()

    exit_code = main(
        [
            "local",
            "run",
            "--pg-dsn",
            "postgresql://example/db",
            "--project-id",
            PROJECT_ID,
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ],
        stdout=out,
        stderr=err,
        client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")),
    )

    assert exit_code == 0
    assert out.getvalue() == ""
    assert calls[0][0] == "service_config"
    assert calls[0][1].pg_dsn == "postgresql://example/db"
    assert calls[1][0] == "admission_settings"
    assert calls[1][1].base_url == "http://127.0.0.1:8000"
    assert calls[1][2] == "/v1"
    assert calls[2] == ("mount", "/admin", "admin-app")
    assert calls[3][2:] == ("0.0.0.0", 8000, "info")
    assert "paths=/v3r1,/admin/v1" in err.getvalue()


def test_cg_setup_project_seed_creates_default_project_and_token_files(test_pg_dsn: str, kernel_app, tmp_path) -> None:
    del kernel_app
    admin_service_token_file = tmp_path / "admin-service.cgac"
    admin_auth_token_file = tmp_path / "admin-api.token"
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "seed",
            "--pg-dsn",
            test_pg_dsn,
            "--default-local",
            "--admin-service-token-file",
            str(admin_service_token_file),
            "--admin-auth-token-file",
            str(admin_auth_token_file),
        ],
        stdout=out,
        client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["ok"] is True
    result = body["result"]
    assert result["project_id"] == "cg-demo"
    assert result["seeded"] is True
    assert result["matches_bootstrap_spec"] is True
    assert result["admin_service_credential_ready"] is True
    assert result["admin_auth_ready"] is True
    assert result["admin_service"]["agent_id"] == ADMIN_SERVICE_AGENT_ID
    assert stat.S_IMODE(admin_service_token_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(admin_auth_token_file.stat().st_mode) == 0o600
    assert admin_service_token_file.read_text(encoding="utf-8").strip() not in out.getvalue()
    assert admin_auth_token_file.read_text(encoding="utf-8").strip() not in out.getvalue()


def test_cg_setup_project_seed_is_idempotent(test_pg_dsn: str, kernel_app, tmp_path) -> None:
    del kernel_app
    admin_service_token_file = tmp_path / "admin-service.cgac"
    admin_auth_token_file = tmp_path / "admin-api.token"
    args = [
        "setup",
        "project",
        "seed",
        "--pg-dsn",
        test_pg_dsn,
        "--project-id",
        PROJECT_ID,
        "--admin-service-token-file",
        str(admin_service_token_file),
        "--admin-auth-token-file",
        str(admin_auth_token_file),
    ]

    first = io.StringIO()
    second = io.StringIO()
    first_exit = main(args, stdout=first, client_factory=lambda **_: None)
    first_admin_service_token = admin_service_token_file.read_text(encoding="utf-8")
    first_admin_auth_token = admin_auth_token_file.read_text(encoding="utf-8")
    second_exit = main(args, stdout=second, client_factory=lambda **_: None)

    assert first_exit == 0
    assert second_exit == 0
    assert admin_service_token_file.read_text(encoding="utf-8") == first_admin_service_token
    assert admin_auth_token_file.read_text(encoding="utf-8") == first_admin_auth_token
    assert json.loads(second.getvalue())["result"]["admin_service_credential_ready"] is True


def test_cg_setup_project_status_uses_default_token_paths(test_pg_dsn: str, kernel_app, tmp_path, monkeypatch) -> None:
    del kernel_app
    monkeypatch.setattr("Integrations.admin_service.project_setup.DEFAULT_OPERATOR_DIR", tmp_path / "operator")
    seed_out = io.StringIO()
    status_out = io.StringIO()

    seed_code = main(
        ["setup", "project", "seed", "--pg-dsn", test_pg_dsn, "--default-local"],
        stdout=seed_out,
        client_factory=lambda **_: None,
    )
    status_code = main(
        ["setup", "project", "status", "--pg-dsn", test_pg_dsn, "--default-local"],
        stdout=status_out,
        client_factory=lambda **_: None,
    )

    assert seed_code == 0
    assert status_code == 0
    result = json.loads(status_out.getvalue())["result"]
    assert result["admin_service_credential_ready"] is True
    assert result["admin_auth_ready"] is True
    assert result["admin_service_token_file"].endswith("cg-demo/admin-service.cgac")
    assert result["admin_auth_token_file"].endswith("cg-demo/admin-api-bearer.token")


def test_cg_setup_project_client_config_writes_cli_connection_config(tmp_path) -> None:
    admin_auth_token_file = tmp_path / "admin-api.token"
    admin_auth_token_file.write_text("admin-secret\n", encoding="utf-8")
    admin_auth_token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "admin_auth": {"token": "stale-inline-secret"},
                "profiles": {
                    "demo/existing": {
                        "project_id": "demo",
                        "agent_id": "existing",
                        "profile_kind": "byoa.work_memory_reporter.v1",
                        "runtime_kind": "codex.local.v1",
                        "display_name": "Existing Agent",
                        "credential_id": "cred-existing",
                        "token_file": "/tmp/existing.token",
                        "status": "ready",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "client-config",
            "--project-id",
            PROJECT_ID,
            "--base-url",
            "http://cg.example",
            "--admin-base-url",
            "http://admin.example",
            "--admin-auth-token-file",
            str(admin_auth_token_file),
            "--config",
            str(config_path),
        ],
        stdout=out,
        client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["ok"] is True
    assert body["result"] == {
        "config_path": str(config_path),
        "base_url": "http://cg.example",
        "admin_base_url": "http://admin.example",
        "admin_auth_token_file": str(admin_auth_token_file),
        "project_id": PROJECT_ID,
    }
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["base_url"] == "http://cg.example"
    assert payload["admin_base_url"] == "http://admin.example"
    assert payload["admin_auth"] == {"token_file": str(admin_auth_token_file)}
    assert "demo/existing" in payload["profiles"]
    assert "admin-secret" not in out.getvalue()
    assert "stale-inline-secret" not in config_path.read_text(encoding="utf-8")


def test_cg_setup_project_client_config_rejects_unsafe_admin_auth_file(tmp_path) -> None:
    admin_auth_token_file = tmp_path / "admin-api.token"
    admin_auth_token_file.write_text("admin-secret\n", encoding="utf-8")
    admin_auth_token_file.chmod(0o644)
    config_path = tmp_path / "config.json"
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "client-config",
            "--admin-auth-token-file",
            str(admin_auth_token_file),
            "--config",
            str(config_path),
        ],
        stdout=out,
        client_factory=lambda **_: None,
    )

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "admin_auth_token_file_invalid"
    assert "admin-secret" not in out.getvalue()
    assert not config_path.exists()


def test_cg_admission_invite_create_outputs_generic_join_command(tmp_path, monkeypatch) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "admin_auth": {"token": "admin-secret"},
                "admin_base_url": "http://admin.example",
                "auth": {"token_file": str(tmp_path / "missing-agent.token")},
                "caller": {"project_id": PROJECT_ID, "agent_id": "existing-agent"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN_FILE", str(tmp_path / "missing-env-agent.token"))
    monkeypatch.setenv("CG_CALLER_PROJECT_ID", "stale-env-project")
    monkeypatch.setenv("CG_CALLER_AGENT_ID", "stale-env-agent")
    out_path = tmp_path / "agent.join.json"
    admin_calls: list[dict[str, Any]] = []
    stdout = io.StringIO()

    exit_code = main(
        [
            "admission",
            "invite",
            "create",
            "--config",
            str(config_path),
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--join-base-url",
            "http://10.0.0.10:8000",
            "--out",
            str(out_path),
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeInviteCreateClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["result"]["join_command"] == "cg agent join http://10.0.0.10:8000 cgjoin_secret_001"
    assert body["result"]["canonical_command"] == "cg agent join"
    assert admin_calls[0]["path"] == f"/admin/v1/projects/{PROJECT_ID}/agent-join-invites"
    assert admin_calls[0]["headers"] == {"Authorization": "Bearer admin-secret"}
    assert admin_calls[0]["json"]["profile_kind"] == "byoa.conversation_worker.v1"
    assert admin_calls[0]["json"]["runtime_kind"] == "manual.shell.v1"
    assert admin_calls[0]["json"]["expires_in_seconds"] == 24 * 60 * 60
    assert "admin-secret" not in stdout.getvalue()
    assert json.loads(out_path.read_text(encoding="utf-8"))["join_command"] == body["result"]["join_command"]
    assert stat.S_IMODE(out_path.stat().st_mode) == stat.S_IRUSR | stat.S_IWUSR


def test_cg_admission_invite_create_uses_explicit_join_urls_for_split_deployment(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "base_url": "http://cg.example",
                "admin_base_url": "http://admin.example",
                "admin_auth": {"token": "admin-secret"},
            }
        ),
        encoding="utf-8",
    )
    admin_calls: list[dict[str, Any]] = []
    stdout = io.StringIO()

    exit_code = main(
        [
            "admission",
            "invite",
            "create",
            "--config",
            str(config_path),
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeInviteCreateClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["result"]["join_command"] == (
        "cg agent join --base-url http://cg.example --admin-base-url http://admin.example --join-code cgjoin_secret_001"
    )


def test_cg_agent_join_redeems_code_and_writes_profile_without_admin_token(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    admin_calls: list[dict[str, Any]] = []
    stdout = io.StringIO()

    exit_code = main(
        [
            "agent",
            "join",
            "--config",
            str(config_path),
            "http://10.0.0.10:8000",
            "cgjoin_secret_001",
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeJoinRedeemClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["result"]["profile"] == f"{PROJECT_ID}/{AGENT_ID}"
    assert body["result"]["base_url"] == "http://10.0.0.10:8000"
    assert "cgjoin_secret_001" not in stdout.getvalue()
    assert "cgac_cred-join-001.secret" not in stdout.getvalue()
    assert admin_calls[0]["path"] == "/admin/v1/agent-joins:redeem"
    assert admin_calls[0]["headers"] is None
    assert admin_calls[0]["json"] == {"join_code": "cgjoin_secret_001"}
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert config["base_url"] == "http://10.0.0.10:8000"
    assert config["admin_base_url"] == "http://10.0.0.10:8000"
    assert "admin_auth" not in config
    profile = config["profiles"][f"{PROJECT_ID}/{AGENT_ID}"]
    token_file = Path(profile["token_file"])
    assert stat.S_IMODE(token_file.stat().st_mode) == stat.S_IRUSR | stat.S_IWUSR
    assert token_file.read_text(encoding="utf-8").strip() == "cgac_cred-join-001.secret"


def test_cg_agent_join_ignores_stale_auth_files_in_existing_config(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN_FILE", str(tmp_path / "missing-env-agent.token"))
    monkeypatch.setenv("CG_ADMIN_AUTH_TOKEN_FILE", str(tmp_path / "missing-env-admin.token"))
    monkeypatch.setenv("CG_CALLER_PROJECT_ID", "stale-env-project")
    monkeypatch.setenv("CG_CALLER_AGENT_ID", "stale-env-agent")
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "auth": {"token_file": str(tmp_path / "missing-agent.token")},
                "admin_auth": {"token_file": str(tmp_path / "missing-admin.token")},
                "caller": {"project_id": PROJECT_ID, "agent_id": "existing-agent"},
            }
        ),
        encoding="utf-8",
    )
    admin_calls: list[dict[str, Any]] = []
    stdout = io.StringIO()

    exit_code = main(
        [
            "agent",
            "join",
            "--config",
            str(config_path),
            "http://10.0.0.10:8000",
            "cgjoin_secret_001",
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeJoinRedeemClient(admin_calls, base_url=kwargs["base_url"]),
    )

    assert exit_code == 0
    assert admin_calls[0]["json"] == {"join_code": "cgjoin_secret_001"}


def test_cg_setup_project_ignores_runtime_agent_auth_env(test_pg_dsn: str, kernel_app, tmp_path, monkeypatch) -> None:
    del kernel_app
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN_FILE", str(tmp_path / "missing-agent-token"))
    monkeypatch.setenv("CG_ADMIN_AUTH_TOKEN_FILE", str(tmp_path / "missing-admin-token"))
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "seed",
            "--pg-dsn",
            test_pg_dsn,
            "--project-id",
            "operator-isolated",
            "--admin-service-token-file",
            str(tmp_path / "admin-service.cgac"),
            "--admin-auth-token-file",
            str(tmp_path / "admin-api.token"),
        ],
        stdout=out,
        client_factory=lambda **_: None,
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["result"]["project_id"] == "operator-isolated"


def test_cg_setup_project_rotate_revokes_previous_admin_service_credential(test_pg_dsn: str, kernel_app, tmp_path) -> None:
    del kernel_app
    admin_service_token_file = tmp_path / "admin-service.cgac"
    args = [
        "setup",
        "project",
        "seed",
        "--pg-dsn",
        test_pg_dsn,
        "--project-id",
        PROJECT_ID,
        "--admin-service-token-file",
        str(admin_service_token_file),
    ]

    first_out = io.StringIO()
    second_out = io.StringIO()
    assert main(args, stdout=first_out, client_factory=lambda **_: None) == 0
    first_credential_id = parse_agent_credential_token(admin_service_token_file.read_text(encoding="utf-8").strip()).credential_id
    assert main([*args, "--rotate-admin-service-token"], stdout=second_out, client_factory=lambda **_: None) == 0
    second_credential_id = parse_agent_credential_token(admin_service_token_file.read_text(encoding="utf-8").strip()).credential_id

    store = PostgresAgentCredentialStore(test_pg_dsn)
    assert first_credential_id != second_credential_id
    assert store.load_agent_credential_by_id(first_credential_id).status == "revoked"
    assert store.load_agent_credential_by_id(second_credential_id).status == "active"


def test_cg_setup_project_reports_admin_auth_token_file_invalid(test_pg_dsn: str, kernel_app, tmp_path) -> None:
    del kernel_app
    admin_auth_token_file = tmp_path / "admin-api.token"
    admin_auth_token_file.write_text("admin-secret\n", encoding="utf-8")
    admin_auth_token_file.chmod(0o644)
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "seed",
            "--pg-dsn",
            test_pg_dsn,
            "--project-id",
            PROJECT_ID,
            "--admin-service-token-file",
            str(tmp_path / "admin-service.cgac"),
            "--admin-auth-token-file",
            str(admin_auth_token_file),
        ],
        stdout=out,
        client_factory=lambda **_: None,
    )

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "admin_auth_token_file_invalid"
    assert "admin-secret" not in out.getvalue()


def test_cg_setup_project_seed_reports_bootstrap_conflict(test_pg_dsn: str, kernel_app, tmp_path) -> None:
    admin_service = AgentRef(project_id=PROJECT_ID, agent_id=ADMIN_SERVICE_AGENT_ID)
    kernel_app.topology.register_agent(
        admin_service,
        role="custom.admin.service",
        capabilities=("custom.capability",),
        accepts_work=True,
        grants=(),
        enabled=True,
    )
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "seed",
            "--pg-dsn",
            test_pg_dsn,
            "--project-id",
            PROJECT_ID,
            "--admin-service-token-file",
            str(tmp_path / "admin-service.cgac"),
        ],
        stdout=out,
        client_factory=lambda **_: None,
    )

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "project_bootstrap_conflict"
    assert body["error"]["status"] == 409


def test_cg_setup_project_status_reports_unseeded_project(test_pg_dsn: str, kernel_app, tmp_path) -> None:
    del kernel_app
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "status",
            "--pg-dsn",
            test_pg_dsn,
            "--project-id",
            "missing-project",
            "--admin-service-token-file",
            str(tmp_path / "admin-service.cgac"),
        ],
        stdout=out,
        client_factory=lambda **_: None,
    )

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["ok"] is False
    assert body["error"]["code"] == "project_not_seeded"
    assert body["error"]["status"] == 404


def test_cg_setup_project_requires_pg_dsn(monkeypatch) -> None:
    monkeypatch.delenv("PG_DSN", raising=False)
    out = io.StringIO()

    exit_code = main(["setup", "project", "status", "--default-local"], stdout=out, client_factory=lambda **_: None)

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "setup_pg_dsn_required"


def test_cg_setup_project_reports_pg_unavailable(monkeypatch) -> None:
    monkeypatch.setenv("PG_DSN", "postgresql://user:secret@127.0.0.1:1/db")

    def _raise_operational_error(**_):
        raise psycopg.OperationalError("connection failed: secret")

    monkeypatch.setattr("CommonGround.cli.project_status", _raise_operational_error)
    out = io.StringIO()

    exit_code = main(["setup", "project", "status", "--default-local"], stdout=out, client_factory=lambda **_: None)

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "setup_pg_unavailable"
    assert body["error"]["status"] == 503
    assert "secret" not in out.getvalue()


def test_cg_setup_project_rejects_invalid_project_id(test_pg_dsn: str, kernel_app, tmp_path) -> None:
    del kernel_app
    out = io.StringIO()

    exit_code = main(
        [
            "setup",
            "project",
            "seed",
            "--pg-dsn",
            test_pg_dsn,
            "--project-id",
            "../bad",
            "--admin-service-token-file",
            str(tmp_path / "admin-service.cgac"),
        ],
        stdout=out,
        client_factory=lambda **_: None,
    )

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invalid_arguments"
    assert "path separators" in body["error"]["message"]


@dataclass(frozen=True)
class _FakeContent:
    data: Any

    def payload(self) -> Any:
        return self.data


class _FakeAdminClient:
    def __init__(self, calls: list[dict[str, Any]], *, base_url: str, credential_id: str = "cred-001") -> None:
        self._calls = calls
        self._base_url = base_url.rstrip("/")
        self._credential_id = credential_id

    def post(self, path: str, *, headers: dict[str, str], json: dict[str, Any]):
        self._calls.append({"path": path, "headers": headers, "json": json, "base_url": self._base_url})
        request = httpx.Request("POST", f"{self._base_url}{path}")
        return httpx.Response(
            200,
            request=request,
            json={
                "request_id": json["request_id"],
                "project_id": PROJECT_ID,
                "agent_id": AGENT_ID,
                "status": "registered",
                "profile": {
                    "project_id": PROJECT_ID,
                    "agent_id": AGENT_ID,
                    "runtime_kind": json["runtime_kind"],
                    "profile_kind": json["profile_kind"],
                    "credential_id": self._credential_id,
                    "status": "credential_ready",
                },
                "credential": {"credential_id": self._credential_id, "status": "active"},
                "agent_credential_token": f"cgac_{self._credential_id}.secret",
            },
        )


class _FakeAdminAuthFailureClient:
    def __init__(self, calls: list[dict[str, Any]], *, base_url: str, status_code: int = 401) -> None:
        self._calls = calls
        self._base_url = base_url.rstrip("/")
        self._status_code = status_code

    def post(self, path: str, *, headers: dict[str, str], json: dict[str, Any]):
        self._calls.append({"path": path, "headers": headers, "json": json, "base_url": self._base_url})
        request = httpx.Request("POST", f"{self._base_url}{path}")
        return httpx.Response(
            self._status_code,
            request=request,
            json={"error": "UnauthorizedError", "message": "product auth denied"},
        )


class _FakeAdminStableErrorClient:
    def __init__(self, calls: list[dict[str, Any]], *, base_url: str, code: str, message: str, status_code: int) -> None:
        self._calls = calls
        self._base_url = base_url.rstrip("/")
        self._code = code
        self._message = message
        self._status_code = status_code

    def post(self, path: str, *, headers: dict[str, str], json: dict[str, Any]):
        self._calls.append({"path": path, "headers": headers, "json": json, "base_url": self._base_url})
        request = httpx.Request("POST", f"{self._base_url}{path}")
        return httpx.Response(
            self._status_code,
            request=request,
            json={"error": "NotFoundError", "code": self._code, "message": self._message},
        )


class _FakeJoinRedeemClient:
    def __init__(self, calls: list[dict[str, Any]], *, base_url: str, credential_id: str = "cred-join-001") -> None:
        self._calls = calls
        self._base_url = base_url.rstrip("/")
        self._credential_id = credential_id

    def post(self, path: str, *, json: dict[str, Any], headers: dict[str, str] | None = None):
        self._calls.append({"path": path, "headers": headers, "json": json, "base_url": self._base_url})
        request = httpx.Request("POST", f"{self._base_url}{path}")
        return httpx.Response(
            200,
            request=request,
            json={
                "request_id": "agjoinreq_001",
                "project_id": PROJECT_ID,
                "agent_id": AGENT_ID,
                "status": "registered",
                "profile": {
                    "project_id": PROJECT_ID,
                    "agent_id": AGENT_ID,
                    "runtime_kind": "manual.shell.v1",
                    "profile_kind": "byoa.conversation_worker.v1",
                    "credential_id": self._credential_id,
                    "status": "credential_ready",
                },
                "credential": {"credential_id": self._credential_id, "status": "active"},
                "agent_credential_token": f"cgac_{self._credential_id}.secret",
            },
        )


class _FakeInviteCreateClient:
    def __init__(self, calls: list[dict[str, Any]], *, base_url: str) -> None:
        self._calls = calls
        self._base_url = base_url.rstrip("/")

    def post(self, path: str, *, headers: dict[str, str], json: dict[str, Any]):
        self._calls.append({"path": path, "headers": headers, "json": json, "base_url": self._base_url})
        request = httpx.Request("POST", f"{self._base_url}{path}")
        return httpx.Response(
            200,
            request=request,
            json={
                "invite": {
                    "invite_id": "aginv_001",
                    "project_id": PROJECT_ID,
                    "agent_id": json["agent_id"],
                    "profile_kind": json["profile_kind"],
                    "runtime_kind": json["runtime_kind"],
                    "display_name": json["display_name"] or "Agent 001",
                    "expires_at": "2026-05-16T00:00:00+00:00",
                    "single_use": json["single_use"],
                    "max_uses": json["max_uses"],
                    "use_count": 0,
                    "disabled": False,
                    "issued_by_user_id": "operator",
                    "created_at": "2026-05-15T00:00:00+00:00",
                    "updated_at": "2026-05-15T00:00:00+00:00",
                    "last_redeemed_at": None,
                    "description": json["description"],
                },
                "join_code": "cgjoin_secret_001",
            },
        )


def _write_profile_config(config_path, *, token_file) -> None:
    config_path.write_text(
        json.dumps(
            {
                "profiles": {
                    f"{PROJECT_ID}/{AGENT_ID}": {
                        "project_id": PROJECT_ID,
                        "agent_id": AGENT_ID,
                        "profile_kind": "byoa.work_memory_reporter.v1",
                        "runtime_kind": "codex.local.v1",
                        "display_name": "Agent 001",
                        "credential_id": "cred-existing",
                        "token_file": str(token_file),
                        "status": "ready",
                    }
                }
            }
        ),
        encoding="utf-8",
    )


def test_cg_dispatch_reads_payload_file_and_returns_json(tmp_path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"task": "hello"}), encoding="utf-8")
    calls: list[dict[str, Any]] = []
    factory_calls: list[dict[str, Any]] = []

    class FakeClient:
        def dispatch(self, **kwargs):
            calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--base-url",
            "http://cg.example",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--target-agent",
            AGENT_ID,
            "--turn-kind",
            "turn.conversation.v1",
            "--request-id",
            "req-1",
            "--dispatch-key",
            "dispatch-1",
            "--payload-file",
            str(payload_path),
        ],
        stdout=stdout,
        client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeClient(),
    )

    assert exit_code == 0
    assert json.loads(stdout.getvalue()) == {
        "ok": True,
        "result": {
            "project_id": PROJECT_ID,
            "turn_id": TURN_ID,
            "agent_id": AGENT_ID,
            "request_id": "req-1",
            "dispatch_key": "dispatch-1",
        },
    }
    assert calls == [
        {
            "requested_by": AgentRef(project_id=PROJECT_ID, agent_id="frontside"),
            "target_agent": AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID),
            "input_payload": {"task": "hello"},
            "authority": DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="req-1"),
            "dispatch_key": "dispatch-1",
            "turn_kind": "turn.conversation.v1",
        }
    ]
    assert factory_calls == [
        {
            "base_url": "http://cg.example",
            "auth_token": "test-token",
            "headers": {
                "X-CG-Project-Id": PROJECT_ID,
                "X-CG-Agent-Id": "frontside",
                "Authorization": "Bearer test-token",
            },
        }
    ]


def test_cg_dispatch_accepts_inline_payload_json() -> None:
    calls: list[dict[str, Any]] = []

    class FakeClient:
        def dispatch(self, **kwargs):
            calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--target-agent",
            AGENT_ID,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--request-id",
            " req-json ",
            "--payload-json",
            '{"task":"inline"}',
        ],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    body = json.loads(stdout.getvalue())
    assert body["result"]["request_id"] == "req-json"
    assert body["result"]["dispatch_key"] == "req-json"
    assert calls[0]["input_payload"] == {"task": "inline"}
    assert calls[0]["authority"] == DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="req-json")
    assert calls[0]["dispatch_key"] == "req-json"


def test_cg_dispatch_accepts_payload_stdin(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr("sys.stdin", io.StringIO('{"task":"stdin"}'))

    class FakeClient:
        def dispatch(self, **kwargs):
            calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--target-agent",
            AGENT_ID,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--dispatch-key",
            "stdin-key",
            "--payload-stdin",
        ],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    assert calls[0]["input_payload"] == {"task": "stdin"}
    assert calls[0]["authority"] == DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="stdin-key")
    assert calls[0]["dispatch_key"] == "stdin-key"


def test_cg_dispatch_rejects_invalid_idempotency_anchor() -> None:
    stdout = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--target-agent",
            AGENT_ID,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--request-id",
            "bad key",
            "--payload-json",
            '{"task":"hello"}',
        ],
        stdout=stdout,
        client_factory=lambda **_: None,
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invalid_arguments"
    assert body["error"]["status"] == 2
    assert "request_id must start with a letter or digit" in body["error"]["message"]


def test_cg_report_work_memory_reads_manifest_and_returns_refs(tmp_path) -> None:
    manifest_path = tmp_path / "report.json"
    manifest = {
        "kind": "agent_work_memory_report_manifest.v1",
        "request_id": "report-1",
        "records": [{"role": "local_experience_summary", "payload": {"summary": "done"}}],
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    calls: list[dict[str, Any]] = []
    factory_calls: list[dict[str, Any]] = []

    class FakeClient:
        def submit_work_memory_report(self, actor: AgentRef, manifest: dict[str, Any]):
            calls.append({"actor": actor, "manifest": manifest})
            return WorkMemoryReportSubmissionResult(
                status="submitted",
                turn=TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID),
                record_refs=(SemanticRecordRef(project_id=PROJECT_ID, record_id="record-1"),),
                final_payload={"summary": "saved"},
            )

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--base-url",
            "http://cg.example",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeClient(),
    )

    assert exit_code == 0
    assert json.loads(stdout.getvalue()) == {
        "ok": True,
        "result": {
            "status": "submitted",
            "turn": {"project_id": PROJECT_ID, "turn_id": TURN_ID},
            "record_refs": [{"project_id": PROJECT_ID, "record_id": "record-1"}],
            "final_payload": {"summary": "saved"},
        },
    }
    assert calls == [{"actor": AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID), "manifest": manifest}]
    assert factory_calls == [
        {
            "base_url": "http://cg.example",
            "auth_token": "test-token",
            "headers": {
                "X-CG-Project-Id": PROJECT_ID,
                "X-CG-Agent-Id": AGENT_ID,
                "Authorization": "Bearer test-token",
            },
        }
    ]


def test_cg_profile_ensure_agent_bootstraps_profile_without_echoing_token(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN_FILE", str(tmp_path / "missing-env-agent.token"))
    monkeypatch.setenv("CG_CALLER_PROJECT_ID", "stale-env-project")
    monkeypatch.setenv("CG_CALLER_AGENT_ID", "stale-env-agent")
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "admin_auth": {"token": "admin-secret"},
                "admin_base_url": "http://admin.example",
                "auth": {"token_file": str(tmp_path / "missing-agent.token")},
            }
        ),
        encoding="utf-8",
    )
    admin_calls: list[dict[str, Any]] = []

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Agent 001",
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeAdminClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body == {
        "ok": True,
        "result": {
            "profile": f"{PROJECT_ID}/{AGENT_ID}",
            "project_id": PROJECT_ID,
            "agent_id": AGENT_ID,
            "credential_id": "cred-001",
            "status": "ready",
        },
    }
    assert "cgac_cred-001.secret" not in stdout.getvalue()
    assert "admin-secret" not in stdout.getvalue()
    assert admin_calls[0]["headers"] == {"Authorization": "Bearer admin-secret"}
    assert "invitation_code" not in admin_calls[0]["json"]
    config = json.loads(config_path.read_text(encoding="utf-8"))
    token_file = config["profiles"][f"{PROJECT_ID}/{AGENT_ID}"]["token_file"]
    assert stat.S_IMODE(Path(token_file).stat().st_mode) == stat.S_IRUSR | stat.S_IWUSR
    assert Path(token_file).read_text(encoding="utf-8").strip() == "cgac_cred-001.secret"


def test_cg_profile_ensure_agent_forwards_invitation_code_file_without_echoing_secret(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"admin_auth": {"token": "admin-secret"}, "admin_base_url": "http://admin.example"}), encoding="utf-8")
    invitation_code_file = tmp_path / "invite-code.txt"
    invitation_code_file.write_text("invite-secret-001\n", encoding="utf-8")
    admin_calls: list[dict[str, Any]] = []

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--profile-kind",
            "byoa.conversation_worker.v1",
            "--runtime-kind",
            "external-runtime.v1",
            "--display-name",
            "Worker 001",
            "--invitation-code-file",
            str(invitation_code_file),
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeAdminClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["ok"] is True
    assert admin_calls[0]["json"]["profile_kind"] == "byoa.conversation_worker.v1"
    assert admin_calls[0]["json"]["invitation_code"] == "invite-secret-001"
    assert "invite-secret-001" not in stdout.getvalue()
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert config["profiles"][f"{PROJECT_ID}/{AGENT_ID}"]["profile_kind"] == "byoa.conversation_worker.v1"


def test_cg_profile_ensure_agent_forwards_inline_invitation_code_without_echoing_secret(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"admin_auth": {"token": "admin-secret"}, "admin_base_url": "http://admin.example"}), encoding="utf-8")
    admin_calls: list[dict[str, Any]] = []

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--profile-kind",
            "byoa.conversation_worker.v1",
            "--runtime-kind",
            "external-runtime.v1",
            "--display-name",
            "Worker 001",
            "--invitation-code",
            "invite-inline-secret-001",
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeAdminClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["ok"] is True
    assert admin_calls[0]["json"]["invitation_code"] == "invite-inline-secret-001"
    assert "invite-inline-secret-001" not in stdout.getvalue()


def test_cg_profile_ensure_agent_preserves_missing_invitation_code_error(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"admin_auth": {"token": "admin-secret"}, "admin_base_url": "http://admin.example"}), encoding="utf-8")
    admin_calls: list[dict[str, Any]] = []

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--profile-kind",
            "byoa.conversation_worker.v1",
            "--runtime-kind",
            "external-runtime.v1",
            "--display-name",
            "Worker 001",
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeAdminStableErrorClient(
            admin_calls,
            base_url=kwargs["base_url"],
            code="invitation_code_required",
            message="BYOA conversation worker profile requires an invitation_code",
            status_code=403,
        ),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invitation_code_required"
    assert body["error"]["status"] == 403
    assert "invitation_code" not in admin_calls[0]["json"]


def test_cg_profile_ensure_agent_preserves_invalid_invitation_code_error_without_echoing_secret(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"admin_auth": {"token": "admin-secret"}, "admin_base_url": "http://admin.example"}), encoding="utf-8")
    admin_calls: list[dict[str, Any]] = []

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--profile-kind",
            "byoa.conversation_worker.v1",
            "--runtime-kind",
            "external-runtime.v1",
            "--display-name",
            "Worker 001",
            "--invitation-code",
            "bad-invite-secret",
        ],
        stdout=stdout,
        admin_client_factory=lambda **kwargs: _FakeAdminStableErrorClient(
            admin_calls,
            base_url=kwargs["base_url"],
            code="invitation_code_invalid",
            message="BYOA invitation code is invalid",
            status_code=403,
        ),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invitation_code_invalid"
    assert body["error"]["status"] == 403
    assert admin_calls[0]["json"]["invitation_code"] == "bad-invite-secret"
    assert "bad-invite-secret" not in stdout.getvalue()


def test_cg_report_work_memory_rejects_removed_ensure_profile_flag(tmp_path) -> None:
    manifest_path = tmp_path / "report.json"
    manifest = {"request_id": "report-ensure-1", "records": [{"role": "summary", "payload": {"summary": "done"}}]}
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--ensure-profile",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invalid_arguments"
    assert "ensure-profile" in body["error"]["message"]


def test_cg_report_work_memory_rejects_root_admin_and_caller_surface(tmp_path) -> None:
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-root-surface", "records": []}), encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "--admin-auth-token",
            "admin-token",
            "--caller-project-id",
            PROJECT_ID,
            "report",
            "work-memory",
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invalid_arguments"
    assert "invalid choice" in body["error"]["message"] or "unrecognized arguments" in body["error"]["message"]


def test_cg_report_work_memory_rejects_command_admin_and_caller_surface(tmp_path) -> None:
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-command-surface", "records": []}), encoding="utf-8")

    rejected = [
        ["--admin-auth-token", "admin-token"],
        ["--caller-project-id", PROJECT_ID],
    ]
    for legacy_args in rejected:
        stdout = io.StringIO()
        exit_code = main(
            [
                "report",
                "work-memory",
                *legacy_args,
                "--profile",
                f"{PROJECT_ID}/{AGENT_ID}",
                "--project-id",
                PROJECT_ID,
                "--agent-id",
                AGENT_ID,
                "--manifest-file",
                str(manifest_path),
            ],
            stdout=stdout,
            client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")),
        )

        body = json.loads(stdout.getvalue())
        assert exit_code == 1
        assert body["error"]["code"] == "invalid_arguments"
        assert "unrecognized arguments" in body["error"]["message"]


def test_cg_read_and_actor_commands_reject_wrong_surfaces() -> None:
    cases = [
        [
            "--admin-auth-token",
            "admin-token",
            "project",
            "offer",
            "list",
            "--project-id",
            PROJECT_ID,
        ],
        [
            "project",
            "offer",
            "list",
            "--admin-auth-token",
            "admin-token",
            "--project-id",
            PROJECT_ID,
        ],
        [
            "dispatch",
            "--caller-project-id",
            PROJECT_ID,
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            AGENT_ID,
            "--target-agent",
            "worker",
            "--turn-kind",
            "turn.conversation.v1",
            "--request-id",
            "req-1",
            "--payload-json",
            "{}",
        ],
    ]

    for argv in cases:
        stdout = io.StringIO()
        exit_code = main(
            argv,
            stdout=stdout,
            client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("client should not be constructed")),
            projection_client_factory=lambda **_: (_ for _ in ()).throw(AssertionError("projection client should not be constructed")),
        )

        body = json.loads(stdout.getvalue())
        assert exit_code == 1
        assert body["error"]["code"] == "invalid_arguments"


def test_cg_report_work_memory_ignores_stale_caller_and_admin_env(tmp_path, monkeypatch) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-ignore-stale-env", "records": []}), encoding="utf-8")
    monkeypatch.setenv("CG_CALLER_PROJECT_ID", "stale-project")
    monkeypatch.setenv("CG_CALLER_AGENT_ID", "stale-agent")
    monkeypatch.setenv("CG_ADMIN_AUTH_TOKEN_FILE", str(tmp_path / "missing-admin.token"))
    factory_calls: list[dict[str, Any]] = []

    class FakeClient:
        def submit_work_memory_report(self, actor: AgentRef, manifest: dict[str, Any]):
            return WorkMemoryReportSubmissionResult(
                status="submitted",
                turn=TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID),
                record_refs=(),
                final_payload=None,
            )

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeClient(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["ok"] is True
    assert factory_calls[0]["headers"]["X-CG-Project-Id"] == PROJECT_ID
    assert factory_calls[0]["headers"]["X-CG-Agent-Id"] == AGENT_ID


def test_cg_report_work_memory_profile_reuse_prefers_profile_token_over_env(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-profile-1", "records": []}), encoding="utf-8")
    factory_calls: list[dict[str, Any]] = []

    class FakeClient:
        def submit_work_memory_report(self, actor: AgentRef, manifest: dict[str, Any]):
            return WorkMemoryReportSubmissionResult(
                status="submitted",
                turn=TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID),
                record_refs=(),
                final_payload=None,
            )

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeClient(),
    )

    assert exit_code == 0
    assert factory_calls[0]["headers"]["Authorization"] == "Bearer profile-token"


def test_cg_dispatch_uses_inferred_profile_token(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    factory_calls: list[dict[str, Any]] = []
    dispatch_calls: list[dict[str, Any]] = []

    class FakeClient:
        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--config",
            str(config_path),
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            AGENT_ID,
            "--target-agent",
            "nanobot",
            "--turn-kind",
            "turn.conversation.v1",
            "--request-id",
            "dispatch-profile-1",
            "--payload-json",
            "{}",
        ],
        stdout=stdout,
        client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeClient(),
    )

    assert exit_code == 0
    assert factory_calls[0]["auth_token"] == "profile-token"
    assert factory_calls[0]["headers"]["Authorization"] == "Bearer profile-token"
    assert factory_calls[0]["headers"]["X-CG-Project-Id"] == PROJECT_ID
    assert factory_calls[0]["headers"]["X-CG-Agent-Id"] == AGENT_ID
    assert dispatch_calls[0]["requested_by"] == AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)


def test_cg_command_profile_flag_is_not_overwritten_by_subparser_defaults(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-global-profile", "records": []}), encoding="utf-8")
    factory_calls: list[dict[str, Any]] = []

    class FakeClient:
        def submit_work_memory_report(self, actor: AgentRef, manifest: dict[str, Any]):
            return WorkMemoryReportSubmissionResult(
                status="submitted",
                turn=TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID),
                record_refs=(),
                final_payload=None,
            )

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeClient(),
    )

    assert exit_code == 0
    assert factory_calls[0]["headers"]["Authorization"] == "Bearer profile-token"


def test_cg_profile_ensure_agent_rejects_existing_profile_actor_mismatch(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            "other-agent",
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Other Agent",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invalid_arguments"
    assert body["error"]["message"] == "profile does not match requested agent"


def test_cg_explicit_profile_with_empty_token_does_not_fallback_to_env(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-empty-profile-token", "records": []}), encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "profile_stale"
    assert "empty" in body["error"]["message"]


def test_cg_explicit_profile_rejects_insecure_token_file_permissions(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o644)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-insecure-token", "records": []}), encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "profile_token_permissions"
    assert "0600" in body["error"]["message"]


def test_cg_inferred_actor_without_profile_or_env_token_returns_profile_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CG_AGENT_CREDENTIAL_TOKEN", raising=False)
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-missing-profile", "records": []}), encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "profile_missing"
    assert body["error"]["message"] == f"CLI profile not found: {PROJECT_ID}/{AGENT_ID}"


def test_cg_project_read_can_use_explicit_profile_with_caller_identity(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    factory_calls: list[dict[str, Any]] = []

    class FakeProjectionClient:
        def list_agents(self, **kwargs):
            return {
                "project_id": PROJECT_ID,
                "items": [],
                "limit": kwargs["limit"],
            }

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "project",
            "agent",
            "list",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            AGENT_ID,
            "--project-id",
            PROJECT_ID,
        ],
        stdout=stdout,
        projection_client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeProjectionClient(),
    )

    assert exit_code == 0
    assert factory_calls[0]["headers"]["Authorization"] == "Bearer profile-token"


def test_cg_report_work_memory_rejects_profile_actor_mismatch(tmp_path) -> None:
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "report-profile-mismatch", "records": []}), encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--profile",
            "demo/other-agent",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "invalid_arguments"
    assert body["error"]["message"] == "profile does not match command actor"


def test_cg_profile_ensure_agent_requires_admin_auth(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Agent 001",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "profile_auth_required"
    assert body["error"]["status"] == 401


def test_cg_profile_ensure_agent_maps_admin_auth_failure(tmp_path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    admin_calls: list[dict[str, Any]] = []

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--admin-auth-token",
            "bad-admin-token",
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Agent 001",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
        admin_client_factory=lambda **kwargs: _FakeAdminAuthFailureClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "profile_auth_required"
    assert body["error"]["message"] == "product auth denied"
    assert admin_calls[0]["headers"] == {"Authorization": "Bearer bad-admin-token"}


@pytest.mark.parametrize(
    ("code", "status_code", "message"),
    [
        ("project_not_seeded", 404, "project is not seeded: demo"),
        ("project_bootstrap_conflict", 409, "project admin-service bootstrap conflict: role"),
        ("admin_service_credential_required", 409, "admin-service AgentCredential is required"),
    ],
)
def test_cg_profile_ensure_agent_preserves_admin_service_stable_error(
    tmp_path,
    code: str,
    status_code: int,
    message: str,
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    admin_calls: list[dict[str, Any]] = []

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--admin-auth-token",
            "admin-token",
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Agent 001",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
        admin_client_factory=lambda **kwargs: _FakeAdminStableErrorClient(
            admin_calls,
            base_url=kwargs["base_url"],
            code=code,
            message=message,
            status_code=status_code,
        ),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == code
    assert body["error"]["status"] == status_code
    assert body["error"]["message"] == message


def test_cg_profile_ensure_agent_rotates_stale_credential(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    stale_token_file = tmp_path / "stale.token"
    stale_token_file.write_text("stale-token\n", encoding="utf-8")
    stale_token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=stale_token_file)
    admin_calls: list[dict[str, Any]] = []

    def raise_expired(*_args, **_kwargs):
        request = httpx.Request("GET", "http://cg.example/v3r1/projects/demo/agents/agent-001")
        response = httpx.Response(401, request=request, json={"error": "UnauthorizedError", "message": "agent credential expired"})
        raise httpx.HTTPStatusError("expired", request=request, response=response)

    monkeypatch.setattr("CommonGround.cli._validate_agent_profile_token", raise_expired)

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--admin-auth-token",
            "admin-secret",
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Agent 001",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
        admin_client_factory=lambda **kwargs: _FakeAdminClient(admin_calls, base_url=kwargs["base_url"], credential_id="cred-rotated"),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert admin_calls
    assert body["result"]["credential_id"] == "cred-rotated"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    token_file = Path(config["profiles"][f"{PROJECT_ID}/{AGENT_ID}"]["token_file"])
    assert token_file.read_text(encoding="utf-8").strip() == "cgac_cred-rotated.secret"


@pytest.mark.parametrize(
    "message",
    [
        "agent credential is no longer active",
        "invalid agent credential secret",
    ],
)
def test_cg_profile_ensure_agent_rotates_additional_stale_credential_errors(
    tmp_path,
    monkeypatch,
    message,
) -> None:
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    token_file = tmp_path / "agent.token"
    token_file.write_text("bad-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    admin_calls: list[dict[str, Any]] = []

    def raise_stale(*_args, **_kwargs):
        request = httpx.Request("GET", "http://cg.example/v3r1/projects/demo/agents/agent-001")
        response = httpx.Response(401, request=request, json={"error": "UnauthorizedError", "message": message})
        raise httpx.HTTPStatusError(message, request=request, response=response)

    monkeypatch.setattr("CommonGround.cli._validate_agent_profile_token", raise_stale)

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--admin-auth-token",
            "admin-secret",
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Agent 001",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
        admin_client_factory=lambda **kwargs: _FakeAdminClient(admin_calls, base_url=kwargs["base_url"], credential_id="cred-recovered"),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert admin_calls
    assert body["result"]["credential_id"] == "cred-recovered"


def test_cg_profile_ensure_agent_does_not_repair_disabled_agent(tmp_path, monkeypatch) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    admin_calls: list[dict[str, Any]] = []

    def raise_disabled(*_args, **_kwargs):
        request = httpx.Request("GET", "http://cg.example/v3r1/projects/demo/agents/agent-001")
        response = httpx.Response(403, request=request, json={"error": "ForbiddenError", "message": "authenticated agent is disabled"})
        raise httpx.HTTPStatusError("disabled", request=request, response=response)

    monkeypatch.setattr("CommonGround.cli._validate_agent_profile_token", raise_disabled)

    stdout = io.StringIO()
    exit_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--admin-auth-token",
            "admin-secret",
            "--profile",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--project-id",
            PROJECT_ID,
            "--requested-agent-id",
            AGENT_ID,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Agent 001",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
        admin_client_factory=lambda **kwargs: _FakeAdminClient(admin_calls, base_url=kwargs["base_url"]),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 1
    assert body["error"]["code"] == "forbidden"
    assert body["error"]["message"] == "authenticated agent is disabled"
    assert admin_calls == []


def test_cg_report_work_memory_rejects_request_id_override_mismatch(tmp_path) -> None:
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(json.dumps({"request_id": "manifest-id", "records": []}), encoding="utf-8")

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
            "--request-id",
            "different-id",
        ],
        stdout=stdout,
        client_factory=lambda **_: object(),
    )

    assert exit_code == 1
    body = json.loads(stdout.getvalue())
    assert body["ok"] is False
    assert body["error"]["code"] == "invalid_arguments"
    assert body["error"]["message"] == "--request-id does not match manifest request_id"


def test_cg_report_work_memory_rejects_manifest_top_level_meta(tmp_path) -> None:
    manifest_path = tmp_path / "report.json"
    manifest_path.write_text(
        json.dumps(
            {
                "kind": "agent_work_memory_report_manifest.v1",
                "request_id": "report-with-meta",
                "records": [{"role": "local_experience_summary", "payload": {"summary": "done"}}],
                "meta": {"note": "spoofed ledger note", "annotations": {"audit": "spoofed"}},
            }
        ),
        encoding="utf-8",
    )

    class FakeClient:
        def submit_work_memory_report(self, actor: AgentRef, manifest: dict[str, Any]):
            raise AssertionError("work-memory report should not be submitted")

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "report",
            "work-memory",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 1
    body = json.loads(stdout.getvalue())
    assert body["ok"] is False
    assert body["error"]["code"] == "invalid_input"
    assert body["error"]["status"] == 422
    assert "work-memory manifest must not include meta" in body["error"]["message"]


def test_cg_turn_get_uses_authoritative_final_result_fields() -> None:
    snapshot = _turn_snapshot(
        state=TurnState.CLOSED,
        outcome=TurnOutcome.SUCCEEDED,
        final_record_role="deliverable",
        final_payload={"answer": "done"},
    )

    class FakeClient:
        def get_turn(self, turn: TurnRef) -> TurnSnapshot:
            assert turn == snapshot.turn
            return snapshot

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        ["turn", "get", "--project-id", PROJECT_ID, "--turn-id", TURN_ID],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["ok"] is True
    assert body["result"]["turn_id"] == TURN_ID
    assert body["result"]["state"] == "closed"
    assert body["result"]["outcome"] == "succeeded"
    assert body["result"]["final_record_role"] == "deliverable"
    assert body["result"]["final_payload"] == {"answer": "done"}


def test_cg_turn_context_returns_semantic_records() -> None:
    snapshot = _turn_snapshot(state=TurnState.RUNNING)
    context = _context(
        snapshot,
        [
            _semantic_item(
                turn_seq=1,
                role="local_subagent",
                payload={"type": "local_subagent_result", "status": "ok"},
            )
        ],
    )
    calls: list[dict[str, Any]] = []

    class FakeClient:
        def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100) -> TurnContext:
            calls.append({"turn": turn, "after_turn_seq": after_turn_seq, "limit": limit})
            return context

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "turn",
            "context",
            "--project-id",
            PROJECT_ID,
            "--turn-id",
            TURN_ID,
            "--after-turn-seq",
            "0",
            "--limit",
            "25",
        ],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["ok"] is True
    assert body["result"]["turn"]["turn"]["turn_id"] == TURN_ID
    assert body["result"]["semantic_items"][0]["record"]["record_role"] == "local_subagent"
    assert body["result"]["semantic_items"][0]["content"] == {
        "data": {"type": "local_subagent_result", "status": "ok"}
    }
    assert calls == [
        {
            "turn": TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID),
            "after_turn_seq": 0,
            "limit": 25,
        }
    ]


def test_cg_turn_wait_polls_until_closed() -> None:
    queued = _turn_snapshot(state=TurnState.RUNNING)
    closed = _turn_snapshot(
        state=TurnState.CLOSED,
        outcome=TurnOutcome.SUCCEEDED,
        final_record_role="deliverable",
        final_payload="done",
    )
    sleeps: list[float] = []

    class FakeClient:
        def __init__(self) -> None:
            self.get_calls = 0

        def get_turn(self, turn: TurnRef) -> TurnSnapshot:
            self.get_calls += 1
            return queued if self.get_calls == 1 else closed

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "turn",
            "wait",
            "--project-id",
            PROJECT_ID,
            "--turn-id",
            TURN_ID,
            "--timeout-seconds",
            "5",
            "--poll-interval-ms",
            "25",
        ],
        stdout=stdout,
        sleep_fn=lambda seconds: sleeps.append(seconds),
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["result"]["state"] == "closed"
    assert body["result"]["final_record_role"] == "deliverable"
    assert body["result"]["final_payload"] == "done"
    assert body["result"]["poll_count"] == 1
    assert sleeps == [0.025]


def test_cg_turn_wait_does_not_treat_suspended_as_terminal() -> None:
    running = _turn_snapshot(state=TurnState.RUNNING)
    suspended = _turn_snapshot(state=TurnState.SUSPENDED)
    closed = _turn_snapshot(
        state=TurnState.CLOSED,
        outcome=TurnOutcome.SUCCEEDED,
        final_record_role="deliverable",
        final_payload="done",
    )
    sleeps: list[float] = []

    class FakeClient:
        def __init__(self) -> None:
            self.get_calls = 0

        def get_turn(self, turn: TurnRef) -> TurnSnapshot:
            self.get_calls += 1
            if self.get_calls == 1:
                return running
            if self.get_calls == 2:
                return suspended
            return closed

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "turn",
            "wait",
            "--project-id",
            PROJECT_ID,
            "--turn-id",
            TURN_ID,
            "--timeout-seconds",
            "5",
            "--poll-interval-ms",
            "25",
        ],
        stdout=stdout,
        sleep_fn=lambda seconds: sleeps.append(seconds),
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["result"]["state"] == "closed"
    assert body["result"]["poll_count"] == 2
    assert sleeps == [0.025, 0.025]


def test_cg_turn_resume_maps_http_conflict_to_error_envelope() -> None:
    request = httpx.Request("POST", f"http://cg.example/v3r1/projects/{PROJECT_ID}/turns/{TURN_ID}:resume")
    response = httpx.Response(409, request=request, json={"error": "ConflictError", "message": "resume denied"})

    class FakeClient:
        def resume_turn(self, requested_by: AgentRef, turn: TurnRef) -> None:
            raise httpx.HTTPStatusError("conflict", request=request, response=response)

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "turn",
            "resume",
            "--project-id",
            PROJECT_ID,
            "--turn-id",
            TURN_ID,
            "--requested-by",
            "operator",
        ],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 1
    assert json.loads(stdout.getvalue()) == {
        "ok": False,
        "error": {"code": "conflict", "message": "resume denied", "status": 409},
    }


def test_cg_agent_get_returns_snapshot() -> None:
    snapshot = AgentSnapshot(
        agent=AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID),
        enabled=True,
        accepts_work=True,
        capacity=1,
        capabilities=("turn.conversation.v1",),
        grants=(),
        public_metadata={"label": "Demo Agent"},
        created_at=NOW,
        updated_at=NOW,
        last_seen_at=NOW,
    )

    class FakeClient:
        def get_agent(self, agent: AgentRef) -> AgentSnapshot | None:
            assert agent == snapshot.agent
            return snapshot

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        ["agent", "get", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body == {
        "ok": True,
        "result": {
            "project_id": PROJECT_ID,
            "agent_id": AGENT_ID,
            "role": None,
            "description": None,
            "enabled": True,
            "accepts_work": True,
            "snapshot": to_jsonable(snapshot),
        },
    }


def test_cg_agent_drain_returns_updated_snapshot() -> None:
    snapshot = AgentSnapshot(
        agent=AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID),
        enabled=True,
        accepts_work=False,
        capacity=1,
        capabilities=("turn.conversation.v1",),
        grants=(),
        public_metadata={},
        created_at=NOW,
        updated_at=NOW,
        last_seen_at=NOW,
    )
    calls: list[AgentRef] = []

    class FakeClient:
        def drain_agent(self, agent: AgentRef, *, requested_by: AgentRef | None = None) -> None:
            calls.append((agent, requested_by))

        def get_agent(self, agent: AgentRef) -> AgentSnapshot | None:
            assert agent == snapshot.agent
            return snapshot

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        ["agent", "drain", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    assert calls == [(snapshot.agent, snapshot.agent)]
    assert json.loads(stdout.getvalue()) == {
        "ok": True,
        "result": {
            "project_id": PROJECT_ID,
            "agent_id": AGENT_ID,
            "accepts_work": False,
            "drained": True,
            "requested_by": AGENT_ID,
            "snapshot": to_jsonable(snapshot),
        },
    }


def test_cg_agent_resume_returns_updated_snapshot() -> None:
    snapshot = AgentSnapshot(
        agent=AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID),
        enabled=True,
        accepts_work=True,
        capacity=1,
        capabilities=("turn.conversation.v1",),
        grants=(),
        public_metadata={},
        created_at=NOW,
        updated_at=NOW,
        last_seen_at=NOW,
    )
    calls: list[AgentRef] = []

    class FakeClient:
        def resume_agent(self, agent: AgentRef, *, requested_by: AgentRef | None = None) -> None:
            calls.append((agent, requested_by))

        def get_agent(self, agent: AgentRef) -> AgentSnapshot | None:
            assert agent == snapshot.agent
            return snapshot

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        ["agent", "resume", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    assert calls == [(snapshot.agent, snapshot.agent)]
    assert json.loads(stdout.getvalue()) == {
        "ok": True,
        "result": {
            "project_id": PROJECT_ID,
            "agent_id": AGENT_ID,
            "accepts_work": True,
            "resumed": True,
            "requested_by": AGENT_ID,
            "snapshot": to_jsonable(snapshot),
        },
    }


def test_cg_agent_drain_allows_override_requested_by() -> None:
    snapshot = AgentSnapshot(
        agent=AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID),
        enabled=True,
        accepts_work=False,
        capacity=1,
        capabilities=("turn.conversation.v1",),
        grants=(),
        public_metadata={},
        created_at=NOW,
        updated_at=NOW,
        last_seen_at=NOW,
    )
    calls: list[tuple[AgentRef, AgentRef | None]] = []

    class FakeClient:
        def drain_agent(self, agent: AgentRef, *, requested_by: AgentRef | None = None) -> None:
            calls.append((agent, requested_by))

        def get_agent(self, agent: AgentRef) -> AgentSnapshot | None:
            return snapshot

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        ["agent", "drain", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID, "--requested-by", "operator"],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    assert calls == [(snapshot.agent, AgentRef(project_id=PROJECT_ID, agent_id="operator"))]
    assert json.loads(stdout.getvalue())["result"]["requested_by"] == "operator"


def test_parse_agent_snapshot_rejects_non_string_description() -> None:
    with pytest.raises(ValueError, match="description must be a string"):
        parse_agent_snapshot(
            {
                "agent": {"project_id": PROJECT_ID, "agent_id": AGENT_ID},
                "role": None,
                "description": ["bad"],
                "enabled": True,
                "accepts_work": True,
                "capacity": 1,
                "capabilities": [],
                "grants": [],
                "public_metadata": {},
                "created_at": NOW.isoformat(),
                "updated_at": NOW.isoformat(),
                "last_seen_at": None,
            }
        )


def test_cg_provision_spawn_builds_default_bootstrap_payload() -> None:
    calls: list[dict[str, Any]] = []

    class FakeClient:
        def dispatch(self, **kwargs):
            calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def close(self) -> None:
            return None

    stdout = io.StringIO()
    exit_code = main(
        [
            "provision",
            "spawn",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--provisioner-agent",
            "provisioner",
            "--role",
            "nanobot.leaf.conversation.v1",
            "--request-id",
            "prov-1",
        ],
        stdout=stdout,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(stdout.getvalue())
    assert exit_code == 0
    assert body["result"] == {
        "project_id": PROJECT_ID,
        "turn_id": TURN_ID,
        "agent_id": "provisioner",
        "request_id": "prov-1",
        "dispatch_key": "prov-1",
        "turn_kind": "turn.provision.agent.spawn.v1",
        "requested_role": "nanobot.leaf.conversation.v1",
    }
    assert calls == [
        {
            "requested_by": AgentRef(project_id=PROJECT_ID, agent_id="frontside"),
            "target_agent": AgentRef(project_id=PROJECT_ID, agent_id="provisioner"),
            "input_payload": {"task": "provision", "agent": {"role": "nanobot.leaf.conversation.v1"}},
            "authority": DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="prov-1"),
            "dispatch_key": "prov-1",
            "turn_kind": "turn.provision.agent.spawn.v1",
        }
    ]


def test_cg_provision_spawn_requires_idempotency_anchor() -> None:
    stdout = io.StringIO()
    exit_code = main(
        [
            "provision",
            "spawn",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--provisioner-agent",
            "provisioner",
            "--role",
            "nanobot.leaf.conversation.v1",
        ],
        stdout=stdout,
        client_factory=lambda **_: None,
    )

    assert exit_code == 1
    assert json.loads(stdout.getvalue()) == {
        "ok": False,
        "error": {
            "code": "invalid_arguments",
            "message": "cg provision spawn requires --request-id or --dispatch-key",
            "status": 2,
        },
    }


def test_cg_dispatch_requires_idempotency_anchor(tmp_path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"task": "hello"}), encoding="utf-8")

    out = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--target-agent",
            AGENT_ID,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--payload-file",
            str(payload_path),
        ],
        stdout=out,
        client_factory=lambda **_: None,
    )

    assert exit_code == 1
    assert json.loads(out.getvalue()) == {
        "ok": False,
        "error": {
            "code": "invalid_arguments",
            "message": "cg dispatch requires --request-id or --dispatch-key",
            "status": 2,
        },
    }


def test_cg_dispatch_copies_request_id_to_dispatch_key(tmp_path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"task": "hello"}), encoding="utf-8")
    calls: list[dict[str, Any]] = []

    class FakeClient:
        def dispatch(self, **kwargs):
            calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--target-agent",
            AGENT_ID,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--request-id",
            "req-only",
            "--payload-file",
            str(payload_path),
        ],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    body = json.loads(out.getvalue())
    assert body["result"]["request_id"] == "req-only"
    assert body["result"]["dispatch_key"] == "req-only"
    assert calls[0]["authority"] == DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="req-only")
    assert calls[0]["dispatch_key"] == "req-only"


def test_cg_dispatch_copies_dispatch_key_to_request_id(tmp_path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"task": "hello"}), encoding="utf-8")
    calls: list[dict[str, Any]] = []

    class FakeClient:
        def dispatch(self, **kwargs):
            calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            "frontside",
            "--target-agent",
            AGENT_ID,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--dispatch-key",
            "dispatch-only",
            "--payload-file",
            str(payload_path),
        ],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    body = json.loads(out.getvalue())
    assert body["result"]["request_id"] == "dispatch-only"
    assert body["result"]["dispatch_key"] == "dispatch-only"
    assert calls[0]["authority"] == DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="dispatch-only")
    assert calls[0]["dispatch_key"] == "dispatch-only"


def test_cg_worker_claim_next_returns_claim_and_context(tmp_path) -> None:
    claim = _claim_token()
    snapshot = _turn_snapshot(state=TurnState.RUNNING)
    context = _context(snapshot, [_semantic_item(turn_seq=1, role="bootstrap", payload={"task": "work"})])
    claim_out_path = tmp_path / "claim.json"

    class FakeClient:
        def claim_turn(self, agent: AgentRef, *, context_after_turn_seq: int = 0, context_limit: int = 100):
            assert agent == AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)
            assert context_after_turn_seq == 0
            assert context_limit == 100
            return ClaimedTurn(claim=claim, context=context)

        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        ["worker", "claim", "next", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID, "--claim-out-file", str(claim_out_path)],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["result"]["claimed"] is True
    assert body["result"]["turn_id"] == TURN_ID
    assert body["result"]["claim"] == to_jsonable(claim)
    assert json.loads(claim_out_path.read_text(encoding="utf-8")) == to_jsonable(claim)


def test_cg_worker_claim_next_returns_claim_when_context_fetch_partially_fails(tmp_path) -> None:
    claim = _claim_token()
    claim_out_path = tmp_path / "claim.json"

    class FakeClient:
        def claim_turn(self, agent: AgentRef, *, context_after_turn_seq: int = 0, context_limit: int = 100):
            raise ClaimTurnPartialFailure(
                claim=claim,
                context_error=RuntimeError("context boom"),
                suspend_error=RuntimeError("suspend boom"),
            )

        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        ["worker", "claim", "next", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID, "--claim-out-file", str(claim_out_path)],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 0
    assert json.loads(out.getvalue()) == {
        "ok": True,
        "result": {
            "claimed": True,
            "project_id": PROJECT_ID,
            "turn_id": TURN_ID,
            "agent_id": AGENT_ID,
            "claim": to_jsonable(claim),
            "context": None,
            "context_fetch_failed": True,
            "context_error": "context boom",
            "suspended_after_failure": False,
            "suspend_error": "suspend boom",
        },
    }
    assert json.loads(claim_out_path.read_text(encoding="utf-8")) == to_jsonable(claim)


def test_cg_worker_claim_run_uses_raw_claim_with_auto_renew_and_child_env(tmp_path, monkeypatch) -> None:
    claim = _claim_token()
    snapshot = _turn_snapshot(state=TurnState.RUNNING)
    context = _context(snapshot, [_semantic_item(turn_seq=1, role="bootstrap", payload={"task": "work"})])
    claim_out_path = tmp_path / "claim.json"
    context_out_path = tmp_path / "context.json"
    renewer_events: list[tuple[str, Any]] = []
    popen_calls: list[dict[str, Any]] = []

    class FakeRenewer:
        def __init__(self, client, *, claim, interval_seconds, max_consecutive_failures=3):
            renewer_events.append(("init", client, claim, interval_seconds, max_consecutive_failures))

        def start(self):
            renewer_events.append(("start",))

        def stop(self):
            renewer_events.append(("stop",))

        def fatal_error(self):
            return None

    class FakePopen:
        def __init__(self, command, *, env, stdout, stderr):
            popen_calls.append({"command": command, "env": env, "stdout": stdout, "stderr": stderr})

        def poll(self):
            return 0

    class FakeClient:
        def claim_turn_handle(self, agent: AgentRef):
            assert agent == AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)
            return claim

        def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100):
            assert turn == claim.turn_ref()
            assert after_turn_seq == 0
            assert limit == 100
            return context

        def close(self) -> None:
            return None

    monkeypatch.setattr("CommonGround.cli.ClaimAutoRenewer", FakeRenewer)
    monkeypatch.setattr("CommonGround.cli.subprocess.Popen", FakePopen)

    out = io.StringIO()
    exit_code = main(
        [
            "worker",
            "claim",
            "run",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--claim-out-file",
            str(claim_out_path),
            "--context-out-file",
            str(context_out_path),
            "--",
            "worker-bin",
            "--flag",
        ],
        stdout=out,
        sleep_fn=lambda _: None,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["result"]["claimed"] is True
    assert body["result"]["claim_file"] == str(claim_out_path)
    assert body["result"]["context_file"] == str(context_out_path)
    assert body["result"]["child_exit_code"] == 0
    assert body["result"]["lease_lost"] is False
    assert json.loads(claim_out_path.read_text(encoding="utf-8")) == to_jsonable(claim)
    assert json.loads(context_out_path.read_text(encoding="utf-8")) == to_jsonable(context)
    assert renewer_events[0][0] == "init"
    assert renewer_events[0][2:] == (claim, 0.5, 3)
    assert renewer_events[1:] == [("start",), ("stop",)]
    assert popen_calls[0]["command"] == ["worker-bin", "--flag"]
    assert popen_calls[0]["env"]["CG_CLAIM_FILE"] == str(claim_out_path)
    assert popen_calls[0]["env"]["CG_CONTEXT_FILE"] == str(context_out_path)
    assert json.loads(popen_calls[0]["env"]["CG_CLAIM_TOKEN"]) == to_jsonable(claim)


def test_cg_worker_once_finishes_from_final_file_without_exposing_claim_token(tmp_path, monkeypatch) -> None:
    claim = _claim_token()
    context = _context(_turn_snapshot(state=TurnState.RUNNING), [_semantic_item(turn_seq=1, role="bootstrap", payload={"task": "work"})])
    closed_snapshot = _turn_snapshot(state=TurnState.CLOSED, outcome=TurnOutcome.SUCCEEDED, final_payload={"summary": "done"})
    work_dir = tmp_path / "worker"
    renewer_events: list[str] = []
    popen_calls: list[dict[str, Any]] = []
    finish_calls: list[dict[str, Any]] = []

    class FakeRenewer:
        def __init__(self, client, *, claim, interval_seconds, max_consecutive_failures=3):
            return None

        def start(self):
            renewer_events.append("start")

        def stop(self):
            renewer_events.append("stop")

        def fatal_error(self):
            return None

        def raise_if_unhealthy(self):
            return None

    class FakePopen:
        def __init__(self, command, *, env, stdout, stderr):
            popen_calls.append({"command": command, "env": env, "stdout": stdout, "stderr": stderr})
            Path(env["CG_FINAL_FILE"]).write_text(json.dumps({"summary": "done"}), encoding="utf-8")

        def poll(self):
            return 0

    class FakeClient:
        def claim_turn_handle(self, agent: AgentRef):
            assert agent == AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)
            return claim

        def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100):
            return context

        def finish_turn(self, loaded_claim: ClaimToken, *, outcome: TurnOutcome, final_payload: Any, final_record_role: str, meta=None):
            finish_calls.append(
                {
                    "claim": loaded_claim,
                    "outcome": outcome,
                    "final_payload": final_payload,
                    "final_record_role": final_record_role,
                }
            )

        def get_turn(self, turn: TurnRef):
            return closed_snapshot

        def close(self) -> None:
            return None

    monkeypatch.setattr("CommonGround.cli.ClaimAutoRenewer", FakeRenewer)
    monkeypatch.setattr("CommonGround.cli.subprocess.Popen", FakePopen)
    monkeypatch.setenv("CG_CLAIM_TOKEN", "inherited-claim-secret")
    monkeypatch.setenv("CG_CLAIM_FILE", "/tmp/inherited-claim.json")

    out = io.StringIO()
    exit_code = main(
        [
            "worker",
            "once",
            "--auth-token",
            "agent-secret",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--work-dir",
            str(work_dir),
            "--command",
            "worker-bin",
            "--flag",
        ],
        stdout=out,
        sleep_fn=lambda _: None,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["result"]["claimed"] is True
    assert body["result"]["outcome"] == "succeeded"
    assert "claim" not in body["result"]
    assert "secret" not in out.getvalue()
    assert "CG_CLAIM_TOKEN" not in popen_calls[0]["env"]
    assert "CG_CLAIM_FILE" not in popen_calls[0]["env"]
    assert "inherited-claim-secret" not in repr(popen_calls[0]["env"])
    assert popen_calls[0]["env"]["CG_CONTEXT_FILE"] == str(work_dir / "context.json")
    assert popen_calls[0]["env"]["CG_FINAL_FILE"] == str(work_dir / "final.json")
    assert finish_calls[0]["final_payload"] == {"summary": "done"}
    assert renewer_events == ["start", "stop"]


def test_cg_worker_loop_reports_idle_iteration_without_claim() -> None:
    class FakeClient:
        def claim_turn_handle(self, agent: AgentRef):
            assert agent == AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)
            return None

        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        [
            "worker",
            "loop",
            "--auth-token",
            "agent-secret",
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT_ID,
            "--max-iterations",
            "1",
            "--command",
            "worker-bin",
        ],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["result"] == {"iterations": 1, "claimed_count": 0, "last": {"claimed": False}}


def test_cg_worker_claim_run_terminates_child_when_lease_is_lost(tmp_path, monkeypatch) -> None:
    claim = _claim_token()
    context = _context(_turn_snapshot(state=TurnState.RUNNING), [])
    lease_error = ClaimLeaseLostError("lease gone")
    process_events: list[str] = []

    class FakeRenewer:
        def __init__(self, client, *, claim, interval_seconds, max_consecutive_failures=3):
            self._polls = 0

        def start(self):
            return None

        def stop(self):
            return None

        def fatal_error(self):
            self._polls += 1
            if self._polls >= 1:
                return lease_error
            return None

    class FakePopen:
        def __init__(self, command, *, env, stdout, stderr):
            return None

        def poll(self):
            return None

        def terminate(self):
            process_events.append("terminate")

        def wait(self, timeout=None):
            process_events.append(f"wait:{timeout}")
            return -15

    class FakeClient:
        def claim_turn_handle(self, agent: AgentRef):
            return claim

        def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100):
            return context

        def close(self) -> None:
            return None

    monkeypatch.setattr("CommonGround.cli.ClaimAutoRenewer", FakeRenewer)
    monkeypatch.setattr("CommonGround.cli.subprocess.Popen", FakePopen)

    out = io.StringIO()
    exit_code = main(
        ["worker", "claim", "run", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID, "--", "worker-bin"],
        stdout=out,
        sleep_fn=lambda _: None,
        client_factory=lambda **_: FakeClient(),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 1
    assert body["result"]["lease_lost"] is True
    assert body["result"]["lease_error"] == "lease gone"
    assert process_events == ["terminate", "wait:5"]


def test_cg_worker_claim_mutation_commands_accept_bare_claim_file(tmp_path) -> None:
    claim_path = tmp_path / "claim.json"
    payload_path = tmp_path / "payload.json"
    claim = _claim_token()
    claim_path.write_text(json.dumps(to_jsonable(claim)), encoding="utf-8")
    payload_path.write_text(json.dumps({"status": "running"}), encoding="utf-8")
    closed_snapshot = _turn_snapshot(state=TurnState.CLOSED, outcome=TurnOutcome.SUCCEEDED)
    suspended_snapshot = _turn_snapshot(state=TurnState.SUSPENDED)
    events: list[tuple[str, Any]] = []

    class FakeClient:
        def append_record(self, loaded_claim: ClaimToken, payload: Any, *, role: str = "progress"):
            events.append(("append", loaded_claim, payload, role))
            return SemanticRecordRef(project_id=PROJECT_ID, record_id="rec-1")

        def finish_turn(self, loaded_claim: ClaimToken, **kwargs) -> None:
            events.append(("finish", loaded_claim, kwargs))

        def dispatch(self, **kwargs):
            events.append(("dispatch", kwargs))
            return TurnRef(project_id=PROJECT_ID, turn_id="T-2")

        def suspend_turn(self, loaded_claim: ClaimToken, *, reason: str, note: str | None = None, meta=None) -> None:
            events.append(("suspend", loaded_claim, reason, note, meta))

        def get_turn(self, turn: TurnRef) -> TurnSnapshot:
            if events and events[-1][0] == "suspend":
                return suspended_snapshot
            return closed_snapshot

        def close(self) -> None:
            return None

    append_out = io.StringIO()
    append_code = main(
        ["worker", "claim", "append", "--claim-file", str(claim_path), "--payload-file", str(payload_path)],
        stdout=append_out,
        client_factory=lambda **_: FakeClient(),
    )
    assert append_code == 0
    assert json.loads(append_out.getvalue())["result"]["record"] == {"project_id": PROJECT_ID, "record_id": "rec-1"}

    finish_payload_path = tmp_path / "finish.json"
    finish_payload_path.write_text(json.dumps({"result": "done"}), encoding="utf-8")
    finish_out = io.StringIO()
    finish_code = main(
        [
            "worker",
            "claim",
            "finish",
            "--claim-file",
            str(claim_path),
            "--outcome",
            "succeeded",
            "--payload-file",
            str(finish_payload_path),
        ],
        stdout=finish_out,
        client_factory=lambda **_: FakeClient(),
    )
    assert finish_code == 0
    assert json.loads(finish_out.getvalue())["result"]["state"] == "closed"
    assert json.loads(finish_out.getvalue())["result"]["final_payload"] is None

    dispatch_out = io.StringIO()
    dispatch_code = main(
        [
            "worker",
            "claim",
            "dispatch-child",
            "--claim-file",
            str(claim_path),
            "--requested-by",
            AGENT_ID,
            "--target-agent",
            "child-agent",
            "--payload-file",
            str(payload_path),
            "--dispatch-key",
            "T-2",
        ],
        stdout=dispatch_out,
        client_factory=lambda **_: FakeClient(),
    )
    assert dispatch_code == 0
    assert json.loads(dispatch_out.getvalue())["result"]["turn_id"] == "T-2"
    assert events[2][1]["authority"] == DispatchAuthority(mode=DispatchAuthorityMode.CHILD_DERIVATION, parent_claim=claim)
    assert events[2][1]["dispatch_key"] == "T-2"

    suspend_out = io.StringIO()
    suspend_code = main(
        ["worker", "claim", "suspend", "--claim-file", str(claim_path), "--reason", "await_child", "--note", "waiting"],
        stdout=suspend_out,
        client_factory=lambda **_: FakeClient(),
    )
    assert suspend_code == 0
    assert json.loads(suspend_out.getvalue())["result"]["state"] == "suspended"

    assert [item[0] for item in events] == ["append", "finish", "dispatch", "suspend"]


def test_cg_smoke_pair_checks_offer_dispatch_wait_and_context(tmp_path) -> None:
    token_file = tmp_path / "agent.token"
    token_file.write_text("profile-token\n", encoding="utf-8")
    token_file.chmod(0o600)
    config_path = tmp_path / "config.json"
    _write_profile_config(config_path, token_file=token_file)
    dispatch_calls: list[dict[str, Any]] = []
    projection_calls: list[dict[str, Any]] = []
    closed_snapshot = _turn_snapshot(state=TurnState.CLOSED, outcome=TurnOutcome.SUCCEEDED, final_payload={"summary": "ok"})
    context = _context(closed_snapshot, [_semantic_item(turn_seq=1, role="deliverable", payload={"summary": "ok"})])

    class FakeProjectionClient:
        def __init__(self, **kwargs):
            projection_calls.append(kwargs)

        def list_turn_offers(self, **kwargs):
            projection_calls.append(kwargs)
            return type(
                "OfferPage",
                (),
                {
                    "items": (
                        {
                            "agent_id": "agent-b",
                            "turn_kind": "turn.conversation.v1",
                            "enabled": True,
                            "accepts_work": True,
                        },
                    )
                },
            )()

        def close(self) -> None:
            return None

    class FakeClient:
        def dispatch(self, **kwargs):
            dispatch_calls.append(kwargs)
            return TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID)

        def get_turn(self, turn: TurnRef):
            return closed_snapshot

        def fetch_context(self, turn: TurnRef, *, after_turn_seq: int = 0, limit: int = 100):
            return context

        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        [
            "smoke",
            "pair",
            "--config",
            str(config_path),
            "--from",
            f"{PROJECT_ID}/{AGENT_ID}",
            "--to",
            "agent-b",
            "--request-id",
            "smoke-req-001",
        ],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
        projection_client_factory=lambda **kwargs: FakeProjectionClient(**kwargs),
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["result"]["dispatch"]["turn_id"] == TURN_ID
    assert body["result"]["terminal_payload"] == {"summary": "ok"}
    assert body["result"]["context"]["turn"]["turn"]["turn_id"] == TURN_ID
    assert projection_calls[1]["agent_id"] == "agent-b"
    assert dispatch_calls[0]["requested_by"] == AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID)
    assert dispatch_calls[0]["target_agent"] == AgentRef(project_id=PROJECT_ID, agent_id="agent-b")


def test_cg_worker_claim_renew_returns_timing_metadata(tmp_path) -> None:
    claim = _claim_token()
    claim_path = tmp_path / "claim.json"
    claim_path.write_text(json.dumps(to_jsonable(claim)), encoding="utf-8")
    seen: list[ClaimToken] = []
    factory_calls: list[dict[str, Any]] = []

    class FakeClient:
        def renew_claim(self, loaded_claim: ClaimToken):
            seen.append(loaded_claim)
            return type(
                "Renewed",
                (),
                {
                    "server_time": NOW,
                    "expires_at": NOW,
                    "recommended_interval_seconds": 15.0,
                },
            )()

        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        ["worker", "claim", "renew", "--claim-file", str(claim_path)],
        stdout=out,
        client_factory=lambda **kwargs: factory_calls.append(kwargs) or FakeClient(),
    )

    assert exit_code == 0
    assert seen == [claim]
    assert json.loads(out.getvalue()) == {
        "ok": True,
        "result": {
            "project_id": PROJECT_ID,
            "turn_id": TURN_ID,
            "agent_id": AGENT_ID,
            "server_time": NOW.isoformat(),
            "expires_at": NOW.isoformat(),
            "recommended_interval_seconds": 15.0,
        },
    }
    assert factory_calls == [
        {
            "base_url": "http://127.0.0.1:8000",
            "auth_token": "test-token",
            "headers": {
                "X-CG-Project-Id": PROJECT_ID,
                "X-CG-Agent-Id": AGENT_ID,
                "Authorization": "Bearer test-token",
            },
        }
    ]


def test_cg_worker_claim_renew_rejects_malformed_envelope_with_json_error(tmp_path) -> None:
    claim_path = tmp_path / "claim.json"
    claim_path.write_text(json.dumps({"ok": False, "error": {"code": "boom"}}), encoding="utf-8")

    class FakeClient:
        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        ["worker", "claim", "renew", "--claim-file", str(claim_path)],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 1
    assert json.loads(out.getvalue()) == {
        "ok": False,
        "error": {
            "code": "invalid_input",
            "message": "claim file envelope must contain result.claim as an object",
            "status": 422,
        },
    }



def test_cg_worker_claim_renew_rejects_non_object_claim_file_with_json_error(tmp_path) -> None:
    claim_path = tmp_path / "claim.json"
    claim_path.write_text(json.dumps(["not-a-claim"]), encoding="utf-8")

    class FakeClient:
        def close(self) -> None:
            return None

    out = io.StringIO()
    exit_code = main(
        ["worker", "claim", "renew", "--claim-file", str(claim_path)],
        stdout=out,
        client_factory=lambda **_: FakeClient(),
    )

    assert exit_code == 1
    assert json.loads(out.getvalue()) == {
        "ok": False,
        "error": {
            "code": "invalid_input",
            "message": "claim file must contain a JSON object",
            "status": 422,
        },
    }


def test_cg_cli_missing_required_argument_returns_json_error() -> None:
    out = io.StringIO()
    exit_code = main(["dispatch"], stdout=out, client_factory=lambda **_: None)

    assert exit_code == 1
    body = json.loads(out.getvalue())
    assert body["ok"] is False
    assert body["error"]["code"] == "invalid_arguments"
    assert body["error"]["status"] == 2


def test_cg_worker_requires_agent_credential_when_claimed_identity_is_inferred(monkeypatch) -> None:
    monkeypatch.delenv("CG_AGENT_CREDENTIAL_TOKEN", raising=False)
    out = io.StringIO()
    exit_code = main(
        ["worker", "claim", "next", "--project-id", PROJECT_ID, "--agent-id", AGENT_ID],
        stdout=out,
        client_factory=lambda **_: None,
    )

    assert exit_code == 1
    body = json.loads(out.getvalue())
    assert body["ok"] is False
    assert body["error"]["code"] == "invalid_arguments"
    assert body["error"]["message"] == "agent credential token is required when claimed agent identity is set"


def test_cg_cli_unknown_subcommand_returns_json_error() -> None:
    out = io.StringIO()
    exit_code = main(["bogus"], stdout=out, client_factory=lambda **_: None)

    assert exit_code == 1
    body = json.loads(out.getvalue())
    assert body["ok"] is False
    assert body["error"]["code"] == "invalid_arguments"
    assert body["error"]["status"] == 2


def test_cg_cli_profile_ensure_then_work_memory_roundtrip_with_admin_service(
    test_client,
    kernel_app,
    test_pg_dsn,
    tmp_path,
    monkeypatch,
) -> None:
    project_id = "cli-profile-e2e"
    agent_id = "local-agent"
    product_token = "product-admin-token"
    product_user = "product-user"
    monkeypatch.setattr("CommonGround.cli_profiles.DEFAULT_CREDENTIALS_DIR", tmp_path / "credentials")
    bootstrap_project_admin_service_agent(kernel_app, project_id=project_id, creator_ref="creator-001")

    workflow = ByoaWorkflowStore(test_pg_dsn)
    processor = ByoaRegistrationProcessor(
        workflow,
        client=test_client,
        admin_service_token=agent_token(AgentRef(project_id=project_id, agent_id=ADMIN_SERVICE_AGENT_ID)),
    )
    facade = AdminServiceByoaFacade(
        workflow,
        processor,
        authorize_request=lambda requester_user_id, requested_project_id: requester_user_id == product_user and requested_project_id == project_id,
    )

    def resolve_requester(request) -> str:
        return product_user if request.headers.get("Authorization") == f"Bearer {product_token}" else ""

    admin_client = TestClient(
        create_agent_credential_token_request_app(
            facade,
            resolve_requester_user_id=resolve_requester,
        )
    )
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "admin_base_url": "http://admin.testserver",
                "admin_auth": {"token": product_token},
            }
        ),
        encoding="utf-8",
    )
    manifest_path = tmp_path / "work-memory.json"
    manifest_path.write_text(
        json.dumps(
            {
                "kind": "agent_work_memory_report_manifest.v1",
                "request_id": "wm-cli-profile-e2e-001",
                "summary": "Submitted through CLI-managed profile.",
                "records": [
                    {
                        "role": "execution_summary",
                        "payload": {
                            "kind": "cg.turn_record.v1",
                            "summary": "CLI profile bootstrap and report succeeded.",
                        },
                    }
                ],
                "final_payload": {"summary": "Profile-backed report submitted."},
            }
        ),
        encoding="utf-8",
    )

    def make_client(**kwargs: Any):
        return HttpAgentClient(client=test_client, headers=kwargs.get("headers"))

    profile_out = io.StringIO()
    profile_code = main(
        [
            "profile",
            "ensure-agent",
            "--config",
            str(config_path),
            "--profile",
            f"{project_id}/{agent_id}",
            "--project-id",
            project_id,
            "--requested-agent-id",
            agent_id,
            "--runtime-kind",
            "codex.local.v1",
            "--display-name",
            "Local Agent",
        ],
        stdout=profile_out,
        admin_client_factory=lambda **_: admin_client,
    )

    profile_body = json.loads(profile_out.getvalue())
    assert profile_code == 0
    assert profile_body["ok"] is True
    assert profile_body["result"]["profile"] == f"{project_id}/{agent_id}"
    assert "cgac_" not in profile_out.getvalue()
    assert product_token not in profile_out.getvalue()

    report_out = io.StringIO()
    report_code = main(
        [
            "report",
            "work-memory",
            "--config",
            str(config_path),
            "--profile",
            f"{project_id}/{agent_id}",
            "--project-id",
            project_id,
            "--agent-id",
            agent_id,
            "--manifest-file",
            str(manifest_path),
        ],
        stdout=report_out,
        client_factory=make_client,
        admin_client_factory=lambda **_: admin_client,
    )

    report_body = json.loads(report_out.getvalue())
    assert report_code == 0
    assert report_body["ok"] is True
    assert report_body["result"]["status"] == "submitted"
    assert report_body["result"]["record_refs"]
    assert report_body["result"]["record_refs"][0]["project_id"] == project_id
    assert "cgac_" not in report_out.getvalue()
    assert product_token not in report_out.getvalue()
    turn_id = report_body["result"]["turn"]["turn_id"]

    context_out = io.StringIO()
    context_code = main(
        [
            "turn",
            "context",
            "--config",
            str(config_path),
            "--profile",
            f"{project_id}/{agent_id}",
            "--project-id",
            project_id,
            "--turn-id",
            turn_id,
        ],
        stdout=context_out,
        client_factory=make_client,
    )

    context_body = json.loads(context_out.getvalue())
    assert context_code == 0
    assert context_body["ok"] is True
    assert context_body["result"]["turn"]["turn"]["turn_id"] == turn_id
    assert context_body["result"]["semantic_items"]


def test_cg_cli_dispatch_get_wait_roundtrip_with_service(test_client, kernel_app, tmp_path, monkeypatch) -> None:
    frontside = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
    worker_agent = AgentRef(project_id=PROJECT_ID, agent_id="worker")
    kernel_app.topology.register_agent(frontside, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(worker_agent, capabilities=(TURN_KIND_CONVERSATION_V1,))
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", agent_token(frontside))
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"task": "cli e2e"}), encoding="utf-8")

    def make_client(**kwargs: Any):
        from CommonGround.agent_client import HttpAgentClient

        return HttpAgentClient(client=test_client, headers=kwargs.get("headers"))

    dispatch_out = io.StringIO()
    dispatch_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            frontside.agent_id,
            "--target-agent",
            worker_agent.agent_id,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--request-id",
            "cli-e2e-1",
            "--payload-file",
            str(payload_path),
        ],
        stdout=dispatch_out,
        client_factory=make_client,
    )

    dispatch_body = json.loads(dispatch_out.getvalue())
    assert dispatch_code == 0
    turn_id = dispatch_body["result"]["turn_id"]

    from CommonGround.adapters import ExternalAgentAdapter

    worker = ExternalAgentAdapter(agent=worker_agent, sdk=kernel_app.sdk)
    claim = worker.claim_turn()
    assert claim is not None
    assert claim.turn_id == turn_id
    worker.finish_current(claim, outcome=TurnOutcome.SUCCEEDED, final_payload={"result": "done"})

    get_out = io.StringIO()
    get_code = main(
        ["turn", "get", "--project-id", PROJECT_ID, "--turn-id", turn_id, "--caller-project-id", PROJECT_ID, "--caller-agent-id", frontside.agent_id],
        stdout=get_out,
        client_factory=make_client,
    )
    get_body = json.loads(get_out.getvalue())
    assert get_code == 0
    assert get_body["result"]["state"] == "closed"
    assert get_body["result"]["final_record_role"] == "deliverable"
    assert get_body["result"]["final_payload"] == {"result": "done"}

    wait_out = io.StringIO()
    wait_code = main(
        [
            "turn",
            "wait",
            "--project-id",
            PROJECT_ID,
            "--turn-id",
            turn_id,
            "--timeout-seconds",
            "1",
            "--poll-interval-ms",
            "10",
            "--caller-project-id",
            PROJECT_ID,
            "--caller-agent-id",
            frontside.agent_id,
        ],
        stdout=wait_out,
        client_factory=make_client,
    )
    wait_body = json.loads(wait_out.getvalue())
    assert wait_code == 0
    assert wait_body["result"]["state"] == "closed"
    assert wait_body["result"]["final_record_role"] == "deliverable"
    assert wait_body["result"]["final_payload"] == {"result": "done"}


def test_cg_cli_turn_resume_roundtrip_with_service(test_client, kernel_app, monkeypatch) -> None:
    frontside = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
    planner = AgentRef(project_id=PROJECT_ID, agent_id="planner")
    child = AgentRef(project_id=PROJECT_ID, agent_id="child")
    kernel_app.topology.register_agent(frontside, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(planner, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(child, capabilities=(TURN_KIND_CONVERSATION_V1,))
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", agent_token(planner))

    parent_turn = kernel_app.sdk.dispatch(
        requested_by=frontside,
        target_agent=planner,
        input_payload={"task": "parent"},
        authority=DispatchAuthority(mode=DispatchAuthorityMode.ROOT_REQUEST, request_id="resume-1"),
        dispatch_key="resume-1",
    )

    from CommonGround.adapters import ExternalAgentAdapter

    planner_adapter = ExternalAgentAdapter(agent=planner, sdk=kernel_app.sdk)
    parent_claim = planner_adapter.claim_turn()
    assert parent_claim is not None
    planner_adapter.dispatch(
        parent_claim,
        target_agent=child,
        input_payload={"task": "child"},
        dispatch_key="resume-child-1",
    )
    planner_adapter.suspend_current(parent_claim, reason="await_child", note="waiting")

    def make_client(**kwargs: Any):
        from CommonGround.agent_client import HttpAgentClient

        return HttpAgentClient(client=test_client, headers=kwargs.get("headers"))

    out = io.StringIO()
    exit_code = main(
        [
            "turn",
            "resume",
            "--project-id",
            PROJECT_ID,
            "--turn-id",
            parent_turn.turn_id,
            "--requested-by",
            planner.agent_id,
        ],
        stdout=out,
        client_factory=make_client,
    )

    body = json.loads(out.getvalue())
    assert exit_code == 0
    assert body["result"]["turn_id"] == parent_turn.turn_id
    assert body["result"]["state"] == "queued"
    assert body["result"]["requested_by"] == planner.agent_id


def test_cg_cli_turn_get_uses_final_result_truth_with_service(test_client, kernel_app, tmp_path, monkeypatch) -> None:
    frontside = AgentRef(project_id=PROJECT_ID, agent_id="frontside")
    worker_agent = AgentRef(project_id=PROJECT_ID, agent_id="worker")
    kernel_app.topology.register_agent(frontside, capabilities=(TURN_KIND_CONVERSATION_V1,))
    kernel_app.topology.register_agent(worker_agent, capabilities=(TURN_KIND_CONVERSATION_V1,))
    monkeypatch.setenv("CG_AGENT_CREDENTIAL_TOKEN", agent_token(frontside))
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(json.dumps({"task": "cli final truth"}), encoding="utf-8")

    def make_client(**kwargs: Any):
        from CommonGround.agent_client import HttpAgentClient

        return HttpAgentClient(client=test_client, headers=kwargs.get("headers"))

    dispatch_out = io.StringIO()
    dispatch_code = main(
        [
            "dispatch",
            "--project-id",
            PROJECT_ID,
            "--requested-by",
            frontside.agent_id,
            "--target-agent",
            worker_agent.agent_id,
            "--turn-kind",
            TURN_KIND_CONVERSATION_V1,
            "--request-id",
            "cli-final-truth-1",
            "--payload-file",
            str(payload_path),
        ],
        stdout=dispatch_out,
        client_factory=make_client,
    )
    assert dispatch_code == 0
    turn_id = json.loads(dispatch_out.getvalue())["result"]["turn_id"]

    from CommonGround.adapters import ExternalAgentAdapter

    worker = ExternalAgentAdapter(agent=worker_agent, sdk=kernel_app.sdk)
    claim = worker.claim_turn()
    assert claim is not None
    worker.append_record(claim, {"draft": "intermediate"}, role="deliverable")
    worker.finish_current(
        claim,
        outcome=TurnOutcome.FAILED,
        final_payload={"error": "final"},
        final_record_role="error_report",
    )

    get_out = io.StringIO()
    get_code = main(
        ["turn", "get", "--project-id", PROJECT_ID, "--turn-id", turn_id, "--caller-project-id", PROJECT_ID, "--caller-agent-id", frontside.agent_id],
        stdout=get_out,
        client_factory=make_client,
    )
    body = json.loads(get_out.getvalue())
    assert get_code == 0
    assert body["result"]["state"] == "closed"
    assert body["result"]["outcome"] == "failed"
    assert body["result"]["final_record_role"] == "error_report"
    assert body["result"]["final_payload"] == {"error": "final"}


def _turn_snapshot(
    *,
    state: TurnState,
    outcome: TurnOutcome | None = None,
    final_record_role: str | None = None,
    final_payload: Any | None = None,
) -> TurnSnapshot:
    return TurnSnapshot(
        turn=TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID),
        target_agent=AgentRef(project_id=PROJECT_ID, agent_id=AGENT_ID),
        turn_kind="turn.conversation.v1",
        cause=CauseRef(kind="request", id="req-1"),
        state=state,
        outcome=outcome,
        stop_requested=False,
        current_claim_agent_id=None,
        claim_expires_at=None,
        spawn_key="spawn-1",
        final_record_role=final_record_role,
        final_cardbox_ref=None,
        final_payload=final_payload,
        created_at=NOW,
        updated_at=NOW,
        closed_at=NOW if state == TurnState.CLOSED else None,
    )


def _context(snapshot: TurnSnapshot, items: list[SemanticContextItem]) -> TurnContext:
    return TurnContext(turn=snapshot, semantic_items=tuple(items))


def _claim_token() -> ClaimToken:
    return ClaimToken(
        project_id=PROJECT_ID,
        turn_id=TURN_ID,
        agent_id=AGENT_ID,
        token="claim-token",
        expires_at=NOW,
    )


def _semantic_item(*, turn_seq: int, role: str, payload: Any) -> SemanticContextItem:
    return SemanticContextItem(
        record=SemanticRecordSnapshot(
            ref=SemanticRecordRef(project_id=PROJECT_ID, record_id=f"rec-{turn_seq}"),
            turn=TurnRef(project_id=PROJECT_ID, turn_id=TURN_ID),
            turn_seq=turn_seq,
            record_role=role,
            cardbox_ref=CardBoxRef(project_id=PROJECT_ID, cardbox_id=f"box-{turn_seq}"),
            created_at=NOW,
        ),
        content=_FakeContent(payload),
    )
