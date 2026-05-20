from __future__ import annotations

import json
import stat

import pytest

from CommonGround.agent_credentials import parse_agent_credential_token, verify_agent_credential_secret
from CommonGround.contracts import AgentRef, ConflictError, NotFoundError
from CommonGround.infra import PostgresAgentCredentialStore
from scripts.setup.issue_agent_credential import issue_agent_credential, main


PROJECT_ID = "bootstrap-credential-project"
AGENT = AgentRef(project_id=PROJECT_ID, agent_id="admin-service")


def test_bootstrap_helper_issues_credential_for_existing_enabled_agent(test_pg_dsn: str, kernel_app) -> None:
    kernel_app.topology.register_agent(AGENT)

    result = issue_agent_credential(
        pg_dsn=test_pg_dsn,
        project_id=PROJECT_ID,
        agent_id=AGENT.agent_id,
        issued_by_agent_id="operator",
        provenance_kind="test_fixture",
        provenance_ref="fixture-001",
        provenance_payload_hash="sha256:fixture",
    )
    parsed = parse_agent_credential_token(result["token"])
    row = PostgresAgentCredentialStore(test_pg_dsn).load_agent_credential_by_id(result["credential_id"])

    assert parsed.credential_id == result["credential_id"]
    assert row is not None
    assert row.project_id == PROJECT_ID
    assert row.agent_id == AGENT.agent_id
    assert row.issued_by_agent_id == "operator"
    assert row.provenance_kind == "test_fixture"
    assert row.provenance_ref == "fixture-001"
    assert row.provenance_payload_hash == "sha256:fixture"
    assert verify_agent_credential_secret(parsed.secret, row.secret_hash) is True
    assert row.secret_hash != result["token"]


def test_bootstrap_helper_rejects_missing_or_disabled_agent(test_pg_dsn: str, kernel_app) -> None:
    with pytest.raises(NotFoundError, match="agent not found"):
        issue_agent_credential(pg_dsn=test_pg_dsn, project_id=PROJECT_ID, agent_id=AGENT.agent_id)

    kernel_app.topology.register_agent(AGENT, enabled=False)
    with pytest.raises(ConflictError, match="agent disabled"):
        issue_agent_credential(pg_dsn=test_pg_dsn, project_id=PROJECT_ID, agent_id=AGENT.agent_id)


def test_bootstrap_helper_can_write_token_file_without_echoing_secret(
    test_pg_dsn: str,
    kernel_app,
    tmp_path,
) -> None:
    kernel_app.topology.register_agent(AGENT)
    token_file = tmp_path / "agent.token"

    result = issue_agent_credential(
        pg_dsn=test_pg_dsn,
        project_id=PROJECT_ID,
        agent_id=AGENT.agent_id,
        provenance_kind="test_fixture",
        token_file=token_file,
    )

    assert "token" not in result
    assert result["token_file"] == str(token_file)
    token = token_file.read_text(encoding="utf-8").strip()
    assert parse_agent_credential_token(token).credential_id == result["credential_id"]
    assert stat.S_IMODE(token_file.stat().st_mode) == stat.S_IRUSR | stat.S_IWUSR


def test_bootstrap_helper_refuses_to_overwrite_existing_token_file(
    test_pg_dsn: str,
    kernel_app,
    tmp_path,
) -> None:
    kernel_app.topology.register_agent(AGENT)
    token_file = tmp_path / "agent.token"
    token_file.write_text("existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        issue_agent_credential(
            pg_dsn=test_pg_dsn,
            project_id=PROJECT_ID,
            agent_id=AGENT.agent_id,
            token_file=token_file,
        )

    assert token_file.read_text(encoding="utf-8") == "existing\n"


def test_bootstrap_cli_prints_json_once(capsys, test_pg_dsn: str, kernel_app) -> None:
    kernel_app.topology.register_agent(AGENT)

    main(
        [
            "--pg-dsn",
            test_pg_dsn,
            "--project-id",
            PROJECT_ID,
            "--agent-id",
            AGENT.agent_id,
            "--provenance-kind",
            "test_fixture",
        ]
    )
    output = capsys.readouterr().out.strip()
    result = json.loads(output)

    assert result["project_id"] == PROJECT_ID
    assert result["agent_id"] == AGENT.agent_id
    assert parse_agent_credential_token(result["token"]).credential_id == result["credential_id"]
