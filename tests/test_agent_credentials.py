from __future__ import annotations

import pytest

from CommonGround.agent_credentials import (
    AGENT_CREDENTIAL_TOKEN_PREFIX,
    ParsedAgentCredentialToken,
    format_agent_credential_token,
    hash_agent_credential_secret,
    issue_agent_credential_token,
    parse_agent_credential_token,
    verify_agent_credential_secret,
)
from CommonGround.contracts import AgentCredentialIssueResult, AgentCredentialRef


def test_issue_token_round_trips_and_uses_unambiguous_separator() -> None:
    token, parsed = issue_agent_credential_token("cred_agent_001")

    assert token.startswith(f"{AGENT_CREDENTIAL_TOKEN_PREFIX}cred_agent_001.")
    assert parse_agent_credential_token(token) == parsed
    assert "." not in parsed.credential_id
    assert "." not in parsed.secret


def test_format_token_rejects_ambiguous_parts() -> None:
    with pytest.raises(ValueError, match="credential_id"):
        format_agent_credential_token("cred.with.dot", "secret")
    with pytest.raises(ValueError, match="secret"):
        format_agent_credential_token("cred_ok", "secret.with.dot")
    with pytest.raises(ValueError, match="credential_id"):
        format_agent_credential_token(" cred_ok", "secret")
    with pytest.raises(ValueError, match="secret"):
        format_agent_credential_token("cred_ok", "secret/value")


@pytest.mark.parametrize(
    "token",
    (
        "",
        "cred_001.secret",
        "cgac_",
        "cgac_cred_001",
        "cgac_.secret",
        "cgac_cred_001.",
        "cgac_cred.001.secret",
    ),
)
def test_parse_token_rejects_malformed_values(token: str) -> None:
    with pytest.raises(ValueError):
        parse_agent_credential_token(token)


def test_hash_and_verify_secret_without_plaintext_round_trip() -> None:
    secret = "secret_value_without_dot"
    secret_hash = hash_agent_credential_secret(secret)

    assert secret_hash != secret
    assert verify_agent_credential_secret(secret, secret_hash) is True
    assert verify_agent_credential_secret("wrong_secret", secret_hash) is False
    assert verify_agent_credential_secret(secret, "") is False


def test_plaintext_secret_is_redacted_from_token_models_repr() -> None:
    parsed = ParsedAgentCredentialToken(credential_id="cred_001", secret="secret_value")
    result = AgentCredentialIssueResult(
        ref=AgentCredentialRef(project_id="project-001", agent_id="agent-001", credential_id="cred_001"),
        token="cgac_cred_001.secret_value",
    )

    assert "secret_value" not in repr(parsed)
    assert "secret_value" not in repr(result)
    assert parsed.secret == "secret_value"
    assert result.token == "cgac_cred_001.secret_value"
