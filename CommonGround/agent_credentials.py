from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import re
import secrets


AGENT_CREDENTIAL_ID_PREFIX = "cred_"
AGENT_CREDENTIAL_TOKEN_PREFIX = "cgac_"
AGENT_CREDENTIAL_SECRET_BYTES = 32
AGENT_CREDENTIAL_ISSUE_ANY_GRANT = "agent.credential.issue.any"
AGENT_CREDENTIAL_REVOKE_ANY_GRANT = "agent.credential.revoke.any"
_TOKEN_PART_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class ParsedAgentCredentialToken:
    credential_id: str
    secret: str = field(repr=False)


def generate_agent_credential_id() -> str:
    return f"{AGENT_CREDENTIAL_ID_PREFIX}{secrets.token_urlsafe(18)}"


def generate_agent_credential_secret() -> str:
    return secrets.token_urlsafe(AGENT_CREDENTIAL_SECRET_BYTES)


def issue_agent_credential_token(credential_id: str | None = None) -> tuple[str, ParsedAgentCredentialToken]:
    credential_id = credential_id or generate_agent_credential_id()
    secret = generate_agent_credential_secret()
    token = format_agent_credential_token(credential_id, secret)
    return token, ParsedAgentCredentialToken(credential_id=credential_id, secret=secret)


def format_agent_credential_token(credential_id: str, secret: str) -> str:
    _validate_token_part("credential_id", credential_id)
    _validate_token_part("secret", secret)
    return f"{AGENT_CREDENTIAL_TOKEN_PREFIX}{credential_id}.{secret}"


def parse_agent_credential_token(token: str) -> ParsedAgentCredentialToken:
    if not isinstance(token, str) or not token:
        raise ValueError("agent credential token must be a non-empty string")
    if not token.startswith(AGENT_CREDENTIAL_TOKEN_PREFIX):
        raise ValueError("agent credential token has invalid prefix")
    body = token[len(AGENT_CREDENTIAL_TOKEN_PREFIX) :]
    if "." not in body:
        raise ValueError("agent credential token must contain credential id and secret")
    credential_id, secret = body.split(".", 1)
    _validate_token_part("credential_id", credential_id)
    _validate_token_part("secret", secret)
    return ParsedAgentCredentialToken(credential_id=credential_id, secret=secret)


def hash_agent_credential_secret(secret: str) -> str:
    _validate_token_part("secret", secret)
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def verify_agent_credential_secret(secret: str, secret_hash: str) -> bool:
    if not isinstance(secret_hash, str) or not secret_hash:
        return False
    return secrets.compare_digest(hash_agent_credential_secret(secret), secret_hash)


def _validate_token_part(field_name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if value.strip() != value:
        raise ValueError(f"{field_name} must not contain surrounding whitespace")
    if _TOKEN_PART_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must contain only URL-safe token characters")


__all__ = [
    "AGENT_CREDENTIAL_ID_PREFIX",
    "AGENT_CREDENTIAL_ISSUE_ANY_GRANT",
    "AGENT_CREDENTIAL_REVOKE_ANY_GRANT",
    "AGENT_CREDENTIAL_SECRET_BYTES",
    "AGENT_CREDENTIAL_TOKEN_PREFIX",
    "ParsedAgentCredentialToken",
    "format_agent_credential_token",
    "generate_agent_credential_id",
    "generate_agent_credential_secret",
    "hash_agent_credential_secret",
    "issue_agent_credential_token",
    "parse_agent_credential_token",
    "verify_agent_credential_secret",
]
