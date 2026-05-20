from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
import secrets
from typing import Any

import psycopg
from psycopg import errors

from CommonGround.agent_credentials import parse_agent_credential_token, verify_agent_credential_secret
from CommonGround.app import build_kernel_app
from CommonGround.cli_profiles import read_token_file, write_token_file
from CommonGround.contracts import AGENT_CREDENTIAL_STATUS_ACTIVE, AgentRef, ConflictError, NotFoundError
from CommonGround.infra import PostgresAgentCredentialStore
from CommonGround.infra.postgres import ensure_schema as ensure_kernel_schema

from .project_bootstrap import (
    ADMIN_SERVICE_AGENT_ID,
    ADMIN_SERVICE_GRANTS,
    bootstrap_project_admin_service_agent,
    admin_service_bootstrap_spec_mismatches,
)


DEFAULT_LOCAL_PROJECT_ID = "cg-demo"
DEFAULT_LOCAL_CREATOR_REF = "local-dev"
DEFAULT_OPERATOR_DIR = Path("~/.local/share/commonground/operator").expanduser()
ADMIN_SERVICE_CREDENTIAL_PROVENANCE_KIND = "operator.project_setup.admin_service_credential.v1"


@dataclass(frozen=True, slots=True)
class ProjectSetupStatus:
    project_id: str
    seeded: bool
    matches_bootstrap_spec: bool
    mismatch_fields: tuple[str, ...]
    admin_service_credential_ready: bool
    admin_service_token_file: str | None
    admin_auth_ready: bool
    admin_auth_token_file: str | None
    admin_service: dict[str, Any] | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "seeded": self.seeded,
            "matches_bootstrap_spec": self.matches_bootstrap_spec,
            "mismatch_fields": list(self.mismatch_fields),
            "admin_service": self.admin_service,
            "admin_service_credential_ready": self.admin_service_credential_ready,
            "admin_service_token_file": self.admin_service_token_file,
            "admin_auth_ready": self.admin_auth_ready,
            "admin_auth_token_file": self.admin_auth_token_file,
        }


def default_admin_service_token_file(project_id: str) -> Path:
    project = safe_project_id(project_id)
    return DEFAULT_OPERATOR_DIR / "projects" / project / "admin-service.cgac"


def default_admin_auth_token_file(project_id: str) -> Path:
    project = safe_project_id(project_id)
    return DEFAULT_OPERATOR_DIR / "projects" / project / "admin-api-bearer.token"


def setup_project(
    *,
    pg_dsn: str,
    project_id: str = DEFAULT_LOCAL_PROJECT_ID,
    creator_ref: str = DEFAULT_LOCAL_CREATOR_REF,
    admin_service_token_file: Path | None = None,
    admin_auth_token_file: Path | None = None,
    rotate_admin_service_token: bool = False,
    rotate_admin_auth_token: bool = False,
) -> ProjectSetupStatus:
    project_id = safe_project_id(project_id)
    _ensure_local_operator_schema(pg_dsn)
    app = build_kernel_app(pg_dsn=pg_dsn)
    bootstrap_project_admin_service_agent(app, project_id=project_id, creator_ref=creator_ref)

    resolved_admin_service_token_file = admin_service_token_file or default_admin_service_token_file(project_id)
    resolved_admin_auth_token_file = admin_auth_token_file or default_admin_auth_token_file(project_id)
    _ensure_admin_service_credential(
        pg_dsn=pg_dsn,
        project_id=project_id,
        token_file=resolved_admin_service_token_file,
        rotate=rotate_admin_service_token,
    )
    _ensure_admin_auth_token_file(resolved_admin_auth_token_file, rotate=rotate_admin_auth_token)
    return project_status(
        pg_dsn=pg_dsn,
        project_id=project_id,
        admin_service_token_file=resolved_admin_service_token_file,
        admin_auth_token_file=resolved_admin_auth_token_file,
    )


def project_status(
    *,
    pg_dsn: str,
    project_id: str = DEFAULT_LOCAL_PROJECT_ID,
    admin_service_token_file: Path | None = None,
    admin_auth_token_file: Path | None = None,
) -> ProjectSetupStatus:
    project_id = safe_project_id(project_id)
    app = build_kernel_app(pg_dsn=pg_dsn)
    admin_ref = AgentRef(project_id=project_id, agent_id=ADMIN_SERVICE_AGENT_ID)
    try:
        snapshot = app.topology.get_agent(admin_ref)
    except errors.UndefinedTable:
        return ProjectSetupStatus(
            project_id=project_id,
            seeded=False,
            matches_bootstrap_spec=False,
            mismatch_fields=(),
            admin_service_credential_ready=False,
            admin_service_token_file=_path_string(admin_service_token_file),
            admin_auth_ready=_token_file_ready(admin_auth_token_file),
            admin_auth_token_file=_path_string(admin_auth_token_file),
            admin_service=None,
        )
    if snapshot is None:
        return ProjectSetupStatus(
            project_id=project_id,
            seeded=False,
            matches_bootstrap_spec=False,
            mismatch_fields=(),
            admin_service_credential_ready=False,
            admin_service_token_file=_path_string(admin_service_token_file),
            admin_auth_ready=_token_file_ready(admin_auth_token_file),
            admin_auth_token_file=_path_string(admin_auth_token_file),
            admin_service=None,
        )

    mismatches = admin_service_bootstrap_spec_mismatches(snapshot)
    credential_ready = False
    if admin_service_token_file is not None:
        credential_ready = _admin_service_token_file_ready(
            pg_dsn=pg_dsn,
            project_id=project_id,
            token_file=admin_service_token_file,
        )
    else:
        credential_ready = _has_active_admin_service_credential(pg_dsn=pg_dsn, project_id=project_id)
    return ProjectSetupStatus(
        project_id=project_id,
        seeded=True,
        matches_bootstrap_spec=not mismatches,
        mismatch_fields=mismatches,
        admin_service_credential_ready=credential_ready,
        admin_service_token_file=_path_string(admin_service_token_file),
        admin_auth_ready=_token_file_ready(admin_auth_token_file),
        admin_auth_token_file=_path_string(admin_auth_token_file),
        admin_service={
            "agent_id": snapshot.agent.agent_id,
            "role": snapshot.role,
            "accepts_work": snapshot.accepts_work,
            "capabilities": list(snapshot.capabilities),
            "grants": list(snapshot.grants),
            "public_metadata": dict(snapshot.public_metadata),
        },
    )


def ensure_seeded_or_raise(
    *,
    pg_dsn: str,
    project_id: str,
    admin_service_token_file: Path | None = None,
) -> ProjectSetupStatus:
    status = project_status(pg_dsn=pg_dsn, project_id=project_id, admin_service_token_file=admin_service_token_file)
    if not status.seeded:
        raise NotFoundError(f"project is not seeded: {project_id}")
    if not status.matches_bootstrap_spec:
        raise ConflictError("project admin-service bootstrap does not match expected spec: " + ", ".join(status.mismatch_fields))
    if admin_service_token_file is not None and not status.admin_service_credential_ready:
        raise ConflictError("admin-service AgentCredential is required")
    return status


def admin_service_credential_token_ready(*, pg_dsn: str, project_id: str, token: str | None) -> bool:
    if not token:
        return False
    try:
        parsed = parse_agent_credential_token(token)
    except ValueError:
        return False
    row = PostgresAgentCredentialStore(pg_dsn).load_agent_credential_by_id(parsed.credential_id)
    if row is None:
        return False
    now = datetime.now(UTC)
    return (
        row.project_id == project_id
        and row.agent_id == ADMIN_SERVICE_AGENT_ID
        and row.status == AGENT_CREDENTIAL_STATUS_ACTIVE
        and (row.expires_at is None or row.expires_at > now)
        and verify_agent_credential_secret(parsed.secret, row.secret_hash)
    )


def safe_project_id(project_id: str) -> str:
    if not isinstance(project_id, str) or not project_id.strip():
        raise ValueError("project_id must be non-empty")
    value = project_id.strip()
    if value in {".", ".."} or "/" in value or "\\" in value:
        raise ValueError("project_id must not contain path separators")
    return value


def _ensure_admin_service_credential(
    *,
    pg_dsn: str,
    project_id: str,
    token_file: Path,
    rotate: bool,
) -> None:
    if token_file.exists() and not rotate:
        if _admin_service_token_file_ready(pg_dsn=pg_dsn, project_id=project_id, token_file=token_file):
            return
        raise ConflictError("admin-service token file is not a valid active AgentCredential; pass --rotate-admin-service-token")
    store = PostgresAgentCredentialStore(pg_dsn)
    old_credential_id = None
    if token_file.exists() and rotate:
        old_credential_id = _active_admin_service_credential_id_from_file(
            pg_dsn=pg_dsn,
            project_id=project_id,
            token_file=token_file,
        )
    issued = store.issue_agent_credential(
        AgentRef(project_id=project_id, agent_id=ADMIN_SERVICE_AGENT_ID),
        provenance_kind=ADMIN_SERVICE_CREDENTIAL_PROVENANCE_KIND,
        provenance_ref=project_id,
    )
    write_token_file(token_file, issued.token)
    if old_credential_id is not None and old_credential_id != issued.ref.credential_id:
        store.revoke_agent_credential(old_credential_id)


def _ensure_local_operator_schema(pg_dsn: str) -> None:
    from .agent_join_invites import ensure_agent_join_invite_schema
    from .byoa_workflow import ensure_byoa_workflow_schema

    ensure_kernel_schema(pg_dsn)
    _ensure_cardbox_schema(pg_dsn)
    ensure_byoa_workflow_schema(pg_dsn)
    ensure_agent_join_invite_schema(pg_dsn)


def _ensure_cardbox_schema(pg_dsn: str) -> None:
    schema_sql = _cardbox_schema_sql()
    conn = psycopg.connect(pg_dsn, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
    finally:
        conn.close()


def _cardbox_schema_sql() -> str:
    return resources.files("cardbox.adapters").joinpath("postgres_schema.sql").read_text(encoding="utf-8")


def _ensure_admin_auth_token_file(token_file: Path, *, rotate: bool) -> None:
    if token_file.exists() and not rotate:
        if _token_file_ready(token_file):
            return
        raise ConflictError("Admin Service bearer token file is empty or has unsafe permissions")
    write_token_file(token_file, secrets.token_urlsafe(32))


def _admin_service_token_file_ready(*, pg_dsn: str, project_id: str, token_file: Path) -> bool:
    return _active_admin_service_credential_id_from_file(
        pg_dsn=pg_dsn,
        project_id=project_id,
        token_file=token_file,
    ) is not None


def _active_admin_service_credential_id_from_file(*, pg_dsn: str, project_id: str, token_file: Path) -> str | None:
    try:
        token = read_token_file(token_file)
    except (FileNotFoundError, PermissionError, ValueError):
        return None
    if not token:
        return None
    try:
        parsed = parse_agent_credential_token(token)
    except ValueError:
        return None
    row = PostgresAgentCredentialStore(pg_dsn).load_agent_credential_by_id(parsed.credential_id)
    if row is None:
        return None
    now = datetime.now(UTC)
    if (
        row.project_id == project_id
        and row.agent_id == ADMIN_SERVICE_AGENT_ID
        and row.status == AGENT_CREDENTIAL_STATUS_ACTIVE
        and (row.expires_at is None or row.expires_at > now)
        and verify_agent_credential_secret(parsed.secret, row.secret_hash)
    ):
        return row.credential_id
    return None


def _has_active_admin_service_credential(*, pg_dsn: str, project_id: str) -> bool:
    rows = PostgresAgentCredentialStore(pg_dsn).list_agent_credentials(
        AgentRef(project_id=project_id, agent_id=ADMIN_SERVICE_AGENT_ID)
    )
    now = datetime.now(UTC)
    return any(row.status == AGENT_CREDENTIAL_STATUS_ACTIVE and (row.expires_at is None or row.expires_at > now) for row in rows)


def _token_file_ready(token_file: Path | None) -> bool:
    if token_file is None:
        return False
    try:
        return bool(read_token_file(token_file))
    except (FileNotFoundError, PermissionError):
        return False


def _path_string(path: Path | None) -> str | None:
    return None if path is None else str(path.expanduser())


__all__ = [
    "DEFAULT_LOCAL_CREATOR_REF",
    "DEFAULT_LOCAL_PROJECT_ID",
    "ProjectSetupStatus",
    "default_admin_auth_token_file",
    "default_admin_service_token_file",
    "ensure_seeded_or_raise",
    "admin_service_credential_token_ready",
    "project_status",
    "safe_project_id",
    "setup_project",
]
