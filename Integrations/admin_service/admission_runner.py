from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import Request

from CommonGround.cli_profiles import read_token_file
from CommonGround.contracts import UnauthorizedError

from .byoa_facade import AdminServiceByoaFacade, ByoaJsonInviteValidator
from .project_setup import DEFAULT_LOCAL_PROJECT_ID, default_admin_auth_token_file, default_admin_service_token_file


DEFAULT_ADMISSION_HOST = "127.0.0.1"
DEFAULT_ADMISSION_PORT = 8001
DEFAULT_ADMISSION_USER_ID = "local-admin-service"


@dataclass(frozen=True, slots=True)
class LocalAdmissionSettings:
    pg_dsn: str
    base_url: str
    project_id: str
    admin_service_token_file: Path
    admin_auth_token_file: Path
    host: str = DEFAULT_ADMISSION_HOST
    port: int = DEFAULT_ADMISSION_PORT
    invite_config_json: Path | None = None
    log_level: str = "info"
    requester_user_id: str = DEFAULT_ADMISSION_USER_ID


def resolve_local_admission_settings(args: Any) -> LocalAdmissionSettings:
    project_id = _arg_env(args, "project_id", "CG_PROJECT_ID", DEFAULT_LOCAL_PROJECT_ID)
    assert project_id is not None
    admin_service_token_file = _optional_path(
        _arg_env(args, "admin_service_token_file", "CG_ADMIN_SERVICE_TOKEN_FILE", None)
    ) or default_admin_service_token_file(project_id)
    admin_auth_token_file = _optional_path(_arg_env(args, "admin_auth_token_file", "CG_ADMIN_AUTH_TOKEN_FILE", None)) or default_admin_auth_token_file(
        project_id
    )
    pg_dsn = _arg_env(args, "pg_dsn", "PG_DSN", None)
    if not pg_dsn:
        raise ValueError("PG_DSN or --pg-dsn is required for cg admission run")
    return LocalAdmissionSettings(
        pg_dsn=pg_dsn,
        base_url=_arg_env(args, "base_url", "CG_BASE_URL", "http://127.0.0.1:8000") or "http://127.0.0.1:8000",
        project_id=project_id,
        admin_service_token_file=admin_service_token_file,
        admin_auth_token_file=admin_auth_token_file,
        host=_arg_env(args, "host", "CG_ADMIN_HOST", DEFAULT_ADMISSION_HOST) or DEFAULT_ADMISSION_HOST,
        port=int(_arg_env(args, "port", "CG_ADMIN_PORT", str(DEFAULT_ADMISSION_PORT)) or DEFAULT_ADMISSION_PORT),
        invite_config_json=_optional_path(_arg_env(args, "invite_config_json", "CG_ADMIN_INVITE_CONFIG_JSON", None)),
        log_level=_arg_env(args, "log_level", "CG_ADMIN_LOG_LEVEL", "info") or "info",
    )


def create_local_admission_app(settings: LocalAdmissionSettings, *, prefix: str = "/admin/v1"):
    from .admission_api import create_agent_credential_token_request_app

    facade, join_invite_store = _local_admission_components(settings)
    admin_auth_token = _read_admin_auth_token(settings)
    app = create_agent_credential_token_request_app(
        facade,
        resolve_requester_user_id=lambda request: _authenticated_requester(settings, request, admin_auth_token),
        join_invite_store=join_invite_store,
        prefix=prefix,
    )
    return app


def create_local_admission_router(settings: LocalAdmissionSettings, *, prefix: str = "/admin/v1"):
    from .admission_api import create_agent_credential_token_request_router

    facade, join_invite_store = _local_admission_components(settings)
    admin_auth_token = _read_admin_auth_token(settings)
    return create_agent_credential_token_request_router(
        facade,
        resolve_requester_user_id=lambda request: _authenticated_requester(settings, request, admin_auth_token),
        join_invite_store=join_invite_store,
        prefix=prefix,
    )


def _local_admission_components(settings: LocalAdmissionSettings):
    from .agent_join_invites import AgentJoinInviteStore

    admin_service_token = read_token_file(settings.admin_service_token_file)
    if not admin_service_token:
        raise ValueError("admin-service token file is empty or unreadable")
    facade = AdminServiceByoaFacade(
        settings.pg_dsn,
        base_url=settings.base_url,
        admin_service_token=admin_service_token,
        authorize_request=lambda _requester_user_id, project_id: project_id == settings.project_id,
        validate_invitation=_invite_validator(settings.invite_config_json),
    )
    return facade, AgentJoinInviteStore(settings.pg_dsn)


def _read_admin_auth_token(settings: LocalAdmissionSettings) -> str:
    admin_auth_token = read_token_file(settings.admin_auth_token_file)
    if not admin_auth_token:
        raise ValueError("Admin Service bearer token file is empty or unreadable")
    return admin_auth_token


def _authenticated_requester(settings: LocalAdmissionSettings, request: Request, expected_token: str) -> str:
    authorization = request.headers.get("Authorization", "")
    if authorization != f"Bearer {expected_token}":
        raise UnauthorizedError("Admin Service bearer auth is required")
    return settings.requester_user_id


def _invite_validator(path: Path | None):
    if path is None:
        return None
    return ByoaJsonInviteValidator.from_json_file(path)


def _optional_path(value: str | None) -> Path | None:
    if not value:
        return None
    return Path(value).expanduser()


def _arg_env(args: Any, attr: str, env_name: str, default: str | None) -> str | None:
    value = getattr(args, attr, None)
    if value is not None:
        return str(value).strip()
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return env_value.strip()
    return default


__all__ = ["LocalAdmissionSettings", "create_local_admission_app", "create_local_admission_router", "resolve_local_admission_settings"]
