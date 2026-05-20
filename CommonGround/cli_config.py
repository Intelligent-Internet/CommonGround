from __future__ import annotations

import json
import os
import fcntl
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Mapping


DEFAULT_CONFIG_PATH = Path("~/.config/commonground/config.json").expanduser()


@dataclass(frozen=True, slots=True)
class CliConfigAuth:
    token: str | None = None
    token_file: str | None = None


@dataclass(frozen=True, slots=True)
class CliConfigAdminAuth:
    token: str | None = None
    token_file: str | None = None


@dataclass(frozen=True, slots=True)
class CliConfigCaller:
    project_id: str | None = None
    agent_id: str | None = None


@dataclass(frozen=True, slots=True)
class CliConfigProfile:
    project_id: str
    agent_id: str
    profile_kind: str | None = None
    runtime_kind: str | None = None
    display_name: str | None = None
    credential_id: str | None = None
    token_file: str | None = None
    status: str | None = None


@dataclass(frozen=True, slots=True)
class CliConfigFile:
    base_url: str | None = None
    admin_base_url: str | None = None
    auth: CliConfigAuth = CliConfigAuth()
    admin_auth: CliConfigAdminAuth = CliConfigAdminAuth()
    caller: CliConfigCaller = CliConfigCaller()
    profiles: Mapping[str, CliConfigProfile] | None = None


@dataclass(frozen=True, slots=True)
class ResolvedCliConfig:
    base_url: str
    admin_base_url: str | None
    auth_token: str | None
    admin_auth_token: str | None
    caller_project_id: str | None
    caller_agent_id: str | None
    config_path: Path | None
    write_config_path: Path
    profile_name: str | None
    profiles: Mapping[str, CliConfigProfile]


@dataclass(frozen=True, slots=True)
class WrittenCliClientConfig:
    config_path: str
    base_url: str
    admin_base_url: str
    admin_auth_token_file: str

    def to_payload(self) -> dict[str, str]:
        return {
            "config_path": self.config_path,
            "base_url": self.base_url,
            "admin_base_url": self.admin_base_url,
            "admin_auth_token_file": self.admin_auth_token_file,
        }


def resolve_cli_config(
    args: Any,
    *,
    default_base_url: str,
    resolve_auth: bool = True,
    resolve_admin_auth: bool = True,
    resolve_profile: bool = True,
    resolve_caller: bool = True,
) -> ResolvedCliConfig:
    config_path = _resolve_config_path(getattr(args, "config", None))
    config = _load_config_file(config_path)
    base_url = (
        _non_empty(getattr(args, "base_url", None))
        or _non_empty(os.environ.get("CG_BASE_URL"))
        or _non_empty(config.base_url)
        or default_base_url
    )
    admin_base_url = (
        _non_empty(getattr(args, "admin_base_url", None))
        or _non_empty(os.environ.get("CG_ADMIN_BASE_URL"))
        or _non_empty(config.admin_base_url)
    )
    auth_token = _resolve_auth_token(args, config) if resolve_auth else None
    admin_auth_token = _resolve_admin_auth_token(args, config) if resolve_admin_auth else None
    caller_project_id = None
    caller_agent_id = None
    if resolve_caller:
        caller_project_id = (
            _non_empty(getattr(args, "caller_project_id", None))
            or _non_empty(os.environ.get("CG_CALLER_PROJECT_ID"))
            or _non_empty(config.caller.project_id)
        )
        caller_agent_id = (
            _non_empty(getattr(args, "caller_agent_id", None))
            or _non_empty(os.environ.get("CG_CALLER_AGENT_ID"))
            or _non_empty(config.caller.agent_id)
        )
    return ResolvedCliConfig(
        base_url=base_url,
        admin_base_url=admin_base_url,
        auth_token=auth_token,
        admin_auth_token=admin_auth_token,
        caller_project_id=caller_project_id,
        caller_agent_id=caller_agent_id,
        config_path=config_path,
        write_config_path=_resolve_write_config_path(getattr(args, "config", None)),
        profile_name=_non_empty(getattr(args, "profile", None)) if resolve_profile else None,
        profiles=config.profiles or {},
    )


def write_cli_client_config(
    path: Path,
    *,
    base_url: str,
    admin_base_url: str,
    admin_auth_token_file: Path,
) -> WrittenCliClientConfig:
    path = path.expanduser()
    admin_auth_token_file = admin_auth_token_file.expanduser()
    base_url = _required_string("base_url", base_url)
    admin_base_url = _required_string("admin_base_url", admin_base_url)
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            payload = _load_raw_config_for_write(path)
            payload["base_url"] = base_url
            payload["admin_base_url"] = admin_base_url
            admin_auth = payload.setdefault("admin_auth", {})
            if not isinstance(admin_auth, dict):
                raise ValueError("CLI config admin_auth must contain a JSON object")
            admin_auth.pop("token", None)
            admin_auth["token_file"] = str(admin_auth_token_file)
            _atomic_write_json(path, payload)
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    return WrittenCliClientConfig(
        config_path=str(path),
        base_url=base_url,
        admin_base_url=admin_base_url,
        admin_auth_token_file=str(admin_auth_token_file),
    )


def _resolve_auth_token(args: Any, config: CliConfigFile) -> str | None:
    direct = (
        _non_empty(getattr(args, "auth_token", None))
        or _non_empty(os.environ.get("CG_AGENT_CREDENTIAL_TOKEN"))
        or _non_empty(config.auth.token)
    )
    if direct:
        return direct
    token_file = (
        _non_empty(getattr(args, "auth_token_file", None))
        or _non_empty(os.environ.get("CG_AGENT_CREDENTIAL_TOKEN_FILE"))
        or _non_empty(config.auth.token_file)
    )
    if not token_file:
        return None
    return Path(token_file).expanduser().read_text(encoding="utf-8").strip() or None


def _resolve_admin_auth_token(args: Any, config: CliConfigFile) -> str | None:
    direct = (
        _non_empty(getattr(args, "admin_auth_token", None))
        or _non_empty(os.environ.get("CG_ADMIN_AUTH_TOKEN"))
        or _non_empty(config.admin_auth.token)
    )
    if direct:
        return direct
    token_file = (
        _non_empty(getattr(args, "admin_auth_token_file", None))
        or _non_empty(os.environ.get("CG_ADMIN_AUTH_TOKEN_FILE"))
        or _non_empty(config.admin_auth.token_file)
    )
    if not token_file:
        return None
    return Path(token_file).expanduser().read_text(encoding="utf-8").strip() or None


def _resolve_config_path(explicit: str | None) -> Path | None:
    candidate = _non_empty(explicit) or _non_empty(os.environ.get("CG_CONFIG_PATH"))
    if candidate:
        return Path(candidate).expanduser()
    return DEFAULT_CONFIG_PATH if DEFAULT_CONFIG_PATH.is_file() else None


def _resolve_write_config_path(explicit: str | None) -> Path:
    candidate = _non_empty(explicit) or _non_empty(os.environ.get("CG_CONFIG_PATH"))
    if candidate:
        return Path(candidate).expanduser()
    return DEFAULT_CONFIG_PATH


def _load_config_file(path: Path | None) -> CliConfigFile:
    if path is None:
        return CliConfigFile()
    if not path.is_file():
        return CliConfigFile()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("CLI config file must contain a JSON object")
    auth = payload.get("auth", {})
    if auth is None:
        auth = {}
    if not isinstance(auth, Mapping):
        raise ValueError("CLI config auth must contain a JSON object")
    caller = payload.get("caller", {})
    if caller is None:
        caller = {}
    if not isinstance(caller, Mapping):
        raise ValueError("CLI config caller must contain a JSON object")
    admin_auth = payload.get("admin_auth", {})
    if admin_auth is None:
        admin_auth = {}
    if not isinstance(admin_auth, Mapping):
        raise ValueError("CLI config admin_auth must contain a JSON object")
    profiles = payload.get("profiles", {})
    if profiles is None:
        profiles = {}
    if not isinstance(profiles, Mapping):
        raise ValueError("CLI config profiles must contain a JSON object")
    return CliConfigFile(
        base_url=_optional_string(payload.get("base_url")),
        admin_base_url=_optional_string(payload.get("admin_base_url")),
        auth=CliConfigAuth(
            token=_optional_string(auth.get("token")),
            token_file=_optional_string(auth.get("token_file")),
        ),
        admin_auth=CliConfigAdminAuth(
            token=_optional_string(admin_auth.get("token")),
            token_file=_optional_string(admin_auth.get("token_file")),
        ),
        caller=CliConfigCaller(
            project_id=_optional_string(caller.get("project_id")),
            agent_id=_optional_string(caller.get("agent_id")),
        ),
        profiles=_profiles_from_mapping(profiles),
    )


def _load_raw_config_for_write(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("CLI config file must contain a JSON object")
    return payload


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as stream:
        temp_path = Path(stream.name)
        json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp_path, path)


def _profiles_from_mapping(value: Mapping[str, Any]) -> Mapping[str, CliConfigProfile]:
    parsed: dict[str, CliConfigProfile] = {}
    for key, raw in value.items():
        if not isinstance(key, str) or not key:
            raise ValueError("CLI config profile keys must be non-empty strings")
        if not isinstance(raw, Mapping):
            raise ValueError("CLI config profile values must contain JSON objects")
        project_id = _optional_string(raw.get("project_id"))
        agent_id = _optional_string(raw.get("agent_id"))
        if project_id is None or agent_id is None:
            raise ValueError("CLI config profiles require project_id and agent_id")
        parsed[key] = CliConfigProfile(
            project_id=project_id,
            agent_id=agent_id,
            profile_kind=_optional_string(raw.get("profile_kind")),
            runtime_kind=_optional_string(raw.get("runtime_kind")),
            display_name=_optional_string(raw.get("display_name")),
            credential_id=_optional_string(raw.get("credential_id")),
            token_file=_optional_string(raw.get("token_file")),
            status=_optional_string(raw.get("status")),
        )
    return parsed


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("CLI config values must be strings when provided")
    return value or None


def _required_string(field_name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _non_empty(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
