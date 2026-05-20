from __future__ import annotations

import contextlib
import fcntl
import json
import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterator, Mapping


DEFAULT_CREDENTIALS_DIR = Path("~/.local/share/commonground/credentials").expanduser()


@dataclass(frozen=True, slots=True)
class AgentProfile:
    project_id: str
    agent_id: str
    profile_kind: str
    runtime_kind: str
    display_name: str
    credential_id: str
    token_file: str
    status: str = "ready"

    @property
    def name(self) -> str:
        return profile_key(self.project_id, self.agent_id)


def profile_key(project_id: str, agent_id: str) -> str:
    return f"{_safe_path_part('project_id', project_id)}/{_safe_path_part('agent_id', agent_id)}"


def default_token_file(*, project_id: str, agent_id: str, credential_id: str) -> Path:
    return (
        DEFAULT_CREDENTIALS_DIR
        / _safe_path_part("project_id", project_id)
        / _safe_path_part("agent_id", agent_id)
        / f"{_safe_path_part('credential_id', credential_id)}.token"
    )


class CliProfileStore:
    def __init__(self, config_path: Path) -> None:
        self._config_path = config_path.expanduser()
        self._lock_path = self._config_path.with_name(f"{self._config_path.name}.lock")

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def raw_config(self) -> dict[str, Any]:
        if not self._config_path.is_file():
            return {}
        payload = json.loads(self._config_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("CLI config file must contain a JSON object")
        return payload

    def profile(self, name: str) -> AgentProfile | None:
        raw = self.raw_config().get("profiles", {})
        if not isinstance(raw, Mapping):
            raise ValueError("CLI config profiles must contain a JSON object")
        value = raw.get(name)
        if value is None:
            return None
        return agent_profile_from_mapping(value)

    def upsert_profile(self, profile: AgentProfile) -> None:
        payload = self.raw_config()
        profiles = payload.setdefault("profiles", {})
        if not isinstance(profiles, dict):
            raise ValueError("CLI config profiles must contain a JSON object")
        profiles[profile.name] = asdict(profile)
        self._atomic_write_json(payload)

    def upsert_connection_and_profile(self, *, base_url: str, admin_base_url: str, profile: AgentProfile) -> None:
        payload = self.raw_config()
        payload["base_url"] = _required_string("base_url", base_url)
        payload["admin_base_url"] = _required_string("admin_base_url", admin_base_url)
        profiles = payload.setdefault("profiles", {})
        if not isinstance(profiles, dict):
            raise ValueError("CLI config profiles must contain a JSON object")
        profiles[profile.name] = asdict(profile)
        self._atomic_write_json(payload)

    def _atomic_write_json(self, payload: Mapping[str, Any]) -> None:
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(self._config_path.parent),
            prefix=f".{self._config_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temp_path = Path(stream.name)
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, self._config_path)


def write_token_file(path: Path, token: str) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd: int | None = None
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp_fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as stream:
            tmp_fd = None
            stream.write(token.strip())
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
        os.chmod(path, 0o600)
    finally:
        if tmp_fd is not None:
            os.close(tmp_fd)
        if tmp_path.exists():
            tmp_path.unlink()


def read_token_file(path: str | Path) -> str | None:
    path = Path(path).expanduser()
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise PermissionError(f"token file permissions must be 0600: {path}")
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def agent_profile_from_mapping(value: Mapping[str, Any]) -> AgentProfile:
    return AgentProfile(
        project_id=_safe_path_part("project_id", value.get("project_id")),
        agent_id=_safe_path_part("agent_id", value.get("agent_id")),
        profile_kind=_required_string("profile_kind", value.get("profile_kind")),
        runtime_kind=_required_string("runtime_kind", value.get("runtime_kind")),
        display_name=_required_string("display_name", value.get("display_name")),
        credential_id=_safe_path_part("credential_id", value.get("credential_id")),
        token_file=_required_string("token_file", value.get("token_file")),
        status=_required_string("status", value.get("status") or "ready"),
    )


def _required_string(field_name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _safe_path_part(field_name: str, value: Any) -> str:
    text = _required_string(field_name, value)
    if text in {".", ".."} or "/" in text or "\\" in text:
        raise ValueError(f"{field_name} must not contain path separators")
    return text
