from __future__ import annotations

import asyncio
import logging
import os
import shlex
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.errors import BadRequestError, NotFoundError
from infra.sandbox_registry import SandboxRegistry

logger = logging.getLogger("E2BExecutor")

def _safe_relpath(path: str) -> str:
    raw = (path or "").strip().lstrip("/")
    if not raw:
        raise BadRequestError("path is empty")
    if ".." in raw.split("/"):
        raise BadRequestError("path traversal detected")
    return raw


def _truncate_text(text: str, max_bytes: int) -> str:
    if not isinstance(text, str):
        text = str(text)
    data = text.encode("utf-8")
    if len(data) <= max_bytes:
        return text
    return data[:max_bytes].decode("utf-8", errors="ignore")


@dataclass
class E2BResult:
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: int
    created_artifacts: List[Dict[str, Any]]
    output_errors: List[Dict[str, Any]]


class E2BExecutor:
    def __init__(
        self,
        *,
        artifact_manager: Any,
        template: Optional[str],
        sandbox_registry: Optional[SandboxRegistry] = None,
        reuse_mode: Optional[str] = None,
        idle_ttl_sec: Optional[int] = None,
        hard_ttl_sec: Optional[int] = None,
        lock_timeout_sec: Optional[int] = None,
        domain: Optional[str] = None,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        max_input_bytes: int,
        max_output_bytes: int,
        max_total_input_bytes: int,
        max_total_output_bytes: int,
        timeout_sec_default: int,
        timeout_sec_max: int,
    ):
        self.artifact_manager = artifact_manager
        self.template = template.strip() if isinstance(template, str) and template.strip() else None
        self.sandbox_registry = sandbox_registry
        if self.sandbox_registry and reuse_mode:
            self.sandbox_registry.reuse_mode = reuse_mode
        if self.sandbox_registry and idle_ttl_sec is not None:
            self.sandbox_registry.idle_ttl_sec = int(idle_ttl_sec)
        if self.sandbox_registry and hard_ttl_sec is not None:
            self.sandbox_registry.hard_ttl_sec = int(hard_ttl_sec)
        if self.sandbox_registry and lock_timeout_sec is not None:
            self.sandbox_registry.lock_timeout_sec = int(lock_timeout_sec)
        domain_val = domain or os.getenv("E2B_DOMAIN")
        self.domain = domain_val.strip() if isinstance(domain_val, str) and domain_val.strip() else None
        self.max_stdout_bytes = int(max_stdout_bytes)
        self.max_stderr_bytes = int(max_stderr_bytes)
        self.max_input_bytes = int(max_input_bytes)
        self.max_output_bytes = int(max_output_bytes)
        self.max_total_input_bytes = int(max_total_input_bytes)
        self.max_total_output_bytes = int(max_total_output_bytes)
        self.timeout_sec_default = int(timeout_sec_default)
        self.timeout_sec_max = int(timeout_sec_max)

    async def execute(
        self,
        *,
        project_id: str,
        agent_id: Optional[str],
        tool_call_id: Optional[str],
        session_id: Optional[str],
        language: str,
        code: str,
        inputs: List[Dict[str, Any]] | None,
        outputs: List[Dict[str, Any]] | None,
        timeout_sec: Optional[int],
        workdir: Optional[str],
        extra_files: Optional[List[Dict[str, Any]]] = None,
    ) -> E2BResult:
        lang = (language or "").strip().lower()
        if lang not in {"python", "bash", "javascript", "typescript"}:
            raise BadRequestError("unsupported language")
        if not isinstance(code, str) or not code.strip():
            raise BadRequestError("code is empty")
        workdir_val = _safe_relpath(workdir) if workdir else "."
        timeout_val = int(timeout_sec) if timeout_sec is not None else self.timeout_sec_default
        if timeout_val <= 0:
            timeout_val = self.timeout_sec_default
        timeout_val = min(timeout_val, self.timeout_sec_max)

        sandbox_id: Optional[str] = None
        sandbox_instance: Optional[Any] = None
        keep_alive = False
        reuse_enabled = bool(self.sandbox_registry and self.sandbox_registry.enabled and agent_id)
        lock_id = str(tool_call_id) if tool_call_id else ""
        if reuse_enabled and self.sandbox_registry:
            existing = await self.sandbox_registry.get_existing(
                project_id=project_id, agent_id=str(agent_id)
            )
            if existing and self.sandbox_registry.is_expired(existing):
                logger.info(
                    "E2B reuse: expired sandbox %s (project=%s agent=%s), removing",
                    existing.get("sandbox_id"),
                    project_id,
                    agent_id,
                )
                await asyncio.to_thread(
                    self._kill_sandbox_id,
                    str(existing.get("sandbox_id")),
                    existing.get("domain") or self.domain,
                )
                await self.sandbox_registry.store.delete_sandbox(
                    project_id=project_id, agent_id=str(agent_id)
                )
                existing = None
            if existing:
                sandbox_id = str(existing.get("sandbox_id"))
                logger.info(
                    "E2B reuse: using sandbox %s (project=%s agent=%s)",
                    sandbox_id,
                    project_id,
                    agent_id,
                )
                await self.sandbox_registry.acquire_lock(
                    project_id=project_id,
                    agent_id=str(agent_id),
                    lock_id=lock_id,
                )
            else:
                sandbox_instance = await asyncio.to_thread(self._create_sandbox_instance, timeout_val)
                new_id = getattr(sandbox_instance, "sandbox_id", None)
                if not new_id:
                    raise BadRequestError("failed to create sandbox")
                logger.info(
                    "E2B reuse: created sandbox %s (project=%s agent=%s)",
                    new_id,
                    project_id,
                    agent_id,
                )
                record = await self.sandbox_registry.register_new(
                    project_id=project_id,
                    agent_id=str(agent_id),
                    sandbox_id=new_id,
                    domain=self.domain,
                    template=self.template,
                    lock_id=lock_id or None,
                )
                sandbox_id = record.sandbox_id
                keep_alive = True

        input_items = inputs if isinstance(inputs, list) else []
        output_items = outputs if isinstance(outputs, list) else []

        total_input = 0
        prepared_inputs: List[Dict[str, Any]] = []
        for item in input_items:
            if not isinstance(item, dict):
                continue
            artifact_id = item.get("artifact_id")
            mount_path = item.get("mount_path")
            if not isinstance(artifact_id, str) or not artifact_id.strip():
                raise BadRequestError("inputs.artifact_id is required")
            if not isinstance(mount_path, str) or not mount_path.strip():
                raise BadRequestError("inputs.mount_path is required")
            mount_path = _safe_relpath(mount_path)
            data, meta = await self.artifact_manager.read_bytes(
                project_id=project_id, artifact_id=artifact_id
            )
            size = len(data)
            if size > self.max_input_bytes:
                raise BadRequestError("input file exceeds size limit")
            total_input += size
            if total_input > self.max_total_input_bytes:
                raise BadRequestError("total input size exceeds limit")
            prepared_inputs.append({"path": mount_path, "data": data})

        start = time.monotonic()
        try:
            stdout, stderr, exit_code, output_blobs = await asyncio.to_thread(
                self._run_in_sandbox,
                lang,
                code,
                prepared_inputs,
                extra_files or [],
                output_items,
                timeout_val,
                workdir_val,
                sandbox_id,
                sandbox_instance,
                keep_alive,
            )
        except Exception as exc:  # noqa: BLE001
            if reuse_enabled and sandbox_id and self._is_missing_sandbox_error(exc):
                logger.warning(
                    "E2B reuse: sandbox %s missing; recreating (project=%s agent=%s)",
                    sandbox_id,
                    project_id,
                    agent_id,
                )
                # Clear stale registry entry and retry once with a fresh sandbox.
                if self.sandbox_registry and agent_id:
                    await self.sandbox_registry.store.delete_sandbox(
                        project_id=project_id, agent_id=str(agent_id)
                    )
                sandbox_instance = await asyncio.to_thread(self._create_sandbox_instance, timeout_val)
                new_id = getattr(sandbox_instance, "sandbox_id", None)
                if not new_id:
                    raise
                logger.info(
                    "E2B reuse: recreated sandbox %s (project=%s agent=%s)",
                    new_id,
                    project_id,
                    agent_id,
                )
                if self.sandbox_registry and agent_id:
                    await self.sandbox_registry.register_new(
                        project_id=project_id,
                        agent_id=str(agent_id),
                        sandbox_id=new_id,
                        domain=self.domain,
                        template=self.template,
                        lock_id=lock_id or None,
                    )
                stdout, stderr, exit_code, output_blobs = await asyncio.to_thread(
                    self._run_in_sandbox,
                    lang,
                    code,
                    prepared_inputs,
                    extra_files or [],
                    output_items,
                    timeout_val,
                    workdir_val,
                    new_id,
                    sandbox_instance,
                    True,
                )
            else:
                raise
        finally:
            if reuse_enabled and self.sandbox_registry and agent_id:
                await self.sandbox_registry.touch(project_id=project_id, agent_id=str(agent_id))
                await self.sandbox_registry.release_lock(
                    project_id=project_id,
                    agent_id=str(agent_id),
                    lock_id=lock_id,
                )
        duration_ms = int((time.monotonic() - start) * 1000)

        created: List[Dict[str, Any]] = []
        output_errors: List[Dict[str, Any]] = []
        total_output = 0
        for output in output_blobs:
            if "error" in output:
                output_errors.append(output)
                continue
            blob = output.get("data")
            path = output.get("path")
            if not isinstance(blob, (bytes, bytearray)):
                output_errors.append({"path": path, "error": "output is empty"})
                continue
            size = len(blob)
            if size > self.max_output_bytes:
                output_errors.append({"path": path, "error": "output exceeds size limit"})
                continue
            total_output += size
            if total_output > self.max_total_output_bytes:
                output_errors.append({"path": path, "error": "total output exceeds size limit"})
                continue
            save_as = output.get("save_as") if isinstance(output.get("save_as"), str) else None
            filename = save_as or os.path.basename(path or "output.dat")
            artifact = await self.artifact_manager.save_bytes(
                project_id=project_id,
                filename=filename,
                data=bytes(blob),
                mime=None,
                metadata={"source_path": path},
            )
            created.append(
                {
                    "artifact_id": artifact.get("artifact_id"),
                    "path": path,
                    "filename": artifact.get("filename"),
                    "mime": artifact.get("mime"),
                    "size": artifact.get("size"),
                }
            )

        return E2BResult(
            stdout=_truncate_text(stdout, self.max_stdout_bytes),
            stderr=_truncate_text(stderr, self.max_stderr_bytes),
            exit_code=int(exit_code or 0),
            duration_ms=duration_ms,
            created_artifacts=created,
            output_errors=output_errors,
        )

    async def prepare_remote_zip(
        self,
        *,
        project_id: str,
        agent_id: Optional[str],
        tool_call_id: Optional[str],
        zip_bytes: bytes,
        dest_dir: str = "skill",
        zip_path: str = "/tmp/skill.zip",
        unzip: bool = True,
        keep_archive: bool = False,
        timeout_sec: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not isinstance(zip_bytes, (bytes, bytearray)) or not zip_bytes:
            raise BadRequestError("zip_bytes is empty")
        if not isinstance(dest_dir, str) or not dest_dir.strip():
            raise BadRequestError("dest_dir is required")
        if not isinstance(zip_path, str) or not zip_path.strip():
            raise BadRequestError("zip_path is required")

        timeout_val = int(timeout_sec) if timeout_sec is not None else self.timeout_sec_default
        if timeout_val <= 0:
            timeout_val = self.timeout_sec_default
        timeout_val = min(timeout_val, self.timeout_sec_max)

        dest_rel = _safe_relpath(dest_dir)
        dest_abs = f"/root/{dest_rel.as_posix()}"
        zip_abs = zip_path if zip_path.startswith("/") else f"/tmp/{zip_path.lstrip('/')}"

        sandbox_id: Optional[str] = None
        sandbox_instance: Optional[Any] = None
        keep_alive = True
        reuse_enabled = bool(self.sandbox_registry and self.sandbox_registry.enabled and agent_id)
        lock_id = str(tool_call_id) if tool_call_id else ""

        if reuse_enabled and self.sandbox_registry:
            existing = await self.sandbox_registry.get_existing(
                project_id=project_id, agent_id=str(agent_id)
            )
            if existing and self.sandbox_registry.is_expired(existing):
                logger.info(
                    "E2B reuse: expired sandbox %s (project=%s agent=%s), removing",
                    existing.get("sandbox_id"),
                    project_id,
                    agent_id,
                )
                await asyncio.to_thread(
                    self._kill_sandbox_id,
                    str(existing.get("sandbox_id")),
                    existing.get("domain") or self.domain,
                )
                await self.sandbox_registry.store.delete_sandbox(
                    project_id=project_id, agent_id=str(agent_id)
                )
                existing = None
            if existing:
                sandbox_id = str(existing.get("sandbox_id"))
                logger.info(
                    "E2B reuse: using sandbox %s (project=%s agent=%s)",
                    sandbox_id,
                    project_id,
                    agent_id,
                )
                await self.sandbox_registry.acquire_lock(
                    project_id=project_id,
                    agent_id=str(agent_id),
                    lock_id=lock_id,
                )
            else:
                sandbox_instance = await asyncio.to_thread(self._create_sandbox_instance, timeout_val)
                new_id = getattr(sandbox_instance, "sandbox_id", None)
                if not new_id:
                    raise BadRequestError("failed to create sandbox")
                logger.info(
                    "E2B reuse: created sandbox %s (project=%s agent=%s)",
                    new_id,
                    project_id,
                    agent_id,
                )
                record = await self.sandbox_registry.register_new(
                    project_id=project_id,
                    agent_id=str(agent_id),
                    sandbox_id=new_id,
                    domain=self.domain,
                    template=self.template,
                    lock_id=lock_id or None,
                )
                sandbox_id = record.sandbox_id

        try:
            sandbox = sandbox_instance
            if sandbox is None:
                if sandbox_id:
                    sandbox = self._connect_sandbox(sandbox_id, timeout_val)
                else:
                    sandbox = self._create_sandbox_instance(timeout_val)
                    sandbox_id = getattr(sandbox, "sandbox_id", None)
                    if sandbox_id is None:
                        raise BadRequestError("failed to create sandbox")

            dir_name = os.path.dirname(zip_abs)
            if dir_name:
                self._run_shell(sandbox, f"mkdir -p {shlex.quote(dir_name)}", timeout_val)
            self._write_file(sandbox, zip_abs, bytes(zip_bytes))

            if unzip:
                self._run_shell(sandbox, f"mkdir -p {shlex.quote(dest_abs)}", timeout_val)
                self._run_shell(
                    sandbox,
                    f"unzip -q {shlex.quote(zip_abs)} -d {shlex.quote(dest_abs)}",
                    timeout_val,
                )
                if not keep_archive:
                    self._run_shell(sandbox, f"rm -f {shlex.quote(zip_abs)}", timeout_val)
        except Exception as exc:  # noqa: BLE001
            if reuse_enabled and sandbox_id and self._is_missing_sandbox_error(exc):
                logger.warning(
                    "E2B reuse: sandbox %s missing; recreating (project=%s agent=%s)",
                    sandbox_id,
                    project_id,
                    agent_id,
                )
                sandbox_instance = await asyncio.to_thread(self._create_sandbox_instance, timeout_val)
                new_id = getattr(sandbox_instance, "sandbox_id", None)
                if not new_id:
                    raise BadRequestError("failed to recreate sandbox") from exc
                if self.sandbox_registry:
                    await self.sandbox_registry.register_new(
                        project_id=project_id,
                        agent_id=str(agent_id),
                        sandbox_id=new_id,
                        domain=self.domain,
                        template=self.template,
                        lock_id=lock_id or None,
                    )
                sandbox_id = new_id
                sandbox = sandbox_instance
                dir_name = os.path.dirname(zip_abs)
                if dir_name:
                    self._run_shell(sandbox, f"mkdir -p {shlex.quote(dir_name)}", timeout_val)
                self._write_file(sandbox, zip_abs, bytes(zip_bytes))
                if unzip:
                    self._run_shell(sandbox, f"mkdir -p {shlex.quote(dest_abs)}", timeout_val)
                    self._run_shell(
                        sandbox,
                        f"unzip -q {shlex.quote(zip_abs)} -d {shlex.quote(dest_abs)}",
                        timeout_val,
                    )
                    if not keep_archive:
                        self._run_shell(sandbox, f"rm -f {shlex.quote(zip_abs)}", timeout_val)
            else:
                raise
        finally:
            if reuse_enabled and self.sandbox_registry and agent_id:
                await self.sandbox_registry.touch(project_id=project_id, agent_id=str(agent_id))
                await self.sandbox_registry.release_lock(
                    project_id=project_id,
                    agent_id=str(agent_id),
                    lock_id=lock_id,
                )

        return {
            "sandbox_id": sandbox_id,
            "dest_dir": dest_abs,
            "zip_path": zip_abs,
            "unzip": unzip,
            "keep_archive": keep_archive,
        }

    def _run_in_sandbox(
        self,
        language: str,
        code: str,
        inputs: List[Dict[str, Any]],
        extra_files: List[Dict[str, Any]],
        outputs: List[Dict[str, Any]],
        timeout_sec: int,
        workdir: str,
        sandbox_id: Optional[str],
        sandbox_instance: Optional[Any],
        keep_alive: bool,
    ) -> tuple[str, str, int, List[Dict[str, Any]]]:
        try:
            from e2b_code_interpreter import Sandbox  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise BadRequestError(f"E2B SDK unavailable: {exc}") from exc

        stdout = ""
        stderr = ""
        exit_code = 0
        output_blobs: List[Dict[str, Any]] = []

        sandbox = None
        owns_sandbox = False
        try:
            if sandbox_instance is not None:
                sandbox = sandbox_instance
                owns_sandbox = False
            elif sandbox_id:
                sandbox = self._connect_sandbox(sandbox_id, timeout_sec)
            else:
                sandbox = Sandbox.create(
                    template=self.template,
                    timeout=timeout_sec,
                    **self._api_opts(),
                )
                owns_sandbox = True

            for item in inputs:
                path = item["path"]
                data = item["data"]
                dir_name = os.path.dirname(path)
                if dir_name:
                    self._run_shell(sandbox, f"mkdir -p {shlex.quote(dir_name)}", timeout_sec)
                self._write_file(sandbox, path, data)

            for item in extra_files:
                path = item.get("path")
                data = item.get("data")
                if not isinstance(path, str) or not isinstance(data, (bytes, bytearray)):
                    continue
                path = _safe_relpath(path)
                dir_name = os.path.dirname(path)
                if dir_name:
                    self._run_shell(sandbox, f"mkdir -p {shlex.quote(dir_name)}", timeout_sec)
                self._write_file(sandbox, path, bytes(data))

            if workdir and workdir not in (".", "/"):
                self._run_shell(sandbox, f"mkdir -p {shlex.quote(workdir)}", timeout_sec)

            if language == "bash":
                result = self._run_shell(sandbox, code, timeout_sec, workdir=workdir)
            else:
                result = self._run_code(sandbox, code, language, timeout_sec, workdir=workdir)

            stdout = getattr(result, "stdout", "") or ""
            stderr = getattr(result, "stderr", "") or ""
            logs = getattr(result, "logs", None)
            if logs:
                stdout = stdout or "".join(getattr(logs, "stdout", []) or [])
                stderr = stderr or "".join(getattr(logs, "stderr", []) or [])
            exit_code_val = getattr(result, "exit_code", None)
            if exit_code_val is None:
                exit_code_val = 1 if getattr(result, "error", None) else 0
            exit_code = int(exit_code_val or 0)

            for item in outputs or []:
                if not isinstance(item, dict):
                    continue
                path = item.get("path")
                if not isinstance(path, str) or not path.strip():
                    continue
                path = _safe_relpath(path)
                if workdir and workdir not in (".", "/"):
                    path = _safe_relpath(f"{workdir}/{path}")
                try:
                    data = self._read_file(sandbox, path)
                    output_blobs.append(
                        {"path": path, "save_as": item.get("save_as"), "data": data}
                    )
                except Exception as exc:  # noqa: BLE001
                    output_blobs.append({"path": path, "error": str(exc)})
        finally:
            if sandbox is not None and owns_sandbox and not keep_alive:
                try:
                    sandbox.kill()
                except Exception:
                    pass

        return stdout, stderr, exit_code, output_blobs

    def _connect_sandbox(self, sandbox_id: str, timeout_sec: int) -> Any:
        try:
            from e2b_code_interpreter import Sandbox  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise BadRequestError(f"E2B SDK unavailable: {exc}") from exc

        try:
            return Sandbox.connect(sandbox_id, timeout=timeout_sec, **self._api_opts())
        except Exception as exc:  # noqa: BLE001
            if not self._is_missing_sandbox_error(exc):
                raise

        try:
            from e2b.sandbox_sync.sandbox_api import SandboxApi  # type: ignore
            from e2b.sandbox_sync.main import Sandbox as BaseSandbox  # type: ignore
            from e2b.connection_config import ConnectionConfig  # type: ignore
            from packaging.version import Version  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise BadRequestError(f"E2B SDK unavailable: {exc}") from exc

        info = SandboxApi._cls_get_info(sandbox_id, **self._api_opts())
        state = getattr(info, "state", None)
        if hasattr(state, "value"):
            state = state.value
        if isinstance(state, str) and state.lower() == "paused":
            self._resume_sandbox(sandbox_id, timeout_sec)
            try:
                info = SandboxApi._cls_get_info(sandbox_id, **self._api_opts())
            except Exception:
                pass
        envd_access_token = getattr(info, "_envd_access_token", None)
        extra_headers = {
            "E2b-Sandbox-Id": sandbox_id,
            "E2b-Sandbox-Port": str(ConnectionConfig.envd_port),
        }
        if isinstance(envd_access_token, str) and envd_access_token:
            extra_headers["X-Access-Token"] = envd_access_token
        connection_config = ConnectionConfig(extra_sandbox_headers=extra_headers, **self._api_opts())
        sandbox_domain = info.sandbox_domain or self.domain
        return BaseSandbox(
            sandbox_id=sandbox_id,
            sandbox_domain=sandbox_domain,
            envd_version=Version(info.envd_version),
            envd_access_token=envd_access_token if isinstance(envd_access_token, str) else None,
            traffic_access_token=None,
            connection_config=connection_config,
        )

    def _resume_sandbox(self, sandbox_id: str, timeout_sec: int) -> None:
        try:
            import httpx
            from e2b.connection_config import ConnectionConfig  # type: ignore
        except Exception as exc:  # noqa: BLE001
            logger.warning("E2B resume: SDK unavailable: %s", exc)
            return

        config = ConnectionConfig(**self._api_opts())
        if not config.api_key:
            logger.warning("E2B resume: missing API key")
            return
        api_url = (config.api_url or "").rstrip("/")
        if not api_url:
            logger.warning("E2B resume: missing API url")
            return
        headers = {
            "X-API-KEY": config.api_key,
            "Content-Type": "application/json",
        }
        payload = {"timeout": int(timeout_sec), "autoPause": False}
        try:
            resp = httpx.post(
                f"{api_url}/sandboxes/{sandbox_id}/resume",
                headers=headers,
                json=payload,
                timeout=30.0,
            )
            if resp.status_code not in (201, 204, 409):
                logger.warning(
                    "E2B resume: failed sandbox=%s status=%s body=%s",
                    sandbox_id,
                    resp.status_code,
                    resp.text,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("E2B resume: error sandbox=%s err=%s", sandbox_id, exc)

    def _api_opts(self) -> Dict[str, Any]:
        opts: Dict[str, Any] = {}
        if self.domain:
            opts["domain"] = self.domain
        return opts

    def _create_sandbox_instance(self, timeout_sec: int) -> Any:
        try:
            from e2b_code_interpreter import Sandbox  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise BadRequestError(f"E2B SDK unavailable: {exc}") from exc
        return Sandbox.create(
            template=self.template,
            timeout=timeout_sec,
            **self._api_opts(),
        )

    def _kill_sandbox_id(self, sandbox_id: str, domain: Optional[str]) -> None:
        if not sandbox_id:
            return
        try:
            from e2b_code_interpreter import Sandbox  # type: ignore
        except Exception as exc:  # noqa: BLE001
            raise BadRequestError(f"E2B SDK unavailable: {exc}") from exc
        opts: Dict[str, Any] = {}
        if domain:
            opts["domain"] = domain
        sandbox = Sandbox.connect(sandbox_id, **opts)
        sandbox.kill()

    def _is_missing_sandbox_error(self, exc: Exception) -> bool:
        msg = str(exc)
        return "404" in msg or "Not Found" in msg or "not found" in msg or "sandbox" in msg and "not found" in msg

    def _run_shell(self, sandbox: Any, command: str, timeout_sec: int, *, workdir: str | None = None) -> Any:
        cmd = command
        if workdir and workdir not in (".", "/"):
            cmd = f"cd {shlex.quote(workdir)} && {command}"
        if hasattr(sandbox, "run"):
            return sandbox.run(cmd, language="bash", timeout=timeout_sec)
        if hasattr(sandbox, "commands"):
            return sandbox.commands.run(cmd, timeout=timeout_sec)
        raise BadRequestError("E2B sandbox does not support shell execution")

    def _run_code(self, sandbox: Any, code: str, language: str, timeout_sec: int, *, workdir: str | None = None) -> Any:
        if hasattr(sandbox, "run_code"):
            return sandbox.run_code(code, language=language, timeout=timeout_sec)
        if hasattr(sandbox, "run"):
            return sandbox.run(code, language=language, timeout=timeout_sec, workdir=workdir)
        if hasattr(sandbox, "notebook") and hasattr(sandbox.notebook, "exec_cell"):
            return sandbox.notebook.exec_cell(code)
        raise BadRequestError("E2B sandbox does not support code execution")

    def _write_file(self, sandbox: Any, path: str, data: bytes) -> None:
        if hasattr(sandbox, "files"):
            files = sandbox.files
            if hasattr(files, "write"):
                files.write(path, data)
                return
        raise BadRequestError("E2B sandbox does not support file write")

    def _read_file(self, sandbox: Any, path: str) -> bytes:
        if hasattr(sandbox, "files"):
            files = sandbox.files
            if hasattr(files, "read_bytes"):
                return files.read_bytes(path)
            if hasattr(files, "read"):
                try:
                    content = files.read(path, format="bytes")
                except TypeError:
                    content = files.read(path)
                if isinstance(content, str):
                    raise BadRequestError("sandbox returned text for binary output")
                if isinstance(content, bytearray):
                    return bytes(content)
                if isinstance(content, bytes):
                    return content
        raise NotFoundError(f"output not found: {path}")
