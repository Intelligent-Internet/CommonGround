from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import uuid6

from core.errors import BadRequestError


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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


_RUN_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _safe_run_id(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raise BadRequestError("session_id is empty")
    if not _RUN_ID_RE.match(raw):
        raise BadRequestError("session_id contains invalid characters")
    return raw


def _write_bash_script(*, base_dir: Path, code: str) -> Path:
    if not isinstance(code, str):
        code = str(code)
    script_path = (base_dir / "__srt_run.sh").resolve()
    content = "#!/usr/bin/env bash\n" + code
    if not content.endswith("\n"):
        content += "\n"
    script_path.write_text(content, encoding="utf-8")
    os.chmod(script_path, 0o700)
    return script_path


@dataclass
class SrtResult:
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: int
    created_artifacts: List[Dict[str, Any]]
    output_errors: List[Dict[str, Any]]


@dataclass
class SrtProcessHandle:
    proc: asyncio.subprocess.Process
    base_dir: Path
    output_items: List[Dict[str, Any]]
    settings_path: Optional[str]
    settings_mode: str
    start_ts: float


class SrtExecutor:
    def __init__(
        self,
        *,
        artifact_manager: Any,
        work_root: str,
        srt_cfg: Dict[str, Any],
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        max_input_bytes: int,
        max_output_bytes: int,
        max_total_input_bytes: int,
        max_total_output_bytes: int,
        timeout_sec_default: int,
        timeout_sec_max: int,
    ) -> None:
        self.artifact_manager = artifact_manager
        self.work_root = str(work_root or "/tmp/skills-local")
        self.srt_cfg = srt_cfg or {}
        self.max_stdout_bytes = int(max_stdout_bytes)
        self.max_stderr_bytes = int(max_stderr_bytes)
        self.max_input_bytes = int(max_input_bytes)
        self.max_output_bytes = int(max_output_bytes)
        self.max_total_input_bytes = int(max_total_input_bytes)
        self.max_total_output_bytes = int(max_total_output_bytes)
        self.timeout_sec_default = int(timeout_sec_default)
        self.timeout_sec_max = int(timeout_sec_max)

    def _build_settings(self, *, work_dir: Path, extra_allow_write: Optional[list[str]] = None) -> Dict[str, Any]:
        network_cfg = self.srt_cfg.get("network", {}) if isinstance(self.srt_cfg, dict) else {}
        fs_cfg = self.srt_cfg.get("filesystem", {}) if isinstance(self.srt_cfg, dict) else {}
        allow_write = []
        if isinstance(fs_cfg.get("allow_write"), list):
            allow_write.extend([str(p) for p in fs_cfg.get("allow_write")])
        if extra_allow_write:
            allow_write.extend([str(p) for p in extra_allow_write])
        allow_write.append(str(work_dir))
        settings = {
            "network": {
                "allowedDomains": list(network_cfg.get("allowed_domains", [])),
                "deniedDomains": list(network_cfg.get("denied_domains", [])),
                "allowUnixSockets": list(network_cfg.get("allow_unix_sockets", [])),
                "allowAllUnixSockets": bool(network_cfg.get("allow_all_unix_sockets", False)),
                "allowLocalBinding": bool(network_cfg.get("allow_local_binding", False)),
            },
            "filesystem": {
                "denyRead": list(fs_cfg.get("deny_read", [])),
                "allowWrite": allow_write,
                "denyWrite": list(fs_cfg.get("deny_write", [])),
            },
            "ignoreViolations": dict(self.srt_cfg.get("ignore_violations", {})),
            "enableWeakerNestedSandbox": bool(self.srt_cfg.get("enable_weaker_nested_sandbox", False)),
        }
        return settings

    async def _prepare_run(
        self,
        *,
        project_id: str,
        tool_call_id: Optional[str],
        session_id: Optional[str],
        inputs: List[Dict[str, Any]] | None,
        outputs: List[Dict[str, Any]] | None,
        workdir: Optional[str],
        extra_files: Optional[List[Dict[str, Any]]] = None,
    ) -> tuple[Path, Path, str, List[Dict[str, Any]], Optional[str], str]:
        if session_id:
            run_id = _safe_run_id(session_id)
        else:
            run_id = tool_call_id or uuid6.uuid7().hex
        base_dir = Path(self.work_root) / project_id / run_id
        base_dir.mkdir(parents=True, exist_ok=True)
        workdir_val = _safe_relpath(workdir) if workdir else "."
        cwd = (base_dir / workdir_val).resolve()
        cwd.mkdir(parents=True, exist_ok=True)

        input_items = inputs if isinstance(inputs, list) else []
        output_items = outputs if isinstance(outputs, list) else []

        total_input = 0
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
            target = (base_dir / mount_path).resolve()
            if not str(target).startswith(str(base_dir.resolve())):
                raise BadRequestError("input path escapes sandbox")
            target.parent.mkdir(parents=True, exist_ok=True)
            meta = await self.artifact_manager.store.get_artifact(
                project_id=project_id, artifact_id=artifact_id
            )
            if not meta:
                raise BadRequestError("input artifact not found")
            expected_size = meta.get("size") if isinstance(meta, dict) else None
            expected_sha = meta.get("sha256") if isinstance(meta, dict) else None
            if target.exists() and target.is_file() and expected_size is not None:
                try:
                    current_size = target.stat().st_size
                except OSError:
                    current_size = -1
                if current_size == expected_size:
                    total_input += int(expected_size)
                    if total_input > self.max_total_input_bytes:
                        raise BadRequestError("total input size exceeds limit")
                    if expected_sha:
                        if _sha256_file(target) == expected_sha:
                            continue
                    else:
                        continue
            data, _meta = await self.artifact_manager.read_bytes(
                project_id=project_id, artifact_id=artifact_id
            )
            size = len(data)
            if size > self.max_input_bytes:
                raise BadRequestError("input file exceeds size limit")
            total_input += size
            if total_input > self.max_total_input_bytes:
                raise BadRequestError("total input size exceeds limit")
            target.write_bytes(data)

        for item in extra_files or []:
            path = item.get("path")
            data = item.get("data")
            if not isinstance(path, str) or not isinstance(data, (bytes, bytearray)):
                continue
            rel = _safe_relpath(path)
            target = (base_dir / rel).resolve()
            if not str(target).startswith(str(base_dir.resolve())):
                raise BadRequestError("extra file path escapes sandbox")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(bytes(data))

        settings = self._build_settings(work_dir=base_dir, extra_allow_write=[str(base_dir / "out")])
        settings_path: Optional[str] = None
        settings_mode = str(self.srt_cfg.get("settings_mode", "generated")).strip().lower() or "generated"
        if settings_mode == "file":
            settings_path = str(self.srt_cfg.get("settings_path") or "").strip()
            if not settings_path:
                raise BadRequestError("srt.settings_path is required for settings_mode=file")
        elif settings_mode == "generated":
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
                json.dump(settings, tmp)
                settings_path = tmp.name
        else:
            raise BadRequestError(f"unsupported srt.settings_mode: {settings_mode}")

        return base_dir, cwd, workdir_val, output_items, settings_path, settings_mode

    def _cleanup_settings(self, settings_path: Optional[str], settings_mode: str) -> None:
        if settings_path and settings_mode == "generated":
            try:
                os.unlink(settings_path)
            except OSError:
                pass

    async def _collect_outputs(
        self,
        *,
        project_id: str,
        base_dir: Path,
        workdir: str,
        output_items: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        created: List[Dict[str, Any]] = []
        output_errors: List[Dict[str, Any]] = []
        total_output = 0
        output_base = (base_dir / _safe_relpath(workdir)).resolve() if workdir else base_dir.resolve()
        for output in output_items:
            if not isinstance(output, dict):
                continue
            path = output.get("path")
            if not isinstance(path, str) or not path.strip():
                output_errors.append({"path": path, "error": "output path is required"})
                continue
            rel = _safe_relpath(path)
            target = (output_base / rel).resolve()
            if not str(target).startswith(str(base_dir.resolve())):
                output_errors.append({"path": path, "error": "output path escapes sandbox"})
                continue
            if not target.exists() or not target.is_file():
                # small retry for race with writer/flush
                await asyncio.sleep(0.15)
            if not target.exists() or not target.is_file():
                output_errors.append({"path": path, "error": "output is empty"})
                continue
            data = target.read_bytes()
            size = len(data)
            if size == 0:
                # small retry for race with writer/flush
                await asyncio.sleep(0.15)
                data = target.read_bytes()
                size = len(data)
            if size > self.max_output_bytes:
                output_errors.append({"path": path, "error": "output exceeds size limit"})
                continue
            total_output += size
            if total_output > self.max_total_output_bytes:
                output_errors.append({"path": path, "error": "total output exceeds size limit"})
                continue
            save_as = output.get("save_as") if isinstance(output.get("save_as"), str) else None
            filename = save_as or os.path.basename(rel or "output.dat")
            artifact = await self.artifact_manager.save_bytes(
                project_id=project_id,
                filename=filename,
                data=data,
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
        return created, output_errors

    async def start_process(
        self,
        *,
        project_id: str,
        tool_call_id: Optional[str],
        session_id: Optional[str],
        code: str,
        inputs: List[Dict[str, Any]] | None,
        outputs: List[Dict[str, Any]] | None,
        workdir: Optional[str],
        extra_files: Optional[List[Dict[str, Any]]] = None,
    ) -> SrtProcessHandle:
        if not isinstance(code, str) or not code.strip():
            raise BadRequestError("code is empty")
        base_dir, cwd, _workdir_val, output_items, settings_path, settings_mode = await self._prepare_run(
            project_id=project_id,
            tool_call_id=tool_call_id,
            session_id=session_id,
            inputs=inputs,
            outputs=outputs,
            workdir=workdir,
            extra_files=extra_files,
        )
        script_path = _write_bash_script(base_dir=base_dir, code=code)
        proc = await asyncio.create_subprocess_exec(
            "srt",
            "--settings",
            settings_path,
            "bash",
            str(script_path),
            cwd=str(cwd),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        return SrtProcessHandle(
            proc=proc,
            base_dir=base_dir,
            output_items=output_items,
            settings_path=settings_path,
            settings_mode=settings_mode,
            start_ts=time.monotonic(),
        )

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
    ) -> SrtResult:
        lang = (language or "").strip().lower()
        if lang not in {"bash"}:
            raise BadRequestError("unsupported language")
        if not isinstance(code, str) or not code.strip():
            raise BadRequestError("code is empty")
        timeout_val = int(timeout_sec) if timeout_sec is not None else self.timeout_sec_default
        if timeout_val <= 0:
            timeout_val = self.timeout_sec_default
        timeout_val = min(timeout_val, self.timeout_sec_max)

        base_dir, cwd, workdir_val, output_items, settings_path, settings_mode = await self._prepare_run(
            project_id=project_id,
            tool_call_id=tool_call_id,
            session_id=session_id,
            inputs=inputs,
            outputs=outputs,
            workdir=workdir,
            extra_files=extra_files,
        )
        start = time.monotonic()
        stdout_text = ""
        stderr_text = ""
        exit_code = 0
        try:
            script_path = _write_bash_script(base_dir=base_dir, code=code)
            proc = await asyncio.create_subprocess_exec(
                "srt",
                "--settings",
                settings_path,
                "bash",
                str(script_path),
                cwd=str(cwd),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=timeout_val)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise BadRequestError("command timed out")
            stdout_text = (stdout_bytes or b"").decode("utf-8", errors="ignore")
            stderr_text = (stderr_bytes or b"").decode("utf-8", errors="ignore")
            exit_code = int(proc.returncode or 0)
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            self._cleanup_settings(settings_path, settings_mode)

        created, output_errors = await self._collect_outputs(
            project_id=project_id,
            base_dir=base_dir,
            workdir=workdir_val,
            output_items=output_items,
        )

        return SrtResult(
            stdout=_truncate_text(stdout_text, self.max_stdout_bytes),
            stderr=_truncate_text(stderr_text, self.max_stderr_bytes),
            exit_code=exit_code,
            duration_ms=duration_ms,
            created_artifacts=created,
            output_errors=output_errors,
        )
