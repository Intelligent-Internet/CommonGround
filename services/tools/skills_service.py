"""
Skills Tool Service (UTP)

Listens on: cg.{PROTOCOL_VERSION}.*.*.cmd.tool.skills.*
Behavior: supports skills.load, skills.activate, skills.run_cmd,
and task tracking tools (skills.task_watch, skills.task_status).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shlex
import shutil
import signal
import socket
import sys
import time
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import toml
import uuid6
from datetime import datetime, UTC

from core.utils import set_loop_policy, set_python_path, REPO_ROOT
from core.time_utils import to_iso, utc_now
from core.utp_protocol import ALLOWED_AFTER_EXECUTION_VALUES, ToolCommandPayload
from core.status import STATUS_FAILED, STATUS_SUCCESS, STATUS_TIMEOUT
from core.errors import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    ProtocolViolationError,
    build_error_payload,
    build_error_result_from_exception,
)

set_loop_policy()
set_python_path()

from infra.nats_client import NATSClient  # noqa: E402
from infra.cardbox_client import CardBoxClient  # noqa: E402
from infra.primitives import report as report_primitive  # noqa: E402
from infra.stores import (  # noqa: E402
    ExecutionStore,
    ResourceStore,
    SkillStore,
    ArtifactStore,
    SandboxStore,
    SkillTaskStore,
)
from infra.agent_routing import resolve_agent_target  # noqa: E402
from core.config import PROTOCOL_VERSION  # noqa: E402
from core.subject import format_subject, parse_subject  # noqa: E402
from core.trace import trace_id_from_headers  # noqa: E402
from infra.idempotency import (  # noqa: E402
    IdempotencyOutcome,
    IdempotentExecutor,
    NatsKvIdempotencyStore,
    build_idempotency_key,
)
from infra.gcs_client import GcsClient, GcsConfig  # noqa: E402
from infra.artifacts import ArtifactManager  # noqa: E402
from infra.skills import SkillManager, _safe_relpath  # noqa: E402
from infra.e2b_executor import E2BExecutor  # noqa: E402
from infra.srt_executor import SrtExecutor  # noqa: E402
from infra.sandbox_registry import SandboxRegistry  # noqa: E402
from infra.worker_helpers import normalize_headers  # noqa: E402
from infra.tool_executor import (  # noqa: E402
    ToolResultBuilder,
    ToolResultContext,
    extract_tool_call_args,
    extract_tool_call_metadata,
)
from pydantic import ValidationError  # noqa: E402


def _configure_logging() -> None:
    level_name = str(os.getenv("CG_LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def _truncate_for_log(text: str, limit: int = 400) -> str:
    if not isinstance(text, str):
        text = str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


_configure_logging()
logger = logging.getLogger("SkillsTool")


def _load_config() -> Dict[str, Any]:
    cfg_path = REPO_ROOT / "config.toml"
    if not cfg_path.exists():
        return {}
    return toml.load(cfg_path)


def _resolve_skill_root(local_dir: Path, skill_name: str) -> Path:
    candidate = (local_dir / skill_name).resolve()
    if candidate.exists() and candidate.is_dir():
        return candidate
    return local_dir



class SkillsToolService:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg or {}
        self.nats = NATSClient(config=cfg.get("nats", {}))
        self.cardbox = CardBoxClient(config=cfg.get("cardbox", {}))
        cardbox_cfg = cfg.get("cardbox", {}) if isinstance(cfg, dict) else {}
        dsn = cardbox_cfg.get("postgres_dsn", "postgresql://postgres:postgres@localhost:5433/cardbox")
        self.resource_store = ResourceStore(dsn)
        self.execution_store = ExecutionStore(dsn)
        self.skill_store = SkillStore(dsn)
        self.artifact_store = ArtifactStore(dsn)
        self.sandbox_store = SandboxStore(dsn)

        gcs_cfg = cfg.get("gcs", {}) if isinstance(cfg, dict) else {}
        gcs = GcsClient(
            GcsConfig(
                bucket=str(gcs_cfg.get("bucket") or "").strip(),
                prefix=str(gcs_cfg.get("prefix") or "").strip(),
                credentials_path=str(gcs_cfg.get("credentials_path") or "").strip() or None,
            )
        )
        skills_cfg = cfg.get("skills", {}) if isinstance(cfg, dict) else {}
        self.mode = str(skills_cfg.get("mode", "remote")).strip().lower() or "remote"
        if self.mode not in {"local", "remote"}:
            raise BadRequestError(f"unsupported skills.mode: {self.mode}")
        self.skills = SkillManager(
            store=self.skill_store,
            gcs=gcs,
            cache_root=str(skills_cfg.get("cache_root") or "/var/lib/skills-cache"),
            max_file_bytes=int(skills_cfg.get("max_file_bytes", 200 * 1024)),
        )
        self.artifact_manager = ArtifactManager(store=self.artifact_store, gcs=gcs)
        self._reaper_task: Optional[asyncio.Task] = None
        self._reaper_interval_sec = int(skills_cfg.get("reaper_interval_sec", 60))
        self._instance_id = uuid6.uuid7().hex
        self._runner_pid = os.getpid()
        self._runner_start_time = self._get_proc_start_time(self._runner_pid)

        if self.mode == "local":
            srt_cfg = cfg.get("srt", {}) if isinstance(cfg, dict) else {}
            try:
                logger.info(
                    "SRT config: mode=local network=%s filesystem=%s",
                    srt_cfg.get("network"),
                    srt_cfg.get("filesystem"),
                )
            except Exception:
                logger.info("SRT config: mode=local (network/filesystem unavailable)")
            self.executor = SrtExecutor(
                artifact_manager=self.artifact_manager,
                work_root=str(skills_cfg.get("local_root") or "/tmp/skills-local"),
                srt_cfg=srt_cfg,
                max_stdout_bytes=int(skills_cfg.get("stdout_max_bytes", 200 * 1024)),
                max_stderr_bytes=int(skills_cfg.get("stderr_max_bytes", 200 * 1024)),
                max_input_bytes=int(skills_cfg.get("input_max_bytes", 20 * 1024 * 1024)),
                max_output_bytes=int(skills_cfg.get("output_max_bytes", 20 * 1024 * 1024)),
                max_total_input_bytes=int(skills_cfg.get("total_input_max_bytes", 50 * 1024 * 1024)),
                max_total_output_bytes=int(skills_cfg.get("total_output_max_bytes", 50 * 1024 * 1024)),
                timeout_sec_default=int(skills_cfg.get("timeout_sec_default", 60)),
                timeout_sec_max=int(skills_cfg.get("timeout_sec_max", 120)),
            )
            self.sandbox_registry = None
        else:
            tool_cfg = (cfg.get("tools") or {}).get("e2b") if isinstance(cfg.get("tools"), dict) else {}
            tool_cfg = tool_cfg if isinstance(tool_cfg, dict) else {}
            self.sandbox_registry = SandboxRegistry(
                store=self.sandbox_store,
                reuse_mode=tool_cfg.get("reuse_mode", "none"),
                idle_ttl_sec=int(tool_cfg.get("idle_ttl_sec", 1800)),
                hard_ttl_sec=int(tool_cfg.get("hard_ttl_sec", 21600)),
                lock_timeout_sec=int(tool_cfg.get("lock_timeout_sec", 600)),
            )
            self.executor = E2BExecutor(
                artifact_manager=self.artifact_manager,
                template=tool_cfg.get("template"),
                sandbox_registry=self.sandbox_registry,
                reuse_mode=tool_cfg.get("reuse_mode"),
                idle_ttl_sec=tool_cfg.get("idle_ttl_sec"),
                hard_ttl_sec=tool_cfg.get("hard_ttl_sec"),
                lock_timeout_sec=tool_cfg.get("lock_timeout_sec"),
                domain=tool_cfg.get("domain"),
                max_stdout_bytes=int(tool_cfg.get("stdout_max_bytes", 200 * 1024)),
                max_stderr_bytes=int(tool_cfg.get("stderr_max_bytes", 200 * 1024)),
                max_input_bytes=int(tool_cfg.get("input_max_bytes", 20 * 1024 * 1024)),
                max_output_bytes=int(tool_cfg.get("output_max_bytes", 20 * 1024 * 1024)),
                max_total_input_bytes=int(tool_cfg.get("total_input_max_bytes", 50 * 1024 * 1024)),
                max_total_output_bytes=int(tool_cfg.get("total_output_max_bytes", 50 * 1024 * 1024)),
                timeout_sec_default=int(tool_cfg.get("timeout_sec_default", 60)),
                timeout_sec_max=int(tool_cfg.get("timeout_sec_max", 120)),
            )
            if not self.sandbox_registry.enabled:
                logger.warning("skills.mode=remote but tools.e2b.reuse_mode is disabled; sandbox reuse is off")

        self.idempotency_store = NatsKvIdempotencyStore(self.nats, bucket_name="cg_tool_idem_v1")
        self.idempotent_executor = IdempotentExecutor(self.idempotency_store, timeout_s=30.0)
        self._watch_registry: Dict[str, Dict[str, Any]] = {}
        self._remote_activations: Dict[str, Dict[str, str]] = {}
        self._bg_tasks: Dict[str, Dict[str, Any]] = {}
        self._bg_lock = asyncio.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._jobs_lock = asyncio.Lock()
        self._session_locks: Dict[str, asyncio.Lock] = {}
        self._stdout_cap_bytes = 1024 * 1024
        self._stderr_cap_bytes = 1024 * 1024
        self._job_default_timeout_sec = 1800
        self._task_store = SkillTaskStore(dsn)

    def _normalize_expose_ports(self, value: Any) -> list[int]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            items = value
        else:
            items = [value]
        ports: list[int] = []
        for item in items:
            try:
                port = int(item)
            except Exception:
                continue
            if port <= 0 or port > 65535:
                continue
            if port not in ports:
                ports.append(port)
        return ports

    def _choose_host_port(self, preferred: int) -> int:
        if preferred > 0:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("127.0.0.1", preferred))
                    return preferred
                except OSError:
                    pass
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return int(s.getsockname()[1])

    def _build_expose_ports_command(
        self,
        *,
        project_id: str,
        session_id: str,
        workdir: str,
        ports: list[int],
    ) -> tuple[str, list[dict[str, Any]]]:
        if not ports:
            return "", []
        if not shutil.which("socat"):
            raise BadRequestError("socat is required for expose_ports but was not found in PATH")
        work_root = getattr(self.executor, "work_root", None)
        if not isinstance(work_root, str) or not work_root:
            raise BadRequestError("executor work_root unavailable for expose_ports")
        base_dir = Path(work_root) / project_id / session_id
        workdir_val = str(workdir or "skill").strip() or "skill"
        uds_dir = base_dir / workdir_val / ".srt" / "ports"
        endpoints: list[dict[str, Any]] = []
        cmd_lines: list[str] = [f"mkdir -p {shlex.quote(str(uds_dir))}"]
        for port in ports:
            uds_path = uds_dir / f"{port}.sock"
            log_path = uds_dir / f"socat_{port}.log"
            cmd_lines.append(f"rm -f {shlex.quote(str(uds_path))}")
            cmd_lines.append(
                " ".join(
                    [
                        "socat",
                        f"UNIX-LISTEN:{shlex.quote(str(uds_path))},fork,reuseaddr,unlink-close",
                        f"TCP:127.0.0.1:{port}",
                        f"> {shlex.quote(str(log_path))} 2>&1 &",
                    ]
                )
            )
            host_port = self._choose_host_port(port)
            endpoints.append(
                {
                    "port": port,
                    "host_port": host_port,
                    "url": f"http://127.0.0.1:{host_port}",
                    "unix_socket": str(uds_path),
                }
            )

        return "\n".join(cmd_lines), endpoints

    async def _start_host_forwarders(
        self,
        endpoints: list[dict[str, Any]],
    ) -> tuple[list[Any], dict[str, int], dict[str, int | str]]:
        if not endpoints:
            return [], {}, {}
        if not shutil.which("socat"):
            raise BadRequestError("socat is required for expose_ports but was not found in PATH")
        host_procs: list[Any] = []
        forwarder_pids: dict[str, int] = {}
        forwarder_start_times: dict[str, int | str] = {}
        for endpoint in endpoints:
            host_port = endpoint.get("host_port")
            uds_path = endpoint.get("unix_socket")
            host_cmd = [
                "socat",
                f"TCP-LISTEN:{host_port},bind=127.0.0.1,reuseaddr,fork",
                f"UNIX-CONNECT:{uds_path}",
            ]
            try:
                host_proc = await asyncio.create_subprocess_exec(
                    *host_cmd,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                host_procs.append(host_proc)
                if host_proc.pid:
                    forwarder_pids[str(host_port)] = int(host_proc.pid)
                    start_time = self._get_proc_start_time(int(host_proc.pid))
                    if start_time is not None:
                        forwarder_start_times[str(host_port)] = int(start_time)
                await asyncio.sleep(0.2)
                if host_proc.returncode is not None and host_proc.returncode != 0:
                    raise BadRequestError("expose_ports failed: host port forwarder exited early")
            except Exception:
                for proc in host_procs:
                    try:
                        if proc and getattr(proc, "pid", None):
                            proc.terminate()
                    except Exception:
                        pass
                raise
        return host_procs, forwarder_pids, forwarder_start_times

    async def _await_expose_sockets(self, endpoints: list[dict[str, Any]], timeout_s: float = 5.0) -> None:
        for endpoint in endpoints:
            uds_path = Path(str(endpoint.get("unix_socket") or "")).expanduser()
            port = endpoint.get("port")
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                if uds_path.exists():
                    break
                await asyncio.sleep(0.1)
            if uds_path.exists():
                continue
            log_path = uds_path.parent / f"socat_{port}.log"
            log_tail = ""
            try:
                if log_path.exists():
                    log_tail = log_path.read_text(encoding="utf-8", errors="ignore")[-2048:]
            except Exception:
                pass
            raise BadRequestError(
                "expose_ports failed: unix socket not created; "
                "check srt.network.allow_unix_sockets and filesystem allow_write. "
                f"socat_stderr={log_tail!s}"
            )

    def _clean_task_for_storage(self, task: Dict[str, Any]) -> Dict[str, Any]:
        snapshot = dict(task)
        snapshot.pop("proc", None)
        snapshot.pop("stream_tasks", None)
        snapshot.pop("forwarder_procs", None)
        snapshot.pop("sidecar_handles", None)
        return snapshot

    async def _persist_task_snapshot(self, task_id: str, task: Dict[str, Any]) -> None:
        if not self._task_store:
            return
        try:
            await self._task_store.upsert_task(
                task_id=task_id,
                project_id=str(task.get("project_id") or ""),
                status=str(task.get("status") or "unknown"),
                payload=self._clean_task_for_storage(task),
            )
        except Exception:
            logger.exception("failed to persist task snapshot: %s", task_id)

    async def _persist_job_snapshot(self, job_id: str, job: Dict[str, Any]) -> None:
        if not self._task_store:
            return
        try:
            await self._task_store.upsert_job(
                job_id=job_id,
                project_id=str(job.get("project_id") or ""),
                status=str(job.get("status") or "unknown"),
                payload=dict(job),
            )
        except Exception:
            logger.exception("failed to persist job snapshot: %s", job_id)

    def _resolve_session_id(
        self,
        *,
        parts: Any,
        data: Dict[str, Any],
        args: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
    ) -> Optional[str]:
        def _sanitize(value: str) -> str:
            return value.replace(":", "_")

        explicit = args.get("session_id")
        if isinstance(explicit, str) and explicit.strip():
            return _sanitize(explicit.strip())
        agent_id = data.get("agent_id")
        if isinstance(agent_id, str) and agent_id.strip():
            return _sanitize(f"{parts.project_id}:{agent_id.strip()}")
        fallback = tool_call_meta.get("tool_call_id") or data.get("tool_call_id")
        if isinstance(fallback, str) and fallback.strip():
            return _sanitize(fallback.strip())
        return None

    def _normalize_inputs(
        self,
        inputs: Any,
        *,
        workdir: Optional[str],
    ) -> Optional[list[dict[str, Any]]]:
        if not isinstance(inputs, list):
            return None
        workdir_val = str(workdir or "skill").strip() or "skill"
        normalized: list[dict[str, Any]] = []
        for item in inputs:
            if not isinstance(item, dict):
                continue
            artifact_id = item.get("artifact_id")
            mount_path = item.get("mount_path")
            if isinstance(mount_path, str) and mount_path.strip():
                basename = os.path.basename(mount_path.strip().rstrip("/"))
            else:
                basename = str(artifact_id or "").strip()
            if not basename:
                basename = "input.bin"
            normalized.append(
                {
                    **item,
                    "mount_path": f"{workdir_val}/inputs/{basename}",
                }
            )
        return normalized

    def _session_lock_key(self, session_id: Optional[str], workdir: Optional[str]) -> str:
        sid = str(session_id or "").strip() or "default"
        wdir = str(workdir or "skill").strip() or "skill"
        return f"{sid}:{wdir}"

    def _get_session_lock(self, key: str) -> asyncio.Lock:
        lock = self._session_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._session_locks[key] = lock
        return lock

    async def _find_session_conflict(
        self,
        *,
        session_id: Optional[str],
        workdir: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        lock_key = self._session_lock_key(session_id, workdir)
        lock = self._get_session_lock(lock_key)
        if lock.locked():
            return {
                "type": "lock",
                "session_id": session_id,
                "workdir": workdir or "skill",
                "lock_key": lock_key,
                "status": "locked",
            }
        async with self._jobs_lock:
            for job in self._jobs.values():
                if job.get("status") not in ("queued", "running"):
                    continue
                if self._session_lock_key(job.get("session_id"), job.get("workdir")) != lock_key:
                    continue
                return {
                    "type": "job",
                    "id": job.get("job_id"),
                    "status": job.get("status"),
                    "session_id": job.get("session_id"),
                    "workdir": job.get("workdir"),
                }
        async with self._bg_lock:
            for task in self._bg_tasks.values():
                if task.get("status") not in ("queued", "starting", "running"):
                    continue
                # NOTE: run_service is intentionally not treated as a conflict.
                # Rationale: services are long-lived (e.g., dev servers) and we
                # want to allow run_cmd/start_job to proceed in parallel for
                # iterative workflows. This is a conscious tradeoff: concurrent
                # writes in the same workdir may race. We accept this risk and
                # rely on callers to avoid destructive overlaps.
                if task.get("mode") == "service":
                    continue
                if self._session_lock_key(task.get("session_id"), task.get("workdir")) != lock_key:
                    continue
                return {
                    "type": "task",
                    "id": task.get("task_id"),
                    "status": task.get("status"),
                    "session_id": task.get("session_id"),
                    "workdir": task.get("workdir"),
                }
        return None

    def _truncate_ring(self, text: str, limit: int) -> tuple[str, bool]:
        if not text:
            return "", False
        if len(text) <= limit:
            return text, False
        return text[-limit:], True

    def _with_env_prefix(self, env: Optional[Dict[str, Any]], command: str) -> str:
        if not isinstance(env, dict) or not env:
            return command
        exports = []
        for key, val in env.items():
            if not isinstance(key, str) or not key.strip():
                continue
            value = "" if val is None else str(val)
            safe_key = key.strip().replace('"', "").replace("'", "")
            safe_val = value.replace('"', '\\"')
            exports.append(f'export {safe_key}="{safe_val}"')
        if not exports:
            return command
        return "\n".join(exports) + "\n" + command

    async def _update_bg_task(self, task_id: str, updates: Dict[str, Any]) -> None:
        snapshot: Optional[Dict[str, Any]] = None
        async with self._bg_lock:
            task = self._bg_tasks.get(task_id)
            if not task:
                return
            if task.get("status") == "canceled" and updates.get("status") != "canceled":
                updates = dict(updates)
                updates.pop("status", None)
            task.update(updates)
            task["last_updated_at"] = to_iso(utc_now())
            snapshot = dict(task)
        if snapshot is not None:
            await self._persist_task_snapshot(task_id, snapshot)

    async def _get_bg_task_snapshot(self, task_id: str, tail_bytes: int) -> Dict[str, Any]:
        task = None
        async with self._bg_lock:
            task = self._bg_tasks.get(task_id)
        if not task and self._task_store:
            stored = await self._task_store.get_task(task_id)
            if stored:
                task = stored
        if not task:
            raise NotFoundError(f"task not found: {task_id}")
        stdout_val = str(task.get("stdout") or "")
        stderr_val = str(task.get("stderr") or "")
        stdout_tail = stdout_val[-tail_bytes:] if tail_bytes > 0 else stdout_val
        stderr_tail = stderr_val[-tail_bytes:] if tail_bytes > 0 else stderr_val
        orphaned = task.get("status") in ("queued", "starting", "running") and task_id not in self._bg_tasks
        return {
            "task_id": task_id,
            "provider": task.get("provider"),
            "status": task.get("status"),
            "mode": task.get("mode"),
            "startup_ready": task.get("startup_ready"),
                "last_output_at": task.get("last_output_at"),
                "started_at": task.get("started_at"),
                "finished_at": task.get("finished_at"),
                "exit_code": task.get("exit_code"),
                "stdout_tail": stdout_tail,
                "stderr_tail": stderr_tail,
                "stdout_truncated": task.get("stdout_truncated"),
                "stderr_truncated": task.get("stderr_truncated"),
            "created_artifacts": task.get("created_artifacts"),
            "output_errors": task.get("output_errors"),
            "session_id": task.get("session_id"),
            "error_message": task.get("error_message"),
            "exposed_endpoints": task.get("exposed_endpoints") or [],
            "exposed_endpoints_active": task.get("exposed_endpoints_active"),
            "orphaned": orphaned,
            "orphaned_reason": "service_restart" if orphaned else None,
        }

    async def _spawn_bg_task(
        self,
        *,
        parts: Any,
        data: Dict[str, Any],
        skill_name: str,
        command: str,
        session_id: Optional[str],
        timeout_sec: Optional[int],
        mode: str = "batch",
        env: Optional[Dict[str, Any]] = None,
        workdir: Optional[str] = None,
        inputs: Optional[list[dict[str, Any]]] = None,
        outputs: Optional[list[dict[str, Any]]] = None,
    ) -> str:
        workdir_val = str(workdir or "skill")
        mode_val = "service" if str(mode).lower() == "service" else "batch"
        normalized_inputs = self._normalize_inputs(inputs, workdir=workdir_val)
        task_id = uuid6.uuid7().hex
        created_at = to_iso(utc_now())
        provider_name = "srt" if self.mode == "local" else "e2b"
        async with self._bg_lock:
            self._bg_tasks[task_id] = {
                "task_id": task_id,
                "project_id": parts.project_id,
                "provider": provider_name,
                "status": "queued",
                "mode": mode_val,
                "startup_ready": False,
                "last_output_at": None,
                "created_at": created_at,
                "started_at": None,
                "finished_at": None,
                "last_updated_at": created_at,
                "exit_code": None,
                "stdout": "",
                "stderr": "",
                "stdout_truncated": False,
                "stderr_truncated": False,
                "session_id": session_id,
                "skill_name": skill_name.strip(),
                "command": command,
                "workdir": workdir_val,
                "created_artifacts": [],
                "output_errors": [],
                "error_message": None,
                "proc": None,
                "stream_tasks": [],
                "settings_path": None,
                "settings_mode": None,
                "exposed_endpoints": [],
                "sidecar_handles": [],
                "forwarder_procs": [],
                "forwarder_pids": {},
                "forwarder_start_times": {},
                "proc_pid": None,
                "proc_start_time": None,
                "runner_instance_id": self._instance_id,
                "runner_pid": self._runner_pid,
                "runner_start_time": self._runner_start_time,
            }
            snapshot = dict(self._bg_tasks[task_id])
        await self._persist_task_snapshot(task_id, snapshot)

        async def _runner() -> None:
            await self._update_bg_task(task_id, {"status": "starting"})
            try:
                extra_files: list[dict[str, Any]] = []
                version_hash = ""
                sandbox_agent_id = data.get("agent_id")
                skill_root = None
                if self.mode == "local":
                    info = await self.skills.ensure_skill_local(
                        project_id=parts.project_id, skill_name=skill_name.strip()
                    )
                    version_hash = info["version_hash"]
                    local_dir = Path(info["local_dir"])
                    skill_root = _resolve_skill_root(local_dir, skill_name.strip())
                    for file_path in skill_root.rglob("*"):
                        if not file_path.is_file():
                            continue
                        rel = file_path.relative_to(skill_root).as_posix()
                        target_path = f"{workdir_val}/{rel}"
                        data_bytes = file_path.read_bytes()
                        if len(data_bytes) > self.skills.max_file_bytes:
                            raise BadRequestError(f"skill file too large: {rel}")
                        extra_files.append({"path": target_path, "data": data_bytes})
                else:
                    if session_id:
                        sandbox_agent_id = session_id
                    version_hash = await self._ensure_remote_activated(
                        project_id=parts.project_id,
                        agent_id=sandbox_agent_id,
                        skill_name=skill_name.strip(),
                        tool_call_id=data.get("tool_call_id"),
                        timeout_sec=timeout_sec,
                    )
                if mode_val == "service":
                    if self.mode != "local":
                        raise BadRequestError("run_service only supported in local mode")
                    handle = await self.executor.start_process(
                        project_id=parts.project_id,
                        tool_call_id=data.get("tool_call_id"),
                        session_id=session_id if self.mode == "local" else None,
                        code=self._with_env_prefix(env, command),
                        inputs=normalized_inputs,
                        outputs=outputs,
                        workdir=workdir_val,
                        extra_files=extra_files,
                    )
                    await self._update_bg_task(
                        task_id,
                        {
                            "status": "running",
                            "started_at": to_iso(utc_now()),
                            "startup_ready": True,
                            "proc": handle.proc,
                            "settings_path": handle.settings_path,
                            "settings_mode": handle.settings_mode,
                            "proc_pid": int(handle.proc.pid) if handle.proc.pid else None,
                            "proc_start_time": self._get_proc_start_time(int(handle.proc.pid))
                            if handle.proc.pid
                            else None,
                        },
                    )
                    stream_tasks: list[asyncio.Task] = []
                    if handle.proc.stdout is not None:
                        stream_tasks.append(asyncio.create_task(self._stream_process_output(task_id, handle.proc.stdout, "stdout")))
                    if handle.proc.stderr is not None:
                        stream_tasks.append(asyncio.create_task(self._stream_process_output(task_id, handle.proc.stderr, "stderr")))
                    await self._update_bg_task(task_id, {"stream_tasks": stream_tasks})
                    asyncio.create_task(self._wait_service_exit(task_id, handle))
                    return

                await self._update_bg_task(
                    task_id,
                    {"status": "running", "started_at": to_iso(utc_now())},
                )
                exec_command = self._with_env_prefix(env, command)
                result = await self.executor.execute(
                    project_id=parts.project_id,
                    agent_id=sandbox_agent_id if self.mode != "local" else data.get("agent_id"),
                    tool_call_id=data.get("tool_call_id"),
                    session_id=session_id if self.mode == "local" else None,
                    language="bash",
                    code=exec_command,
                    inputs=normalized_inputs,
                    outputs=outputs,
                    timeout_sec=timeout_sec,
                    workdir=workdir_val,
                    extra_files=extra_files,
                )
                stdout_tail, stdout_truncated = self._truncate_ring(result.stdout, self._stdout_cap_bytes)
                stderr_tail, stderr_truncated = self._truncate_ring(result.stderr, self._stderr_cap_bytes)
                finished_at = to_iso(utc_now())
                status = "succeeded" if result.exit_code == 0 else "failed"
                updates = {
                    "status": status,
                    "finished_at": finished_at,
                    "exit_code": result.exit_code,
                    "stdout": stdout_tail,
                    "stderr": stderr_tail,
                    "stdout_truncated": stdout_truncated,
                    "stderr_truncated": stderr_truncated,
                    "created_artifacts": result.created_artifacts,
                    "output_errors": result.output_errors,
                    "version_hash": version_hash,
                }
                if skill_root is not None:
                    updates["skill_root"] = str(skill_root)
                await self._update_bg_task(task_id, updates)
            except Exception as exc:  # noqa: BLE001
                finished_at = to_iso(utc_now())
                await self._update_bg_task(
                    task_id,
                    {
                        "status": "failed",
                        "finished_at": finished_at,
                        "exit_code": 1,
                        "error_message": str(exc),
                    },
                )
                await self._cleanup_task_forwarders(task_id)

        asyncio.create_task(_runner())
        return task_id

    async def _stream_process_output(self, task_id: str, stream: asyncio.StreamReader, field: str) -> None:
        while True:
            chunk = await stream.readline()
            if not chunk:
                break
            text = chunk.decode("utf-8", errors="ignore")
            async with self._bg_lock:
                task = self._bg_tasks.get(task_id)
                if not task:
                    return
                current = str(task.get(field) or "")
                combined = current + text
                cap = self._stdout_cap_bytes if field == "stdout" else self._stderr_cap_bytes
                tail, truncated = self._truncate_ring(combined, cap)
                task[field] = tail
                task[f"{field}_truncated"] = truncated
                task["last_output_at"] = to_iso(utc_now())

    async def _wait_service_exit(self, task_id: str, handle: Any) -> None:
        proc = handle.proc
        try:
            exit_code = await proc.wait()
        finally:
            self.executor._cleanup_settings(handle.settings_path, handle.settings_mode)
        try:
            if proc and getattr(proc, "pid", None):
                os.killpg(proc.pid, signal.SIGTERM)
        except Exception:
            pass
        status = "succeeded" if int(exit_code or 0) == 0 else "failed"
        finished_at = to_iso(utc_now())
        await self._update_bg_task(
            task_id,
            {
                "status": status,
                "finished_at": finished_at,
                "exit_code": int(exit_code or 0),
            },
        )
        await self._cleanup_task_forwarders(task_id)

    async def _cancel_task(self, task_id: str) -> Dict[str, Any]:
        async with self._bg_lock:
            task = self._bg_tasks.get(task_id)
        if not task:
            raise NotFoundError(f"task not found: {task_id}")
        status = str(task.get("status") or "")
        if status in ("succeeded", "failed", "canceled"):
            return {"task_id": task_id, "status": status, "canceled": False}
        await self._cleanup_task_forwarders(task_id)
        proc = task.get("proc")
        if proc and getattr(proc, "pid", None):
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except Exception:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        stream_tasks = task.get("stream_tasks") or []
        for t in stream_tasks:
            if isinstance(t, asyncio.Task):
                t.cancel()
        await self._update_bg_task(
            task_id,
            {
                "status": "canceled",
                "finished_at": to_iso(utc_now()),
                "exit_code": -1,
            },
        )
        return {"task_id": task_id, "status": "canceled", "canceled": True}

    async def _cleanup_task_forwarders(self, task_id: str) -> None:
        async with self._bg_lock:
            task = self._bg_tasks.get(task_id) or {}
            forwarders = task.get("forwarder_procs") or []
            sidecars = task.get("sidecar_handles") or []
        for fwd in forwarders:
            try:
                if fwd and getattr(fwd, "pid", None):
                    fwd.terminate()
            except Exception:
                pass
        for sc in sidecars:
            proc = sc.get("proc") if isinstance(sc, dict) else None
            if proc and getattr(proc, "pid", None):
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                self.executor._cleanup_settings(sc.get("settings_path"), sc.get("settings_mode"))
            except Exception:
                pass
        updated_task = None
        async with self._bg_lock:
            task = self._bg_tasks.get(task_id)
            if task is not None:
                task["forwarder_procs"] = []
                task["forwarder_pids"] = {}
                task["forwarder_start_times"] = {}
                task["sidecar_handles"] = []
                task["exposed_endpoints_active"] = False
                updated_task = dict(task)
        if updated_task is not None:
            await self._persist_task_snapshot(task_id, updated_task)

    async def _reap_orphaned_forwarders(self) -> None:
        if not self._task_store:
            return
        limit = 200
        offset = 0
        max_pages = 10
        max_seconds = 5.0
        started = time.monotonic()
        while True:
            if (offset // limit) >= max_pages:
                return
            if time.monotonic() - started > max_seconds:
                return
            try:
                rows = await self._task_store.list_tasks_by_status(
                    statuses=["starting", "running"],
                    limit=limit,
                    offset=offset,
                )
            except Exception:
                logger.exception("failed to list tasks for forwarder reaping")
                return
            if not rows:
                return
            for row in rows:
                task_id = row.get("task_id")
                payload = row.get("payload") or {}
                runner_id = payload.get("runner_instance_id")
                runner_pid = payload.get("runner_pid")
                runner_start_time = payload.get("runner_start_time")
                if runner_id and runner_id == self._instance_id:
                    continue
                if runner_pid and runner_start_time and self._pid_alive(int(runner_pid), runner_start_time):
                    continue
                forwarder_pids = payload.get("forwarder_pids") or {}
                forwarder_start_times = payload.get("forwarder_start_times") or {}
                proc_pid = payload.get("proc_pid")
                proc_start_time = payload.get("proc_start_time")
                settings_path = payload.get("settings_path")
                proc_reaped = False
                endpoints = payload.get("exposed_endpoints") or []
                updated = False
                for endpoint in endpoints:
                    host_port = endpoint.get("host_port")
                    if host_port is None:
                        continue
                    pid = forwarder_pids.get(str(host_port))
                    if not pid:
                        continue
                    start_time = forwarder_start_times.get(str(host_port))
                    if self._kill_forwarder_pid(pid, host_port, start_time):
                        forwarder_pids.pop(str(host_port), None)
                        forwarder_start_times.pop(str(host_port), None)
                        updated = True
                if forwarder_pids == {} and endpoints:
                    payload["exposed_endpoints_active"] = False
                    updated = True
                if (
                    proc_pid
                    and proc_start_time
                    and settings_path
                    and str(payload.get("provider") or "") == "srt"
                ):
                    if self._kill_sandbox_pid(int(proc_pid), proc_start_time, str(settings_path)):
                        now = to_iso(utc_now())
                        payload["status"] = "failed"
                        payload["finished_at"] = now
                        payload["exit_code"] = -1
                        payload["error_message"] = "orphaned_reaped"
                        payload["orphaned"] = True
                        payload["orphaned_reason"] = "reaper"
                        proc_reaped = True
                        updated = True
                if updated and task_id:
                    payload["forwarder_pids"] = forwarder_pids
                    payload["forwarder_start_times"] = forwarder_start_times
                    try:
                        await self._task_store.upsert_task(
                            task_id=task_id,
                            project_id=str(payload.get("project_id") or ""),
                            status=str(payload.get("status") or "unknown"),
                            payload=self._clean_task_for_storage(payload),
                        )
                    except Exception:
                        logger.exception("failed to update task after reaping forwarders: %s", task_id)
                if proc_reaped:
                    logger.info("reaped orphaned sandbox process for task %s", task_id)
            offset += limit

    def _kill_forwarder_pid(self, pid: int, host_port: int, start_time: int | None) -> bool:
        try:
            cmdline = self._get_proc_cmdline(pid)
            if cmdline:
                if "socat" not in cmdline or f"TCP-LISTEN:{host_port}" not in cmdline:
                    return False
            if start_time is not None:
                current_start = self._get_proc_start_time(pid)
                if current_start is None:
                    return False
                if isinstance(start_time, str) or isinstance(current_start, str):
                    if str(current_start) != str(start_time):
                        return False
                else:
                    if int(current_start) != int(start_time):
                        return False
            os.kill(pid, signal.SIGTERM)
            return True
        except ProcessLookupError:
            return True
        except Exception:
            return False

    def _kill_sandbox_pid(self, pid: int, start_time: int | str, settings_path: str) -> bool:
        try:
            cmdline = self._get_proc_cmdline(pid)
            if cmdline:
                if "srt" not in cmdline or settings_path not in cmdline:
                    return False
            current_start = self._get_proc_start_time(pid)
            if current_start is None:
                return False
            if isinstance(start_time, str) or isinstance(current_start, str):
                if str(current_start) != str(start_time):
                    return False
            else:
                if int(current_start) != int(start_time):
                    return False
            try:
                os.killpg(pid, signal.SIGTERM)
            except Exception:
                os.kill(pid, signal.SIGTERM)
            return True
        except ProcessLookupError:
            return True
        except Exception:
            return False

    def _get_proc_cmdline(self, pid: int) -> str | None:
        if sys.platform == "linux":
            try:
                cmdline_path = Path(f"/proc/{pid}/cmdline")
                if cmdline_path.exists():
                    return cmdline_path.read_text(encoding="utf-8", errors="ignore").replace("\x00", " ")
            except Exception:
                logger.warning("failed to read /proc cmdline for pid %s", pid)
                return None
        if sys.platform == "darwin":
            try:
                out = subprocess.check_output(
                    ["ps", "-p", str(pid), "-o", "command="],
                    stderr=subprocess.DEVNULL,
                )
                return out.decode("utf-8", errors="ignore").strip() or None
            except Exception:
                logger.warning("failed to read ps command for pid %s", pid)
                return None
        return None

    def _get_proc_start_time(self, pid: int) -> int | str | None:
        if sys.platform == "linux":
            try:
                stat_path = Path(f"/proc/{pid}/stat")
                data = stat_path.read_text(encoding="utf-8", errors="ignore")
                end = data.rfind(")")
                if end == -1:
                    return None
                rest = data[end + 2 :].split()
                # starttime is the 22nd field, i.e. index 19 in the rest list
                if len(rest) < 20:
                    return None
                return int(rest[19])
            except Exception:
                logger.warning("failed to read /proc start time for pid %s", pid)
                return None
        if sys.platform == "darwin":
            try:
                out = subprocess.check_output(
                    ["ps", "-p", str(pid), "-o", "lstart="],
                    stderr=subprocess.DEVNULL,
                )
                value = out.decode("utf-8", errors="ignore").strip()
                return value or None
            except Exception:
                logger.warning("failed to read ps lstart for pid %s", pid)
                return None
        return None

    def _pid_alive(self, pid: int, start_time: int | str | None = None) -> bool:
        current_start = None
        if start_time is not None:
            current_start = self._get_proc_start_time(pid)
            if current_start is None:
                return False
            if isinstance(start_time, str) or isinstance(current_start, str):
                if str(current_start) != str(start_time):
                    return False
            else:
                if int(current_start) != int(start_time):
                    return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except Exception:
            return False

    async def start(self):
        await self.nats.connect()
        await self.cardbox.init()
        await self.resource_store.open()
        await self.execution_store.open()
        await self.skill_store.open()
        await self.artifact_store.open()
        await self._task_store.open()
        if self.sandbox_registry is not None:
            await self.sandbox_store.open()
        await self._reap_orphaned_forwarders()
        self._start_reaper_loop()
        subject = f"cg.{PROTOCOL_VERSION}.*.*.cmd.tool.skills.*"
        queue = "tool_skills"
        logger.info("Listening on %s (queue=%s)", subject, queue)
        await self.nats.subscribe_cmd(
            subject,
            queue,
            self._handle_cmd,
            durable_name=f"{queue}_cmd_{PROTOCOL_VERSION}",
            deliver_policy="all",
        )
        while True:
            await asyncio.sleep(1)

    async def close(self) -> None:
        if self._reaper_task and not self._reaper_task.done():
            self._reaper_task.cancel()
        try:
            if self.sandbox_registry is not None:
                await self.sandbox_store.close()
        finally:
            await self.artifact_store.close()
            await self.skill_store.close()
            await self.execution_store.close()
            await self.resource_store.close()
            await self.cardbox.close()
            await self._task_store.close()
            await self.nats.close()

    def _start_reaper_loop(self) -> None:
        if self._reaper_task and not self._reaper_task.done():
            return
        interval = max(5, int(self._reaper_interval_sec or 60))

        async def _loop() -> None:
            while True:
                try:
                    await self._reap_orphaned_forwarders()
                except Exception:
                    logger.exception("forwarder reaper loop failed")
                await asyncio.sleep(interval)

        self._reaper_task = asyncio.create_task(_loop())

    async def _execute_logic(
        self,
        parts: Any,
        data: Dict[str, Any],
        headers: Dict[str, str],
    ) -> Tuple[Dict[str, Any], Any]:
        try:
            payload = ToolCommandPayload.validate(
                data,
                forbid_keys={"args", "arguments", "result"},
                require_tool_call_card_id=True,
            )
        except ValidationError as exc:
            missing = ToolCommandPayload.missing_fields_from_error(exc)
            err = BadRequestError(f"missing required fields {missing or ['unknown']}")
            result, error = build_error_result_from_exception(err, source="tool")
            return self._create_result_package(parts, data, {}, STATUS_FAILED, result, error)
        except (BadRequestError, ProtocolViolationError) as exc:
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, {}, STATUS_FAILED, result, error)

        tool_call_id = payload.tool_call_id
        tool_call_card_id = payload.tool_call_card_id
        tool_call_cards = await self.cardbox.get_cards([str(tool_call_card_id)], project_id=parts.project_id)
        tool_call_card = tool_call_cards[0] if tool_call_cards else None
        if not tool_call_card:
            result, error = build_error_result_from_exception(
                NotFoundError(f"tool_call_card_id not found: {tool_call_card_id}"),
                source="tool",
            )
            return self._create_result_package(parts, data, {}, STATUS_FAILED, result, error)

        if getattr(tool_call_card, "type", None) != "tool.call":
            result, error = build_error_result_from_exception(
                ProtocolViolationError("tool_call_card_id type mismatch: expected 'tool.call'"),
                source="tool",
            )
            return self._create_result_package(parts, data, {}, STATUS_FAILED, result, error)

        if getattr(tool_call_card, "tool_call_id", None) not in (None, "", tool_call_id):
            result, error = build_error_result_from_exception(
                ProtocolViolationError("tool_call_id mismatch between payload and tool.call card"),
                source="tool",
            )
            return self._create_result_package(parts, data, extract_tool_call_metadata(tool_call_card), STATUS_FAILED, result, error)

        tool_call_meta = extract_tool_call_metadata(tool_call_card)
        try:
            args = extract_tool_call_args(tool_call_card)
        except ProtocolViolationError as exc:
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

        tool_name = data.get("tool_name")
        if tool_name == "skills.load":
            return await self._handle_load(parts, data, tool_call_meta, args)
        if tool_name == "skills.run_cmd":
            return await self._handle_run_cmd(parts, data, tool_call_meta, args)
        if tool_name == "skills.run_cmd_async":
            return await self._handle_run_cmd_async(parts, data, tool_call_meta, args)
        if tool_name == "skills.run_service":
            return await self._handle_run_service(parts, data, tool_call_meta, args)
        if tool_name == "skills.start_job":
            return await self._handle_start_job(parts, data, tool_call_meta, args)
        if tool_name == "skills.job_status":
            return await self._handle_job_status(parts, data, tool_call_meta, args)
        if tool_name == "skills.job_cancel":
            return await self._handle_job_cancel(parts, data, tool_call_meta, args)
        if tool_name == "skills.job_watch":
            return await self._handle_job_watch(parts, data, tool_call_meta, args)
        if tool_name == "skills.activate":
            return await self._handle_activate(parts, data, tool_call_meta, args)
        if tool_name == "skills.task_status":
            return await self._handle_task_status(parts, data, tool_call_meta, args)
        if tool_name == "skills.task_cancel":
            return await self._handle_task_cancel(parts, data, tool_call_meta, args)
        if tool_name == "skills.task_watch":
            return await self._handle_task_watch(parts, data, tool_call_meta, args)
        result, error = build_error_result_from_exception(
            BadRequestError(f"unsupported tool_name: {tool_name}"),
            source="tool",
        )
        return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _fetch_skill_zip_bytes(
        self, *, project_id: str, skill_name: str
    ) -> tuple[bytes, str]:
        skill = await self.skill_store.get_skill(project_id=project_id, skill_name=skill_name)
        if not skill:
            raise NotFoundError(f"skill not found: {skill_name}")
        if not skill.get("enabled", True):
            raise BadRequestError(f"skill disabled: {skill_name}")
        version_hash = skill.get("active_version_hash")
        if not isinstance(version_hash, str) or not version_hash.strip():
            raise BadRequestError(f"skill has no active version: {skill_name}")
        version_hash = version_hash.strip()
        version_row = await self.skill_store.get_skill_version(
            project_id=project_id, skill_name=skill_name, version_hash=version_hash
        )
        if not version_row:
            raise NotFoundError(f"skill version not found: {skill_name}@{version_hash}")
        storage_uri = version_row.get("storage_uri")
        if not isinstance(storage_uri, str) or not storage_uri.startswith("gs://"):
            raise BadRequestError("skill storage_uri invalid")
        bucket, path = storage_uri[5:].split("/", 1)
        if bucket != self.skills.gcs.bucket_name:
            raise BadRequestError("skill bucket mismatch")
        tmp_path = Path("/tmp") / f"skill_{project_id}_{skill_name}_{version_hash}.zip"
        self.skills.gcs.download_to_file_raw(path, str(tmp_path))
        data = tmp_path.read_bytes()
        tmp_path.unlink(missing_ok=True)
        return data, version_hash

    async def _get_active_skill_version_hash(self, *, project_id: str, skill_name: str) -> str:
        skill = await self.skill_store.get_skill(project_id=project_id, skill_name=skill_name)
        if not skill:
            raise NotFoundError(f"skill not found: {skill_name}")
        if not skill.get("enabled", True):
            raise BadRequestError(f"skill disabled: {skill_name}")
        version_hash = skill.get("active_version_hash")
        if not isinstance(version_hash, str) or not version_hash.strip():
            raise BadRequestError(f"skill has no active version: {skill_name}")
        return version_hash.strip()

    async def _ensure_remote_activated(
        self,
        *,
        project_id: str,
        agent_id: Optional[str],
        skill_name: str,
        tool_call_id: Optional[str],
        timeout_sec: Optional[int],
    ) -> str:
        if not self.sandbox_registry or not self.sandbox_registry.enabled:
            return ""
        if not agent_id:
            raise BadRequestError("agent_id is required for remote sandbox reuse")
        active = await self.sandbox_registry.get_active(project_id=project_id, agent_id=str(agent_id))
        if not active:
            active = None
        version_hash = await self._get_active_skill_version_hash(
            project_id=project_id, skill_name=skill_name
        )
        if active:
            by_skill = self._remote_activations.get(active.sandbox_id, {})
            if by_skill.get(skill_name) == version_hash:
                return version_hash
        zip_bytes, version_hash = await self._fetch_skill_zip_bytes(
            project_id=project_id, skill_name=skill_name
        )
        result_obj = await self.executor.prepare_remote_zip(
            project_id=project_id,
            agent_id=agent_id,
            tool_call_id=tool_call_id,
            zip_bytes=zip_bytes,
            dest_dir="skill",
            zip_path="/tmp/skill.zip",
            unzip=True,
            keep_archive=False,
            timeout_sec=int(timeout_sec) if timeout_sec is not None else None,
        )
        sandbox_id = str(result_obj.get("sandbox_id") or "")
        if sandbox_id:
            by_skill = self._remote_activations.setdefault(sandbox_id, {})
            by_skill[skill_name] = version_hash
        return version_hash

    async def _handle_load(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        skill_name = args.get("skill_name")
        path = args.get("path") or "SKILL.md"
        session_id = self._resolve_session_id(
            parts=parts,
            data=data,
            args=args,
            tool_call_meta=tool_call_meta,
        )
        if not isinstance(skill_name, str) or not skill_name.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("skill_name is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        try:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "tool_call_id=%s skill_name=%s path=%s session_id=%s",
                    tool_call_meta.get("tool_call_id"),
                    skill_name,
                    path,
                    session_id,
                )
            info = await self.skills.ensure_skill_local(
                project_id=parts.project_id, skill_name=skill_name.strip()
            )
            skill_root = _resolve_skill_root(Path(info["local_dir"]), skill_name.strip())
            file_info = self.skills.read_skill_file(base_dir=str(skill_root), rel_path=str(path))
            result_obj = {
                "content": file_info["content"],
                "bytes": file_info["bytes"],
                "sha256": file_info["sha256"],
                "version_hash": info["version_hash"],
            }
            if session_id:
                result_obj["session_id"] = session_id
                if self.mode == "local":
                    work_root = getattr(self.executor, "work_root", None)
                    if isinstance(work_root, str) and work_root:
                        session_root = (Path(work_root) / parts.project_id / session_id / "skill").resolve()
                        session_root.mkdir(parents=True, exist_ok=True)
                        result_obj["session_root"] = str(session_root)
            return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _handle_run_cmd(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        skill_name = args.get("skill_name")
        command = args.get("command")
        session_id = self._resolve_session_id(
            parts=parts,
            data=data,
            args=args,
            tool_call_meta=tool_call_meta,
        )
        if not isinstance(skill_name, str) or not skill_name.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("skill_name is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        if not isinstance(command, str) or not command.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("command is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        conflict = await self._find_session_conflict(session_id=session_id, workdir="skill")
        if conflict:
            result, error = build_error_result_from_exception(
                ConflictError(
                    "session workdir is busy; use skills.job_status/skills.task_status and retry",
                    detail={"reason": "workdir_busy", "conflict": conflict},
                ),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        try:
            extra_files: list[dict[str, Any]] = []
            version_hash = ""
            sandbox_agent_id = data.get("agent_id")
            if self.mode == "local":
                info = await self.skills.ensure_skill_local(
                    project_id=parts.project_id, skill_name=skill_name.strip()
                )
                version_hash = info["version_hash"]
                local_dir = Path(info["local_dir"])
                skill_root = _resolve_skill_root(local_dir, skill_name.strip())

                for file_path in skill_root.rglob("*"):
                    if not file_path.is_file():
                        continue
                    rel = file_path.relative_to(skill_root).as_posix()
                    target_path = f"skill/{rel}"
                    data_bytes = file_path.read_bytes()
                    if len(data_bytes) > self.skills.max_file_bytes:
                        raise BadRequestError(f"skill file too large: {rel}")
                    extra_files.append({"path": target_path, "data": data_bytes})
            else:
                if session_id:
                    sandbox_agent_id = session_id
                version_hash = await self._ensure_remote_activated(
                    project_id=parts.project_id,
                    agent_id=sandbox_agent_id,
                    skill_name=skill_name.strip(),
                    tool_call_id=data.get("tool_call_id"),
                    timeout_sec=args.get("timeout_sec"),
                )

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "tool_call_id=%s skill_name=%s session_id=%s command=%s timeout=%s workdir=%s",
                    tool_call_meta.get("tool_call_id"),
                    skill_name,
                    session_id,
                    _truncate_for_log(command),
                    args.get("timeout_sec"),
                    "skill",
                )

            result = await self.executor.execute(
                project_id=parts.project_id,
                agent_id=sandbox_agent_id if self.mode != "local" else data.get("agent_id"),
                tool_call_id=data.get("tool_call_id"),
                session_id=session_id if self.mode == "local" else None,
                language="bash",
                code=command,
                inputs=self._normalize_inputs(args.get("inputs"), workdir="skill"),
                outputs=args.get("outputs"),
                timeout_sec=args.get("timeout_sec"),
                workdir="skill",
                extra_files=extra_files,
            )
            status = STATUS_SUCCESS if result.exit_code == 0 else STATUS_FAILED
            result_obj = {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
                "created_artifacts": result.created_artifacts,
                "output_errors": result.output_errors,
                "timing": {"duration_ms": result.duration_ms},
                "version_hash": version_hash,
                "workdir": "skill",
            }
            if self.mode == "local":
                result_obj["skill_root"] = str(skill_root)
                if session_id:
                    result_obj["session_id"] = session_id
            return self._create_result_package(parts, data, tool_call_meta, status, result_obj, None)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _handle_run_background_command(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        skill_name = args.get("skill_name")
        command = args.get("command")
        mode = args.get("mode") or "batch"
        session_id = self._resolve_session_id(
            parts=parts,
            data=data,
            args=args,
            tool_call_meta=tool_call_meta,
        )
        if not isinstance(skill_name, str) or not skill_name.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("skill_name is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        if not isinstance(command, str) or not command.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("command is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        if str(mode).lower() == "service" and self.mode != "local":
            result, error = build_error_result_from_exception(
                BadRequestError("run_service only supported in local mode"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        if str(mode).lower() != "service":
            conflict = await self._find_session_conflict(session_id=session_id, workdir=args.get("workdir"))
            if conflict:
                result, error = build_error_result_from_exception(
                    ConflictError(
                        "session workdir is busy; use skills.job_status/skills.task_status and retry",
                        detail={"reason": "workdir_busy", "conflict": conflict},
                    ),
                    source="tool",
                )
                return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

        expose_ports = self._normalize_expose_ports(args.get("expose_ports"))
        endpoints: list[dict[str, Any]] = []
        host_procs: list[Any] = []
        forwarder_pids: dict[str, int] = {}
        forwarder_start_times: dict[str, int] = {}
        wrapped_command = command
        if expose_ports and str(mode).lower() == "service":
            try:
                expose_cmd, endpoints = self._build_expose_ports_command(
                    project_id=parts.project_id,
                    session_id=str(session_id or "").strip(),
                    workdir=str(args.get("workdir") or "skill"),
                    ports=expose_ports,
                )
                wrapped_command = f"{expose_cmd}\nexec {command}"
            except Exception as exc:  # noqa: BLE001
                result, error = build_error_result_from_exception(exc, source="tool")
                return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

        task_id = await self._spawn_bg_task(
            parts=parts,
            data=data,
            skill_name=skill_name.strip(),
            command=wrapped_command,
            session_id=session_id,
            timeout_sec=args.get("timeout_sec"),
            mode=str(mode).lower(),
            env=args.get("env") if isinstance(args, dict) else None,
            workdir=args.get("workdir"),
            inputs=args.get("inputs"),
            outputs=args.get("outputs"),
        )
        if expose_ports and str(mode).lower() == "service":
            try:
                await self._await_expose_sockets(endpoints)
                host_procs, forwarder_pids, forwarder_start_times = await self._start_host_forwarders(endpoints)
                async with self._bg_lock:
                    task = self._bg_tasks.get(task_id)
                    if task:
                        task["exposed_endpoints"] = endpoints
                        task["forwarder_procs"] = host_procs
                        task["forwarder_pids"] = forwarder_pids
                        task["forwarder_start_times"] = forwarder_start_times
                        task["exposed_endpoints_active"] = True
                await self._persist_task_snapshot(task_id, dict(self._bg_tasks.get(task_id) or {}))
            except Exception as exc:  # noqa: BLE001
                try:
                    await self._cancel_task(task_id)
                except Exception:
                    logger.exception("failed to cancel task after expose_ports error")
                result, error = build_error_result_from_exception(exc, source="tool")
                return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        result_obj = {
            "task_id": task_id,
            "provider": "srt" if self.mode == "local" else "e2b",
            "status": "queued",
            "session_id": session_id,
            "workdir": str(args.get("workdir") or "skill"),
            "skill_name": skill_name.strip(),
            "mode": "service" if str(mode).lower() == "service" else "batch",
        }
        if expose_ports and str(mode).lower() == "service":
            result_obj["exposed_endpoints"] = endpoints
        return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)

    async def _handle_run_cmd_async(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        args = dict(args or {})
        args["mode"] = "batch"
        return await self._handle_run_background_command(parts, data, tool_call_meta, args)

    async def _handle_run_service(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        session_id = args.get("session_id")
        if not isinstance(session_id, str) or not session_id.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("session_id is required for run_service"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        args = dict(args or {})
        args["mode"] = "service"
        return await self._handle_run_background_command(parts, data, tool_call_meta, args)

    async def _handle_activate(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        skill_name = args.get("skill_name")
        session_id = self._resolve_session_id(
            parts=parts,
            data=data,
            args=args,
            tool_call_meta=tool_call_meta,
        )
        if not isinstance(skill_name, str) or not skill_name.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("skill_name is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        dest_dir = "skill"
        zip_path = "/tmp/skill.zip"
        unzip = True
        keep_archive = False
        timeout_sec = args.get("timeout_sec")
        try:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "tool_call_id=%s skill_name=%s session_id=%s mode=%s",
                    tool_call_meta.get("tool_call_id"),
                    skill_name,
                    session_id,
                    self.mode,
                )
            if self.mode == "local":
                info = await self.skills.ensure_skill_local(
                    project_id=parts.project_id, skill_name=skill_name.strip()
                )
                result_obj = {
                    "mode": "local",
                    "skill_name": skill_name.strip(),
                    "version_hash": info["version_hash"],
                    "local_dir": info["local_dir"],
                    "active_path": info["active_path"],
                }
                if session_id:
                    result_obj["session_id"] = session_id
                return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)

            zip_bytes, version_hash = await self._fetch_skill_zip_bytes(
                project_id=parts.project_id, skill_name=skill_name.strip()
            )
            sandbox_agent_id = session_id or data.get("agent_id")
            result_obj = await self.executor.prepare_remote_zip(
                project_id=parts.project_id,
                agent_id=sandbox_agent_id,
                tool_call_id=data.get("tool_call_id"),
                zip_bytes=zip_bytes,
                dest_dir=str(dest_dir),
                zip_path=str(zip_path),
                unzip=bool(unzip),
                keep_archive=bool(keep_archive),
                timeout_sec=int(timeout_sec) if timeout_sec is not None else None,
            )
            sandbox_id = str(result_obj.get("sandbox_id") or "")
            if sandbox_id:
                by_skill = self._remote_activations.setdefault(sandbox_id, {})
                by_skill[skill_name.strip()] = version_hash
            result_obj["mode"] = "remote"
            result_obj["version_hash"] = version_hash
            result_obj["skill_name"] = skill_name.strip()
            if session_id:
                result_obj["session_id"] = session_id
            return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _handle_start_job(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        session_id = args.get("session_id")
        steps = args.get("steps")
        if not isinstance(session_id, str) or not session_id.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("session_id is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        if not isinstance(steps, list) or not steps:
            result, error = build_error_result_from_exception(
                BadRequestError("steps must be a non-empty list"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        skill_name = args.get("skill_name")
        if not isinstance(skill_name, str) or not skill_name.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("skill_name is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

        workdir = str(args.get("workdir") or "skill")
        conflict = await self._find_session_conflict(session_id=session_id, workdir=workdir)
        if conflict:
            result, error = build_error_result_from_exception(
                ConflictError(
                    "session workdir is busy; use skills.job_status/skills.task_status and retry",
                    detail={"reason": "workdir_busy", "conflict": conflict},
                ),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        job_env = args.get("env") if isinstance(args.get("env"), dict) else {}
        max_wall_time_sec = int(args.get("max_wall_time_sec") or self._job_default_timeout_sec)
        created_at = to_iso(utc_now())
        job_id = uuid6.uuid7().hex
        step_defs = []
        for step in steps:
            if not isinstance(step, dict):
                continue
            cmd = step.get("command")
            if not isinstance(cmd, str) or not cmd.strip():
                continue
            step_defs.append(
                {
                    "name": step.get("name"),
                    "command": cmd,
                    "timeout_sec": step.get("timeout_sec"),
                    "env": step.get("env") if isinstance(step.get("env"), dict) else {},
                    "status": "queued",
                    "task_id": None,
                    "exit_code": None,
                }
            )
        if not step_defs:
            result, error = build_error_result_from_exception(
                BadRequestError("steps must include at least one valid command"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

        async with self._jobs_lock:
            self._jobs[job_id] = {
                "job_id": job_id,
                "status": "queued",
                "project_id": parts.project_id,
                "session_id": session_id.strip(),
                "skill_name": skill_name.strip(),
                "workdir": workdir,
                "env": job_env,
                "max_concurrency": 1,
                "max_wall_time_sec": max_wall_time_sec,
                "current_step_index": -1,
                "steps": step_defs,
                "created_at": created_at,
                "started_at": None,
                "finished_at": None,
                "last_updated_at": created_at,
                "error_message": None,
            }
            job_snapshot = dict(self._jobs[job_id])
        await self._persist_job_snapshot(job_id, job_snapshot)

        asyncio.create_task(self._run_job(job_id, parts, data))
        result_obj = {
            "job_id": job_id,
            "status": "queued",
            "session_id": session_id.strip(),
        }
        return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)

    async def _get_job_status_snapshot(self, job_id: str, tail_bytes: int) -> Dict[str, Any]:
        job = None
        async with self._jobs_lock:
            job = self._jobs.get(job_id)
        if not job and self._task_store:
            stored = await self._task_store.get_job(job_id)
            if stored:
                job = stored
        if not job:
            raise NotFoundError(f"job not found: {job_id}")

        steps_out = []
        for step in job.get("steps", []):
            task_id = step.get("task_id")
            step_status = step.get("status")
            step_out = {
                "name": step.get("name"),
                "status": step_status,
                "task_id": task_id,
                "exit_code": step.get("exit_code"),
            }
            if task_id:
                try:
                    snap = await self._get_bg_task_snapshot(str(task_id), tail_bytes)
                    step_out["status"] = snap.get("status")
                    step_out["exit_code"] = snap.get("exit_code")
                    step_out["stdout_tail"] = snap.get("stdout_tail")
                    step_out["stderr_tail"] = snap.get("stderr_tail")
                    step_out["provider"] = snap.get("provider")
                except Exception:
                    pass
            steps_out.append(step_out)

        return {
            "job_id": job_id,
            "status": job.get("status"),
            "current_step_index": job.get("current_step_index"),
            "steps": steps_out,
            "started_at": job.get("started_at"),
            "finished_at": job.get("finished_at"),
            "error_message": job.get("error_message"),
        }

    async def _handle_job_status(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        job_id = args.get("job_id")
        tail_bytes = int(args.get("tail_bytes") or 8192)
        if not isinstance(job_id, str) or not job_id.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("job_id is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        try:
            result_obj = await self._get_job_status_snapshot(job_id, tail_bytes)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)

    async def _handle_job_watch(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        poll_interval = max(1, int(args.get("poll_interval_sec") or 2))
        timeout_sec = int(args.get("timeout_sec") or 600)
        heartbeat_sec = max(5, int(args.get("heartbeat_sec") or 20))
        dedupe_key = str(args.get("dedupe_key") or "").strip()
        job_id = args.get("job_id")
        if not isinstance(job_id, str) or not job_id.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("job_id is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        if not dedupe_key:
            dedupe_key = f"{parts.project_id}:{data.get('agent_id')}:{job_id}"
        started = time.monotonic()
        last_emit = 0.0
        last_status: Dict[str, Any] = {}
        try:
            existing = self._watch_registry.get(dedupe_key)
            if existing and existing.get("expires_at", 0) > time.monotonic():
                result_obj = {
                    "job_watch": "already_running",
                    "dedupe_key": dedupe_key,
                    "status": existing.get("last_status"),
                }
                return self._create_result_package(
                    parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None
                )
            while True:
                last_status = await self._get_job_status_snapshot(
                    str(job_id), int(args.get("tail_bytes") or 8192)
                )
                self._watch_registry[dedupe_key] = {
                    "expires_at": time.monotonic() + timeout_sec,
                    "last_status": last_status,
                }
                state_val = str(last_status.get("status") or "").lower()
                done = state_val in ("succeeded", "failed", "canceled")
                if done:
                    break
                if time.monotonic() - started > timeout_sec:
                    raise TimeoutError("job_watch timeout exceeded")
                now = time.monotonic()
                if now - last_emit >= heartbeat_sec:
                    await self._emit_task_watch_heartbeat(
                        parts=parts,
                        data=data,
                        tool_call_meta=tool_call_meta,
                        status_payload={"job_watch": "running", "status": last_status},
                    )
                    last_emit = now
                await asyncio.sleep(poll_interval)
            last_status["timing"] = {"duration_ms": int((time.monotonic() - started) * 1000)}
            status = STATUS_SUCCESS if state_val == "succeeded" else STATUS_FAILED
            if dedupe_key in self._watch_registry:
                self._watch_registry.pop(dedupe_key, None)
            return self._create_result_package(parts, data, tool_call_meta, status, last_status, None)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            if last_status:
                result = {"last_status": last_status, "error": str(exc)}
            if dedupe_key in self._watch_registry:
                self._watch_registry.pop(dedupe_key, None)
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _run_job(self, job_id: str, parts: Any, data: Dict[str, Any]) -> None:
        async with self._jobs_lock:
            job = self._jobs.get(job_id)
        if not job:
            return
        lock_key = self._session_lock_key(job.get("session_id"), job.get("workdir"))
        lock = self._get_session_lock(lock_key)
        async with lock:
            async with self._jobs_lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                job["status"] = "running"
                job["started_at"] = to_iso(utc_now())
                job["last_updated_at"] = job["started_at"]
                start_ts = time.monotonic()
                max_wall = int(job.get("max_wall_time_sec") or self._job_default_timeout_sec)
                deadline = start_ts + max_wall
                job_snapshot = dict(job)
            await self._persist_job_snapshot(job_id, job_snapshot)

            steps = list(job.get("steps") or [])
            for idx, step in enumerate(steps):
                async with self._jobs_lock:
                    job = self._jobs.get(job_id)
                    if not job:
                        return
                    if job.get("status") == "canceled":
                        return
                async with self._jobs_lock:
                    job = self._jobs.get(job_id)
                    if not job:
                        return
                    job["current_step_index"] = idx
                    step["status"] = "starting"
                    job["last_updated_at"] = to_iso(utc_now())
                    job_snapshot = dict(job)
                await self._persist_job_snapshot(job_id, job_snapshot)

                merged_env = dict(job.get("env") or {})
                step_env = step.get("env") if isinstance(step.get("env"), dict) else {}
                merged_env.update(step_env)
                task_id = await self._spawn_bg_task(
                    parts=parts,
                    data=data,
                    skill_name=job.get("skill_name") or "",
                    command=step.get("command") or "",
                    session_id=job.get("session_id"),
                    timeout_sec=step.get("timeout_sec"),
                    env=merged_env,
                    workdir=job.get("workdir"),
                )
                async with self._jobs_lock:
                    job = self._jobs.get(job_id)
                    if not job:
                        return
                    step["task_id"] = task_id
                    step["status"] = "running"
                    job["last_updated_at"] = to_iso(utc_now())
                    job_snapshot = dict(job)
                await self._persist_job_snapshot(job_id, job_snapshot)

                # Poll task status
                while True:
                    if time.monotonic() > deadline:
                        try:
                            await self._cancel_task(task_id)
                        except Exception:
                            logger.exception("failed to cancel task on job timeout: %s", task_id)
                        async with self._jobs_lock:
                            job = self._jobs.get(job_id)
                            if not job:
                                return
                            job["status"] = "failed"
                            job["error_message"] = "job_timeout"
                            job["finished_at"] = to_iso(utc_now())
                            job["last_updated_at"] = job["finished_at"]
                            step["status"] = "canceled"
                            job_snapshot = dict(job)
                        await self._persist_job_snapshot(job_id, job_snapshot)
                        return
                    snap = await self._get_bg_task_snapshot(task_id, 0)
                    status = snap.get("status")
                    if status in ("succeeded", "failed", "canceled"):
                        async with self._jobs_lock:
                            job = self._jobs.get(job_id)
                            if not job:
                                return
                            step["status"] = status
                            step["exit_code"] = snap.get("exit_code")
                            job["last_updated_at"] = to_iso(utc_now())
                            job_snapshot = dict(job)
                        await self._persist_job_snapshot(job_id, job_snapshot)
                        if status != "succeeded":
                            async with self._jobs_lock:
                                job = self._jobs.get(job_id)
                                if not job:
                                    return
                                job["status"] = "failed"
                                job["error_message"] = "step_failed"
                                job["finished_at"] = to_iso(utc_now())
                                job["last_updated_at"] = job["finished_at"]
                                job_snapshot = dict(job)
                            await self._persist_job_snapshot(job_id, job_snapshot)
                            return
                        break
                    await asyncio.sleep(1)

            async with self._jobs_lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                job["status"] = "succeeded"
                job["finished_at"] = to_iso(utc_now())
                job["last_updated_at"] = job["finished_at"]
                job_snapshot = dict(job)
            await self._persist_job_snapshot(job_id, job_snapshot)

    async def _handle_job_cancel(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        job_id = args.get("job_id")
        reason = args.get("reason") if isinstance(args.get("reason"), str) else "canceled"
        if not isinstance(job_id, str) or not job_id.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("job_id is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        async with self._jobs_lock:
            job = self._jobs.get(job_id)
            if not job:
                result, error = build_error_result_from_exception(
                    NotFoundError(f"job not found: {job_id}"),
                    source="tool",
                )
                return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
            job["status"] = "canceled"
            job["error_message"] = reason
            job["finished_at"] = to_iso(utc_now())
            job["last_updated_at"] = job["finished_at"]
            current_idx = job.get("current_step_index", -1)
            steps = job.get("steps") or []
            if isinstance(current_idx, int) and 0 <= current_idx < len(steps):
                steps[current_idx]["status"] = "canceled"
                task_id = steps[current_idx].get("task_id")
            else:
                task_id = None
            job_snapshot = dict(job)
        await self._persist_job_snapshot(job_id, job_snapshot)
        if task_id:
            try:
                await self._cancel_task(task_id)
            except Exception:
                logger.exception("failed to cancel task on job_cancel: %s", task_id)
        result_obj = {"job_id": job_id, "status": "canceled", "reason": reason}
        return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)
    def _extract_task_state(
        self,
        payload: Dict[str, Any],
        *,
        status_field: str,
        progress_field: Optional[str],
        events_field: Optional[str],
        done_states: list[str],
        error_states: list[str],
    ) -> Dict[str, Any]:
        state = payload.get(status_field)
        if state is None:
            for candidate in ("status", "state", "job_status", "jobState"):
                if candidate in payload:
                    state = payload.get(candidate)
                    break
        state_str = str(state) if state is not None else "unknown"
        normalized = state_str.lower()
        done = normalized in done_states
        failed = normalized in error_states
        progress = payload.get(progress_field) if progress_field else None
        events = payload.get(events_field) if events_field else None
        return {
            "state": state_str,
            "done": done,
            "error": failed,
            "progress": progress,
            "events": events,
        }

    async def _query_task_cmd(
        self,
        *,
        parts: Any,
        data: Dict[str, Any],
        command: str,
        timeout_sec: int,
        workdir: str,
        session_id: Optional[str],
    ) -> Dict[str, Any]:
        sandbox_agent_id = session_id or data.get("agent_id")
        result = await self.executor.execute(
            project_id=parts.project_id,
            agent_id=sandbox_agent_id if self.mode != "local" else data.get("agent_id"),
            tool_call_id=data.get("tool_call_id"),
            session_id=session_id if self.mode == "local" else None,
            language="bash",
            code=command,
            inputs=None,
            outputs=None,
            timeout_sec=timeout_sec,
            workdir=workdir,
            extra_files=[],
        )
        payload: Dict[str, Any] = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "exit_code": result.exit_code,
            "timing": {"duration_ms": result.duration_ms},
        }
        if result.stdout:
            try:
                payload["json"] = json.loads(result.stdout)
            except Exception:
                pass
        return payload

    async def _get_task_status(
        self,
        *,
        parts: Any,
        data: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Dict[str, Any]:
        provider = str(args.get("provider") or "cmd").lower()
        task_id = args.get("task_id") or args.get("job_id") or args.get("id")
        tail_bytes = int(args.get("tail_bytes") or 8192)
        session_id = self._resolve_session_id(
            parts=parts,
            data=data,
            args=args,
            tool_call_meta={},
        )
        status_field = str(args.get("status_field") or "status")
        progress_field = args.get("progress_field")
        events_field = args.get("events_field")
        done_states = [s.lower() for s in (args.get("done_states") or ["done", "success", "completed"])]
        error_states = [s.lower() for s in (args.get("error_states") or ["failed", "error", "errored"])]
        timeout_sec = int(args.get("timeout_sec") or 30)

        raw_payload: Dict[str, Any] = {}
        if provider == "bg":
            if not task_id:
                raise BadRequestError("task_id is required for provider=bg")
            snap = await self._get_bg_task_snapshot(str(task_id), tail_bytes)
            if session_id and not snap.get("session_id"):
                snap["session_id"] = session_id
            return snap
        if provider == "cmd":
            command = args.get("query_command")
            if not isinstance(command, str) or not command.strip():
                raise BadRequestError("query_command is required for provider=cmd")
            workdir = str(args.get("workdir") or "skill")
            raw_payload = await self._query_task_cmd(
                parts=parts,
                data=data,
                command=command.strip(),
                timeout_sec=timeout_sec,
                workdir=workdir,
                session_id=session_id,
            )
            if "json" in raw_payload:
                raw_payload = raw_payload["json"]
        else:
            raise BadRequestError("provider must be cmd")

        extracted = self._extract_task_state(
            raw_payload,
            status_field=status_field,
            progress_field=progress_field,
            events_field=events_field,
            done_states=done_states,
            error_states=error_states,
        )
        return {
            "task_id": task_id,
            "provider": provider,
            "status_field": status_field,
            "state": extracted["state"],
            "done": extracted["done"],
            "error": extracted["error"],
            "progress": extracted["progress"],
            "events": extracted["events"],
            "raw": raw_payload,
        }

    async def _handle_task_status(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        try:
            result_obj = await self._get_task_status(parts=parts, data=data, args=args)
            return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _handle_task_cancel(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        task_id = args.get("task_id") or args.get("id")
        if not isinstance(task_id, str) or not task_id.strip():
            result, error = build_error_result_from_exception(
                BadRequestError("task_id is required"),
                source="tool",
            )
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)
        try:
            result_obj = await self._cancel_task(task_id.strip())
            return self._create_result_package(parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _handle_task_watch(
        self,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Any]:
        poll_interval = max(1, int(args.get("poll_interval_sec") or 2))
        timeout_sec = int(args.get("timeout_sec") or 600)
        heartbeat_sec = max(5, int(args.get("heartbeat_sec") or 20))
        dedupe_key = str(args.get("dedupe_key") or "").strip()
        if not dedupe_key:
            task_id = args.get("task_id") or args.get("job_id") or args.get("id") or ""
            dedupe_key = f"{parts.project_id}:{data.get('agent_id')}:{task_id}:{args.get('query_command')}"
        started = time.monotonic()
        last_emit = 0.0
        last_status: Dict[str, Any] = {}
        try:
            existing = self._watch_registry.get(dedupe_key)
            if existing and existing.get("expires_at", 0) > time.monotonic():
                result_obj = {
                    "task_watch": "already_running",
                    "dedupe_key": dedupe_key,
                    "status": existing.get("last_status"),
                }
                return self._create_result_package(
                    parts, data, tool_call_meta, STATUS_SUCCESS, result_obj, None
                )
            while True:
                last_status = await self._get_task_status(parts=parts, data=data, args=args)
                self._watch_registry[dedupe_key] = {
                    "expires_at": time.monotonic() + timeout_sec,
                    "last_status": last_status,
                }
                if last_status.get("done") or last_status.get("error"):
                    break
                if time.monotonic() - started > timeout_sec:
                    raise TimeoutError("task_watch timeout exceeded")
                now = time.monotonic()
                if now - last_emit >= heartbeat_sec:
                    await self._emit_task_watch_heartbeat(
                        parts=parts,
                        data=data,
                        tool_call_meta=tool_call_meta,
                        status_payload=last_status,
                    )
                    last_emit = now
                await asyncio.sleep(poll_interval)
            last_status["timing"] = {"duration_ms": int((time.monotonic() - started) * 1000)}
            status = STATUS_SUCCESS if not last_status.get("error") else STATUS_FAILED
            if dedupe_key in self._watch_registry:
                self._watch_registry.pop(dedupe_key, None)
            return self._create_result_package(parts, data, tool_call_meta, status, last_status, None)
        except Exception as exc:  # noqa: BLE001
            result, error = build_error_result_from_exception(exc, source="tool")
            if last_status:
                result = {"last_status": last_status, "error": str(exc)}
            if dedupe_key in self._watch_registry:
                self._watch_registry.pop(dedupe_key, None)
            return self._create_result_package(parts, data, tool_call_meta, STATUS_FAILED, result, error)

    async def _emit_task_watch_heartbeat(
        self,
        *,
        parts: Any,
        data: Dict[str, Any],
        tool_call_meta: Dict[str, Any],
        status_payload: Dict[str, Any],
    ) -> None:
        tool_call_id = data.get("tool_call_id")
        payload_out, card = self._create_result_package(
            parts,
            data,
            tool_call_meta,
            STATUS_SUCCESS,
            {
                "task_watch": "running",
                "status": status_payload,
            },
            None,
            after_execution="suspend",
        )
        await self.cardbox.save_card(card)
        safe_headers, _, _ = normalize_headers(
            nats=self.nats,
            headers={},
            default_depth=0,
        )
        await self._report_tool_result_and_wakeup(
            parts=parts,
            payload=payload_out,
            correlation_id=str(tool_call_id or ""),
            safe_headers=safe_headers,
            fallback_agent_id=str(data.get("agent_id") or ""),
        )

    async def _report_tool_result_and_wakeup(
        self,
        *,
        parts: Any,
        payload: Dict[str, Any],
        correlation_id: str,
        safe_headers: Dict[str, str],
        fallback_agent_id: str,
    ) -> None:
        resolved_agent_id = str(payload.get("agent_id") or fallback_agent_id or "")
        async with self.execution_store.pool.connection() as conn:
            async with conn.transaction():
                report_result = await report_primitive(
                    store=self.execution_store,
                    project_id=parts.project_id,
                    channel_id=parts.channel_id,
                    target_agent_id=resolved_agent_id,
                    message_type="tool_result",
                    payload=payload,
                    correlation_id=correlation_id,
                    headers=safe_headers,
                    traceparent=safe_headers.get("traceparent"),
                    tracestate=safe_headers.get("tracestate"),
                    trace_id=trace_id_from_headers(safe_headers),
                    parent_step_id=str(payload.get("step_id") or ""),
                    source_agent_id="tool.skills",
                    conn=conn,
                )
        safe_headers = dict(safe_headers)
        safe_headers["traceparent"] = report_result.traceparent
        target = await resolve_agent_target(
            resource_store=self.resource_store,
            project_id=parts.project_id,
            agent_id=resolved_agent_id,
        )
        wakeup_subject = format_subject(
            parts.project_id,
            parts.channel_id,
            "cmd",
            "agent",
            target,
            "wakeup",
        )
        await self.nats.publish_event(
            wakeup_subject,
            {
                "agent_id": resolved_agent_id,
                "agent_turn_id": payload.get("agent_turn_id"),
            },
            headers=safe_headers,
        )

    def _create_result_package(
        self,
        parts: Any,
        original_data: Dict[str, Any],
        tool_call_meta: Optional[Dict[str, Any]],
        status: str,
        result: Any = None,
        error: Optional[Dict[str, Any]] = None,
        after_execution: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Any]:
        if after_execution is None:
            after_execution = original_data.get("after_execution")
        tool_name = original_data.get("tool_name") or "skills"
        result_context = ToolResultContext.from_cmd_data(
            project_id=parts.project_id,
            cmd_data=original_data,
            tool_call_meta=tool_call_meta,
        )
        return ToolResultBuilder(
            result_context,
            author_id="tool.skills",
            function_name=tool_name,
            error_source="tool",
        ).build(
            status=status,
            result=result,
            error=error,
            function_name=tool_name,
            after_execution=after_execution,
        )

    async def _handle_cmd(self, subject: str, data: Dict[str, Any], headers: Dict[str, str]):
        try:
            parts = parse_subject(subject)
            if not parts or parts.target != "skills":
                return
            try:
                payload = ToolCommandPayload.validate(
                    data,
                    require_tool_name=True,
                    require_after_execution=True,
                    allowed_after_execution=ALLOWED_AFTER_EXECUTION_VALUES,
                )
            except (ValidationError, BadRequestError) as exc:
                logger.error("Invalid tool payload: %s", exc)
                return

            tool_name = payload.tool_name
            tool_call_id = payload.tool_call_id
            agent_id = payload.agent_id

            safe_headers = dict(headers or {})
            correlation_id = str(tool_call_id or "")
            safe_headers, _, _ = normalize_headers(
                nats=self.nats,
                headers=safe_headers,
                default_depth=0,
            )
            key = build_idempotency_key("tool", parts.project_id, str(tool_name), str(tool_call_id))

            async def _leader_execute():
                payload_out, card = await self._execute_logic(parts, data, headers)
                await self.cardbox.save_card(card)
                return payload_out

            try:
                outcome = await self.idempotent_executor.execute(key, _leader_execute)
            except TimeoutError:
                logger.warning("Idempotency timeout for key=%s; returning timeout result", key)
                entry = await self.idempotency_store.get_entry(key)
                if entry and entry.get("status") == "done":
                    outcome = IdempotencyOutcome(is_leader=False, payload=entry.get("payload"))
                else:
                    tool_call_meta: Dict[str, Any] = {}
                    tool_call_card_id = data.get("tool_call_card_id")
                    if tool_call_card_id:
                        cards = await self.cardbox.get_cards(
                            [str(tool_call_card_id)],
                            project_id=parts.project_id,
                        )
                        tool_call_card = cards[0] if cards else None
                        if tool_call_card and getattr(tool_call_card, "type", None) == "tool.call":
                            tool_call_meta = extract_tool_call_metadata(tool_call_card)
                    error_detail = {"key": key, "status": entry.get("status") if entry else None}
                    error = build_error_payload(
                        "idempotency_timeout",
                        "idempotency wait timed out; result unknown",
                        detail=error_detail,
                        source="tool",
                        retryable=True,
                        http_status=504,
                    )
                    result = {
                        "error_code": error.get("code"),
                        "error_message": error.get("message"),
                        "error": error,
                        "error_detail": error_detail,
                    }
                    payload_out, card = self._create_result_package(
                        parts,
                        data,
                        tool_call_meta,
                        STATUS_TIMEOUT,
                        result,
                        error,
                    )
                    await self.cardbox.save_card(card)
                    outcome = IdempotencyOutcome(is_leader=False, payload=payload_out)
            await self._report_tool_result_and_wakeup(
                parts=parts,
                payload=outcome.payload,
                correlation_id=str(tool_call_id),
                safe_headers=safe_headers,
                fallback_agent_id=str(agent_id),
            )
        except Exception:
            logger.exception("Unexpected error handling %s", subject)


async def main() -> None:
    cfg = _load_config()
    service = SkillsToolService(cfg)
    try:
        await service.start()
    finally:
        await service.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
