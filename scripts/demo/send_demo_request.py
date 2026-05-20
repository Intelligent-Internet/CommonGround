from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

from .common_agent_demo import load_demo_env


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PAYLOAD_FILE = ROOT / "examples" / "byoa" / "conversation_worker" / "root_request.json"
CG_SKILL_DIR = ROOT / "examples" / "skills" / "cg"
CG_BIN_DIR = ROOT / ".venv" / "bin"


def _load_base_config(config_path: str | None) -> dict:
    if config_path is None:
        default = Path.home() / ".nanobot" / "config.json"
        if not default.exists():
            raise FileNotFoundError(
                "NANOBOT_CONFIG_PATH is not set and ~/.nanobot/config.json was not found"
            )
        config_path = str(default)
    path = Path(config_path).expanduser().resolve()
    return json.loads(path.read_text(encoding="utf-8"))


def _merged_requester_config(base_config: dict) -> dict:
    merged = deepcopy(base_config)
    tools = merged.setdefault("tools", {})
    exec_cfg = tools.setdefault("exec", {})
    existing = exec_cfg.get("pathAppend", "")
    segments = [segment for segment in str(existing).split(os.pathsep) if segment]
    cg_bin = str(CG_BIN_DIR)
    if cg_bin not in segments:
        segments.append(cg_bin)
    exec_cfg["pathAppend"] = os.pathsep.join(segments)
    allowed = list(exec_cfg.get("allowedEnvKeys") or [])
    for key in (
        "CG_BASE_URL",
        "CG_AUTH_TOKEN",
        "CG_AUTH_TOKEN_FILE",
        "CG_CONFIG_PATH",
        "CG_CALLER_PROJECT_ID",
        "CG_CALLER_AGENT_ID",
    ):
        if key not in allowed:
            allowed.append(key)
    exec_cfg["allowedEnvKeys"] = allowed
    return merged


def _install_cg_skill(workspace: Path) -> None:
    skills_root = workspace / "skills"
    skills_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(CG_SKILL_DIR, skills_root / "cg", dirs_exist_ok=True)


def _nanobot_agent_argv(config_path: Path, workspace: Path, message: str, *, session_id: str) -> list[str]:
    nanobot_bin = shutil.which("nanobot")
    if nanobot_bin:
        return [
            nanobot_bin,
            "agent",
            "--config",
            str(config_path),
            "--workspace",
            str(workspace),
            "--session",
            session_id,
            "--message",
            message,
            "--no-markdown",
        ]
    return [
        "uv",
        "run",
        "nanobot",
        "agent",
        "--config",
        str(config_path),
        "--workspace",
        str(workspace),
        "--session",
        session_id,
        "--message",
        message,
        "--no-markdown",
    ]


def _extract_json_objects(text: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            objects.append(payload)
    return objects


def _normalize_requester_result(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, bool]:
    if payload.get("ok") is True and isinstance(payload.get("result"), dict):
        payload = payload["result"]
    project_id = payload.get("project_id")
    turn_id = payload.get("turn_id")
    if not isinstance(project_id, str) or not isinstance(turn_id, str):
        return None, False

    result: dict[str, Any] = {
        "project_id": project_id,
        "turn_id": turn_id,
    }
    is_terminal_result = False
    if "final_record_role" in payload:
        result["final_record_role"] = payload["final_record_role"]
        is_terminal_result = True
    if "final_payload" in payload:
        result["final_payload"] = payload["final_payload"]
        is_terminal_result = True
    return result, is_terminal_result


def _extract_requester_result_from_text(text: str) -> dict[str, Any] | None:
    first_json: dict[str, Any] | None = None
    first_turn_ref: dict[str, Any] | None = None
    for payload in _extract_json_objects(text):
        if first_json is None:
            first_json = payload
        candidate, terminal = _normalize_requester_result(payload)
        if terminal:
            return candidate
        if candidate is not None and first_turn_ref is None:
            first_turn_ref = candidate
    return first_turn_ref or first_json


def _session_path(workspace: Path, session_id: str) -> Path:
    return workspace / "sessions" / f"{session_id}.jsonl"


def _extract_requester_result_from_session(session_path: Path) -> dict[str, Any] | None:
    if not session_path.exists():
        return None

    messages: list[dict[str, Any]] = []
    with session_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and payload.get("_type") != "metadata":
                messages.append(payload)

    first_turn_ref: dict[str, Any] | None = None
    for message in reversed(messages):
        if message.get("role") not in {"assistant", "tool"}:
            continue
        content = message.get("content")
        if not isinstance(content, str):
            continue
        for payload in reversed(_extract_json_objects(content)):
            candidate, terminal = _normalize_requester_result(payload)
            if terminal:
                return candidate
            if candidate is not None and first_turn_ref is None:
                first_turn_ref = candidate
    return first_turn_ref


def _terminal_result(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    candidate, terminal = _normalize_requester_result(payload)
    if not terminal:
        return None
    return candidate


def _turn_ref(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    candidate, _ = _normalize_requester_result(payload)
    return candidate


def _run_cg_json(args: list[str], *, env: dict[str, str]) -> dict[str, Any]:
    completed = subprocess.run(
        [str(CG_BIN_DIR / "cg"), *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "cg command failed\n"
            f"args: {args}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    payload = json.loads(completed.stdout)
    if not payload.get("ok"):
        raise RuntimeError(f"cg command returned error: {payload}")
    return payload["result"]


def _request_message(*, payload_file: Path, project_id: str, requested_by: str, target_agent: str, request_id: str) -> str:
    return (
        "Use the cg skill from your workspace. "
        f"Dispatch {payload_file} to CommonGround with project_id {project_id}, "
        f"requested_by {requested_by}, target_agent {target_agent}, and turn_kind turn.conversation.v1. "
        f"Use request_id {request_id} and dispatch_key {request_id}. "
        "Then wait for terminal completion and respond with exactly one JSON object containing "
        "project_id, turn_id, final_record_role, and final_payload. "
        "Do not use curl. Do not use cg worker. Do not call the CommonGround HTTP API directly."
    )


def main() -> None:
    env = load_demo_env()
    payload_file = DEFAULT_PAYLOAD_FILE
    workspace = Path(env.nanobot_requester_workspace).expanduser().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    _install_cg_skill(workspace)

    runtime_config = _merged_requester_config(_load_base_config(env.nanobot_config_path))
    temp_config_path = workspace / "cg-requester.config.json"
    temp_config_path.write_text(
        json.dumps(runtime_config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    request_id = f"demo-{uuid.uuid4()}"
    session_id = f"cg-requester-{uuid.uuid4().hex}"
    child_env = os.environ.copy()
    child_env.setdefault("CG_BASE_URL", env.base_url)
    child_env.setdefault("CG_CALLER_PROJECT_ID", env.project_id)
    child_env.setdefault("CG_CALLER_AGENT_ID", env.frontside_agent_id)

    completed = subprocess.run(
        _nanobot_agent_argv(
            temp_config_path,
            workspace,
            _request_message(
                payload_file=payload_file,
                project_id=env.project_id,
                requested_by=env.frontside_agent_id,
                target_agent=env.nanobot_a_agent_id,
                request_id=request_id,
            ),
            session_id=session_id,
        ),
        cwd=env.nanobot_repo_root or ROOT,
        env=child_env,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "nanobot requester failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    session_path = _session_path(workspace, session_id)
    session_result = _extract_requester_result_from_session(session_path)
    stdout_result = _extract_requester_result_from_text(completed.stdout)
    result = _terminal_result(session_result) or _terminal_result(stdout_result)
    turn_ref = _turn_ref(session_result) or _turn_ref(stdout_result)
    if result is None:
        if turn_ref is None:
            turn_ref = _run_cg_json(
                [
                    "dispatch",
                    "--project-id",
                    env.project_id,
                    "--requested-by",
                    env.frontside_agent_id,
                    "--target-agent",
                    env.nanobot_a_agent_id,
                    "--turn-kind",
                    "turn.conversation.v1",
                    "--request-id",
                    request_id,
                    "--dispatch-key",
                    request_id,
                    "--payload-file",
                    str(payload_file),
                ],
                env=child_env,
            )
        result = _run_cg_json(
            [
                "turn",
                "wait",
                "--project-id",
                turn_ref["project_id"],
                "--turn-id",
                turn_ref["turn_id"],
                "--timeout-seconds",
                "120",
                "--poll-interval-ms",
                "500",
            ],
            env=child_env,
        )
    print(
        json.dumps(
            {
                "project_id": result["project_id"],
                "turn_id": result["turn_id"],
                "final_record_role": result["final_record_role"],
                "final_payload": result["final_payload"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
