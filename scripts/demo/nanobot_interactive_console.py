from __future__ import annotations

import argparse
import json
import locale
import os
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common_agent_demo import load_demo_env


ROOT = Path(__file__).resolve().parents[2]
CG_BIN = ROOT / ".venv" / "bin" / "cg"
DEFAULT_TURN_KIND = "turn.conversation.v1"
_FALLBACK_INPUT_ENCODINGS = ("utf-8", "gb18030")
_ORCHESTRATION_KEYWORDS = (
    "subagent",
    "subagents",
    "multi-agent",
    "split",
    "decompose",
    "parallel",
    "multiple",
    "multi-topic",
    "research",
    "summarize",
    "report",
    "compare",
)


@dataclass(frozen=True, slots=True)
class ConsoleConfig:
    base_url: str
    project_id: str
    requested_by: str
    target_agent: str
    turn_kind: str
    timeout_seconds: float
    poll_interval_seconds: float


def _run_cg_json(args: list[str], *, env: dict[str, str]) -> dict[str, Any]:
    completed = subprocess.run(
        [str(CG_BIN), *args],
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
    envelope = json.loads(completed.stdout)
    if not envelope.get("ok"):
        raise RuntimeError(f"cg command returned error: {json.dumps(envelope, ensure_ascii=False)}")
    return envelope["result"]


def _cg_env(config: ConsoleConfig) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("CG_BASE_URL", config.base_url)
    env.setdefault("CG_CALLER_PROJECT_ID", config.project_id)
    env.setdefault("CG_CALLER_AGENT_ID", config.requested_by)
    return env


def _build_payload(user_message: str, history: list[dict[str, str]]) -> dict[str, Any]:
    recent_history = history[-8:]
    payload = {
        "task": user_message,
        "instructions": {
            "style": "concise",
            "preserve_context": True,
        },
        "conversation_context": recent_history,
        "execution_input": {
            "user_message": user_message,
            "conversation_context": recent_history,
        },
        "expected_output": {
            "type": "text",
            "style": "complete_when_task_requires_detail",
        },
    }
    if _should_orchestrate(user_message):
        payload["orchestration"] = {
            "mode": "auto",
            "max_child_tasks": 4,
            "strategy": "split research, comparison, multi-topic requests, or explicit subagent requests across child agents",
        }
    return payload


def _should_orchestrate(user_message: str) -> bool:
    normalized = user_message.casefold()
    return any(keyword.casefold() in normalized for keyword in _ORCHESTRATION_KEYWORDS)


def _write_payload_file(payload: dict[str, Any]) -> Path:
    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".json",
        prefix="cg-nanobot-console-",
        delete=False,
    )
    try:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
        return Path(tmp.name)
    finally:
        tmp.close()


def _submit_turn(config: ConsoleConfig, user_message: str, history: list[dict[str, str]], *, env: dict[str, str]) -> dict[str, Any]:
    request_id = f"interactive-{uuid.uuid4()}"
    payload_file = _write_payload_file(_build_payload(user_message, history))
    try:
        return _run_cg_json(
            [
                "dispatch",
                "--project-id",
                config.project_id,
                "--requested-by",
                config.requested_by,
                "--target-agent",
                config.target_agent,
                "--turn-kind",
                config.turn_kind,
                "--request-id",
                request_id,
                "--payload-file",
                str(payload_file),
            ],
            env=env,
        )
    finally:
        payload_file.unlink(missing_ok=True)


def _list_turns(config: ConsoleConfig, *, env: dict[str, str], limit: int = 30) -> list[dict[str, Any]]:
    result = _run_cg_json(
        [
            "project",
            "turn",
            "list",
            "--project-id",
            config.project_id,
            "--limit",
            str(limit),
        ],
        env=env,
    )
    return list(result.get("items") or [])


def _get_turn(config: ConsoleConfig, turn_id: str, *, env: dict[str, str]) -> dict[str, Any]:
    return _run_cg_json(
        [
            "turn",
            "get",
            "--project-id",
            config.project_id,
            "--turn-id",
            turn_id,
        ],
        env=env,
    )


def _get_lineage(config: ConsoleConfig, turn_id: str, *, env: dict[str, str]) -> dict[str, Any]:
    return _run_cg_json(
        [
            "project",
            "turn",
            "lineage",
            "--project-id",
            config.project_id,
            "--turn-id",
            turn_id,
        ],
        env=env,
    )


def _lineage_items(lineage: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    parent = lineage.get("parent")
    if isinstance(parent, dict):
        items.append(parent)
    for item in lineage.get("direct_children") or []:
        if isinstance(item, dict):
            items.append(item)
    return sorted(items, key=lambda item: int(item.get("project_turn_seq") or 0))


def _print_turn_row(item: dict[str, Any], *, prefix: str = "") -> None:
    state = item.get("state")
    outcome = item.get("outcome")
    status = f"{state}/{outcome}" if outcome else str(state)
    print(
        f"{prefix}{item.get('turn_id')} "
        f"{item.get('target_agent_id')} "
        f"{item.get('turn_kind')} "
        f"cause={item.get('cause_kind')}:{item.get('cause_id')} "
        f"status={status}",
        flush=True,
    )


def _print_recent_turns(config: ConsoleConfig, *, env: dict[str, str], limit: int = 12) -> None:
    for item in _list_turns(config, env=env, limit=limit):
        _print_turn_row(item)


def _watch_until_closed(config: ConsoleConfig, submit_result: dict[str, Any], *, env: dict[str, str]) -> dict[str, Any]:
    root_turn_id = submit_result["turn_id"]
    request_id = submit_result["request_id"]
    deadline = time.monotonic() + config.timeout_seconds
    seen: dict[str, tuple[Any, Any, Any]] = {}
    print(f"submitted root turn: {root_turn_id} request_id={request_id}", flush=True)

    while True:
        turn = _get_turn(config, root_turn_id, env=env)
        for item in _lineage_items(_get_lineage(config, root_turn_id, env=env)):
            fingerprint = (item.get("state"), item.get("outcome"))
            turn_id = str(item.get("turn_id"))
            if seen.get(turn_id) != fingerprint:
                seen[turn_id] = fingerprint
                _print_turn_row(item, prefix="  ")
        if turn.get("state") == "closed":
            return turn
        if time.monotonic() >= deadline:
            raise TimeoutError(f"turn {root_turn_id} did not close within {config.timeout_seconds} seconds")
        time.sleep(config.poll_interval_seconds)


def _final_text(result: dict[str, Any]) -> str:
    payload = result.get("final_payload")
    if isinstance(payload, dict):
        content = payload.get("content")
        if isinstance(content, str) and content:
            return content
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _handle_message(config: ConsoleConfig, line: str, history: list[dict[str, str]], *, env: dict[str, str]) -> None:
    history.append({"role": "user", "content": line})
    submit_result = _submit_turn(config, line, history, env=env)
    result = _watch_until_closed(config, submit_result, env=env)
    answer = _final_text(result)
    history.append({"role": "assistant", "content": answer})
    print("\nfinal:")
    print(answer)
    print("")


def _build_parser() -> argparse.ArgumentParser:
    env = load_demo_env()
    parser = argparse.ArgumentParser(description="Interactive CommonGround NanoBot demo console")
    parser.add_argument("--project-id", default=env.project_id)
    parser.add_argument("--requested-by", default=env.frontside_agent_id)
    parser.add_argument("--target-agent", default=env.nanobot_a_agent_id)
    parser.add_argument("--turn-kind", default=DEFAULT_TURN_KIND)
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--poll-interval-ms", type=int, default=750)
    return parser


def _decode_console_bytes(raw: bytes) -> str:
    encodings: list[str] = []
    for candidate in (
        getattr(sys.stdin, "encoding", None),
        locale.getpreferredencoding(False),
        *_FALLBACK_INPUT_ENCODINGS,
    ):
        if isinstance(candidate, str) and candidate and candidate not in encodings:
            encodings.append(candidate)
    for encoding in encodings:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode(encodings[0] if encodings else "utf-8", errors="replace")


def _read_console_line(prompt: str) -> str:
    print(prompt, end="", flush=True)
    raw = sys.stdin.buffer.readline()
    if raw == b"":
        raise EOFError
    return _decode_console_bytes(raw).rstrip("\r\n")


def main() -> None:
    env_defaults = load_demo_env()
    args = _build_parser().parse_args()
    config = ConsoleConfig(
        base_url=env_defaults.base_url,
        project_id=args.project_id,
        requested_by=args.requested_by,
        target_agent=args.target_agent,
        turn_kind=args.turn_kind,
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_ms / 1000.0,
    )
    env = _cg_env(config)
    history: list[dict[str, str]] = []

    print("CommonGround NanoBot interactive console")
    print(f"project={config.project_id} requested_by={config.requested_by} target={config.target_agent}")
    print("Commands: /help /turns /clear /quit")
    print("")

    while True:
        try:
            line = _read_console_line("cg> ")
        except EOFError:
            print("")
            return
        line = line.strip()
        if not line:
            continue
        if line in {"/quit", "/exit"}:
            return
        if line == "/help":
            print("Type a user message to submit a new root turn. Use /turns to inspect recent turns.")
            print("This console keeps local conversation context, but each message is a new CommonGround turn.")
            continue
        if line == "/turns":
            _print_recent_turns(config, env=env)
            continue
        if line == "/clear":
            history.clear()
            print("local conversation context cleared")
            continue
        try:
            _handle_message(config, line, history, env=env)
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
