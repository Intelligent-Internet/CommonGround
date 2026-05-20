from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4


DEFAULT_PROJECT_ID = os.environ.get("CG_PROJECT_ID", "cg-demo")
DEFAULT_REQUESTED_BY = os.environ.get("CG_FRONTSIDE_AGENT_ID", "frontside")
DEFAULT_TARGET_AGENT = os.environ.get("CG_NANOBOT_A_AGENT_ID", "nanobot_a")
DEFAULT_TURN_KIND = "turn.conversation.v1"
DEFAULT_CG_BIN = os.environ.get(
    "CG_BIN",
    str(Path(__file__).resolve().parents[2] / ".venv" / "bin" / "cg"),
)
DEFAULT_PAYLOAD_FILE = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "byoa"
    / "conversation_worker"
    / "root_request.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.demo.run_cg_skill_roundtrip",
        description="Dispatch a CommonGround request through the cg CLI and wait for terminal completion.",
    )
    parser.add_argument("--cg-bin", default=DEFAULT_CG_BIN)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--requested-by", default=DEFAULT_REQUESTED_BY)
    parser.add_argument("--target-agent", default=DEFAULT_TARGET_AGENT)
    parser.add_argument("--turn-kind", default=DEFAULT_TURN_KIND)
    parser.add_argument("--payload-file", default=str(DEFAULT_PAYLOAD_FILE))
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--poll-interval-ms", type=int, default=500)
    return parser


def _run_json_command(argv: list[str]) -> dict:
    completed = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        check=False,
    )
    if not completed.stdout.strip():
        raise RuntimeError(
            f"command returned no stdout: {' '.join(argv)}\nstderr:\n{completed.stderr.strip()}"
        )
    try:
        envelope = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"command returned non-JSON stdout: {' '.join(argv)}\nstdout:\n{completed.stdout}"
        ) from exc
    if not envelope.get("ok"):
        error = envelope.get("error") or {}
        raise RuntimeError(
            f"cg command failed: {' '.join(argv)}\n"
            f"code={error.get('code')} message={error.get('message')}"
        )
    return envelope


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    request_id = f"cg-skill-demo:{uuid4().hex}"
    dispatch_envelope = _run_json_command(
        [
            args.cg_bin,
            "dispatch",
            "--project-id",
            args.project_id,
            "--requested-by",
            args.requested_by,
            "--target-agent",
            args.target_agent,
            "--turn-kind",
            args.turn_kind,
            "--request-id",
            request_id,
            "--dispatch-key",
            request_id,
            "--payload-file",
            args.payload_file,
        ]
    )
    turn_id = dispatch_envelope["result"]["turn_id"]
    wait_envelope = _run_json_command(
        [
            args.cg_bin,
            "turn",
            "wait",
            "--project-id",
            args.project_id,
            "--turn-id",
            turn_id,
            "--timeout-seconds",
            str(args.timeout_seconds),
            "--poll-interval-ms",
            str(args.poll_interval_ms),
        ]
    )
    summary = {
        "dispatch": dispatch_envelope["result"],
        "final": {
            "turn_id": wait_envelope["result"]["turn_id"],
            "state": wait_envelope["result"]["state"],
            "outcome": wait_envelope["result"]["outcome"],
            "final_record_role": wait_envelope["result"]["final_record_role"],
            "final_payload": wait_envelope["result"]["final_payload"],
        },
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
