import json
from pathlib import Path

from scripts.demo.send_demo_request import (
    _extract_requester_result_from_session,
    _extract_requester_result_from_text,
    _nanobot_agent_argv,
    _session_path,
)


def _write_session(path: Path, messages: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        {
            "_type": "metadata",
            "key": "cg-requester-test",
            "created_at": "2026-04-28T00:00:00",
            "updated_at": "2026-04-28T00:00:01",
            "metadata": {},
            "last_consolidated": 0,
        },
        *messages,
    ]
    path.write_text(
        "".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines),
        encoding="utf-8",
    )


def test_nanobot_agent_argv_uses_explicit_session(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/nanobot" if name == "nanobot" else None)

    argv = _nanobot_agent_argv(
        tmp_path / "config.json",
        tmp_path / "workspace",
        "hello",
        session_id="cg-requester-test",
    )

    assert argv[0] == "/usr/bin/nanobot"
    assert argv[argv.index("--session") + 1] == "cg-requester-test"


def test_session_path_targets_workspace_session_file(tmp_path: Path) -> None:
    assert _session_path(tmp_path / "workspace", "cg-requester-test") == (
        tmp_path / "workspace" / "sessions" / "cg-requester-test.jsonl"
    )


def test_extract_requester_result_from_session_prefers_terminal_wait_result(tmp_path: Path) -> None:
    session = tmp_path / "sessions" / "cg-requester-test.jsonl"
    _write_session(
        session,
        [
            {
                "role": "tool",
                "content": json.dumps(
                    {
                        "ok": True,
                        "result": {
                            "project_id": "demo",
                            "turn_id": "T-1",
                            "dispatch_key": "req-1",
                        },
                    }
                ),
            },
            {
                "role": "tool",
                "content": json.dumps(
                    {
                        "ok": True,
                        "result": {
                            "project_id": "demo",
                            "turn_id": "T-1",
                            "state": "closed",
                            "final_record_role": "assistant.final",
                            "final_payload": {"answer": 42},
                            "snapshot": {"large": "omitted"},
                        },
                    }
                ),
            },
            {"role": "assistant", "content": "Done."},
        ],
    )

    assert _extract_requester_result_from_session(session) == {
        "project_id": "demo",
        "turn_id": "T-1",
        "final_record_role": "assistant.final",
        "final_payload": {"answer": 42},
    }


def test_extract_requester_result_from_session_recovers_turn_ref(tmp_path: Path) -> None:
    session = tmp_path / "sessions" / "cg-requester-test.jsonl"
    _write_session(
        session,
        [
            {"role": "assistant", "content": "dispatching"},
            {
                "role": "tool",
                "content": json.dumps(
                    {
                        "ok": True,
                        "result": {
                            "project_id": "demo",
                            "turn_id": "T-2",
                            "dispatch_key": "req-2",
                        },
                    }
                ),
            },
        ],
    )

    assert _extract_requester_result_from_session(session) == {
        "project_id": "demo",
        "turn_id": "T-2",
    }


def test_extract_requester_result_from_text_keeps_stdout_fallback() -> None:
    result = _extract_requester_result_from_text(
        'progress\n{"project_id":"demo","turn_id":"T-3","final_record_role":"assistant.final","final_payload":{"ok":true}}\n'
    )

    assert result == {
        "project_id": "demo",
        "turn_id": "T-3",
        "final_record_role": "assistant.final",
        "final_payload": {"ok": True},
    }
