from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from scripts.demo.nanobot_interactive_console import (
    ConsoleConfig,
    _build_payload,
    _decode_console_bytes,
    _lineage_items,
    _read_console_line,
    _submit_turn,
    _watch_until_closed,
)


def _config() -> ConsoleConfig:
    return ConsoleConfig(
        base_url="http://cg.example",
        project_id="demo",
        requested_by="frontside",
        target_agent="nanobot_a",
        turn_kind="turn.conversation.v1",
        timeout_seconds=5.0,
        poll_interval_seconds=0.01,
    )


def test_submit_turn_uses_dispatch_with_request_id_only(monkeypatch, tmp_path: Path) -> None:
    payloads: list[dict] = []
    captured_args: list[list[str]] = []

    monkeypatch.setattr("scripts.demo.nanobot_interactive_console.uuid.uuid4", lambda: "abc123")

    def fake_run_cg_json(args: list[str], *, env: dict[str, str]) -> dict[str, str]:
        captured_args.append(args)
        payload_index = args.index("--payload-file") + 1
        payload_path = Path(args[payload_index])
        assert payload_path.exists()
        payloads.append(json.loads(payload_path.read_text(encoding="utf-8")))
        return {
            "project_id": "demo",
            "turn_id": "T-1",
            "request_id": "interactive-abc123",
            "dispatch_key": "interactive-abc123",
        }

    monkeypatch.setattr("scripts.demo.nanobot_interactive_console._run_cg_json", fake_run_cg_json)

    result = _submit_turn(
        _config(),
        "hello",
        [{"role": "user", "content": "older context"}],
        env={"CG_BASE_URL": "http://cg.example"},
    )

    assert result["turn_id"] == "T-1"
    assert captured_args == [
        [
            "dispatch",
            "--project-id",
            "demo",
            "--requested-by",
            "frontside",
            "--target-agent",
            "nanobot_a",
            "--turn-kind",
            "turn.conversation.v1",
            "--request-id",
            "interactive-abc123",
            "--payload-file",
            captured_args[0][-1],
        ]
    ]
    assert payloads == [
        {
            "task": "hello",
            "instructions": {
                "style": "concise",
                "preserve_context": True,
            },
            "conversation_context": [{"role": "user", "content": "older context"}],
            "execution_input": {
                "user_message": "hello",
                "conversation_context": [{"role": "user", "content": "older context"}],
            },
            "expected_output": {
                "type": "text",
                "style": "complete_when_task_requires_detail",
            },
        }
    ]
    assert not Path(captured_args[0][-1]).exists()


def test_build_payload_enables_orchestration_only_for_complex_requests() -> None:
    simple = _build_payload("who am I?", [{"role": "user", "content": "my name is Demo User"}])
    simple_agent_question = _build_payload("what agent are you and which LLM drives you?", [])
    complex_payload = _build_payload(
        "split the topics across different subagents, research them separately, and summarize the report",
        [],
    )

    assert "orchestration" not in simple
    assert "orchestration" not in simple_agent_question
    assert complex_payload["orchestration"] == {
        "mode": "auto",
        "max_child_tasks": 4,
        "strategy": "split research, comparison, multi-topic requests, or explicit subagent requests across child agents",
    }


def test_lineage_items_flattens_and_sorts_parent_then_children() -> None:
    items = _lineage_items(
        {
            "parent": {"turn_id": "T-2", "project_turn_seq": 2},
            "direct_children": [
                {"turn_id": "T-4", "project_turn_seq": 4},
                {"turn_id": "T-3", "project_turn_seq": 3},
            ],
        }
    )

    assert [item["turn_id"] for item in items] == ["T-2", "T-3", "T-4"]


def test_watch_until_closed_uses_root_turn_as_authoritative_state(monkeypatch, capsys) -> None:
    turns = iter(
        [
            {"turn_id": "T-1", "state": "running", "outcome": None},
            {
                "turn_id": "T-1",
                "state": "closed",
                "outcome": "succeeded",
                "final_payload": {"content": "done"},
            },
        ]
    )
    lineages = iter(
        [
            {
                "parent": {
                    "turn_id": "T-1",
                    "project_turn_seq": 1,
                    "target_agent_id": "nanobot_a",
                    "turn_kind": "turn.conversation.v1",
                    "cause_kind": "external_request",
                    "cause_id": "interactive-1",
                    "state": "running",
                    "outcome": None,
                },
                "direct_children": [],
            },
            {
                "parent": {
                    "turn_id": "T-1",
                    "project_turn_seq": 1,
                    "target_agent_id": "nanobot_a",
                    "turn_kind": "turn.conversation.v1",
                    "cause_kind": "external_request",
                    "cause_id": "interactive-1",
                    "state": "running",
                    "outcome": None,
                },
                "direct_children": [
                    {
                        "turn_id": "T-2",
                        "project_turn_seq": 2,
                        "target_agent_id": "child",
                        "turn_kind": "turn.conversation.v1",
                        "cause_kind": "turn",
                        "cause_id": "T-1",
                        "state": "closed",
                        "outcome": "succeeded",
                    }
                ],
            },
        ]
    )
    monotonic_values = iter([0.0, 1.0, 2.0])

    monkeypatch.setattr("scripts.demo.nanobot_interactive_console._get_turn", lambda config, turn_id, *, env: next(turns))
    monkeypatch.setattr("scripts.demo.nanobot_interactive_console._get_lineage", lambda config, turn_id, *, env: next(lineages))
    monkeypatch.setattr("scripts.demo.nanobot_interactive_console.time.monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr("scripts.demo.nanobot_interactive_console.time.sleep", lambda _: None)

    result = _watch_until_closed(
        _config(),
        {"turn_id": "T-1", "request_id": "interactive-1"},
        env={"CG_BASE_URL": "http://cg.example"},
    )

    assert result["state"] == "closed"
    assert result["final_payload"] == {"content": "done"}
    stdout = capsys.readouterr().out
    assert "submitted root turn: T-1 request_id=interactive-1" in stdout
    assert "T-1 nanobot_a turn.conversation.v1 cause=external_request:interactive-1 status=running" in stdout
    assert "T-2 child turn.conversation.v1 cause=turn:T-1 status=closed/succeeded" in stdout


def test_decode_console_bytes_falls_back_to_gb18030() -> None:
    raw = bytes.fromhex("ced2cac7b4f3c3c8c3c8")

    assert _decode_console_bytes(raw).encode("gb18030") == raw


def test_read_console_line_reads_raw_bytes_without_input_decode_crash(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "scripts.demo.nanobot_interactive_console.sys.stdin",
        SimpleNamespace(buffer=SimpleNamespace(readline=lambda: "hello\n".encode("utf-8")), encoding="utf-8"),
    )

    assert _read_console_line("cg> ") == "hello"
    assert capsys.readouterr().out == "cg> "


def test_read_console_line_raises_eof_on_empty_stream(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.demo.nanobot_interactive_console.sys.stdin",
        SimpleNamespace(buffer=SimpleNamespace(readline=lambda: b""), encoding="utf-8"),
    )

    try:
        _read_console_line("cg> ")
    except EOFError:
        return
    raise AssertionError("expected EOFError")
