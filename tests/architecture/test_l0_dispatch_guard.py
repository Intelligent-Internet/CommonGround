from __future__ import annotations

from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[2]

GUARDED_FILES = [
    "services/agent_worker/utp_dispatcher.py",
    "services/tools/tool_caller.py",
    "services/api/main.py",
    "services/pmo/l0_guard/guard.py",
]

FORBIDDEN_PATTERNS = [
    r"\.insert_execution_edge\(",
    r"\.insert_execution_edge_with_conn\(",
    r"\.publish_event\(",
]


def test_no_direct_edge_write_or_cmd_publish_in_guarded_files() -> None:
    violations: list[str] = []
    for relative_path in GUARDED_FILES:
        file_path = REPO_ROOT / relative_path
        content = file_path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_PATTERNS:
            for match in re.finditer(pattern, content):
                line_no = content.count("\n", 0, match.start()) + 1
                violations.append(f"{relative_path}:{line_no}: matched {pattern}")

    assert not violations, "Direct edge/cmd publish is forbidden outside L0:\n" + "\n".join(violations)
