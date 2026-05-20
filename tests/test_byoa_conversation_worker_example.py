from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_minimal_byoa_worker_runtime_writes_final_payload(tmp_path: Path) -> None:
    context_file = tmp_path / "context.json"
    final_file = tmp_path / "final.json"
    context_file.write_text(
        json.dumps(
            {
                "turn": {"turn": {"project_id": "cg-demo", "turn_id": "T-1"}},
                "semantic_items": [{"record": {"record_role": "bootstrap"}}],
            }
        ),
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "CG_CONTEXT_FILE": str(context_file),
        "CG_FINAL_FILE": str(final_file),
        "CG_AGENT_ID": "worker-1",
    }

    subprocess.run(
        [sys.executable, "examples/byoa/conversation_worker/worker_runtime.py"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(final_file.read_text(encoding="utf-8"))
    assert payload == {
        "summary": "Minimal BYOA worker completed the turn.",
        "turn_id": "T-1",
        "agent_id": "worker-1",
        "semantic_item_count": 1,
        "latest_record_role": "bootstrap",
    }
