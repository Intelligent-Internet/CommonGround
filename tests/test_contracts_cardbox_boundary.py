from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys

from CommonGround.contracts import CardBoxRef, HydratedCardBox


def test_hydrated_cardbox_payload_uses_structural_content_types() -> None:
    text_box = HydratedCardBox(
        ref=CardBoxRef(project_id="cg-demo", cardbox_id="box-text"),
        box=SimpleNamespace(card_ids=("card-1",)),
        cards=(SimpleNamespace(content=SimpleNamespace(text="hello"), metadata={}),),
    )
    json_box = HydratedCardBox(
        ref=CardBoxRef(project_id="cg-demo", cardbox_id="box-json"),
        box=SimpleNamespace(card_ids=("card-1",)),
        cards=(SimpleNamespace(content=SimpleNamespace(data={"ok": True}), metadata={}),),
    )

    assert text_box.payload() == "hello"
    assert json_box.payload() == {"ok": True}


def test_contracts_and_kernel_import_without_cardbox_dependency() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    code = """
import builtins

real_import = builtins.__import__

def blocked(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "cardbox" or name.startswith("cardbox.") or name == "card_box_core" or name.startswith("card_box_core."):
        raise RuntimeError(f"unexpected import: {name}")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = blocked

import CommonGround.contracts
import CommonGround.kernel.lifecycle
import CommonGround.kernel.semantic

print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
