#!/usr/bin/env python3
"""
Review a single commit and report lines with indentation changes.

Usage:
    python scripts/review_indent_changes.py <commit>
    python scripts/review_indent_changes.py <commit> --base <base_commit>
    python scripts/review_indent_changes.py <commit> --all-indent-changes
"""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class IndentFinding:
    path: str
    old_line: int | None
    new_line: int | None
    old_text: str
    new_text: str
    kind: str
    reason: str


def run(cmd: list[str]) -> str:
    return subprocess.check_output(cmd, text=True).strip("\n")


def leading_indent_len(text: str) -> int:
    stripped = text.lstrip(" \t")
    return len(text) - len(stripped)


def detect_indent_changes(
    removed: List[Tuple[int, str]],
    added: List[Tuple[int, str]],
    path: str,
) -> List[IndentFinding]:
    """
    Compare a removed block and an added block.
    We report:
      - whitespace-only indent changes
      - indentation changes when content also changes but same logical line likely moved/replaced
    """
    findings: List[IndentFinding] = []

    # Primary matching path: same code body with different leading whitespace.
    matched_added = set()
    matched_removed = set()

    for ri, (old_line, old_text) in enumerate(removed):
        old_code = old_text.lstrip(" \t")
        for ai, (new_line, new_text) in enumerate(added):
            if ai in matched_added:
                continue
            if new_text.lstrip(" \t") == old_code and old_code.strip() != "":
                old_indent = leading_indent_len(old_text)
                new_indent = leading_indent_len(new_text)
                if old_indent != new_indent:
                    findings.append(
                        IndentFinding(
                            path=path,
                            old_line=old_line,
                            new_line=new_line,
                            old_text=old_text.rstrip("\n"),
                            new_text=new_text.rstrip("\n"),
                            kind="indent-only",
                            reason="same content, different indentation",
                        )
                    )
                matched_added.add(ai)
                matched_removed.add(ri)
                break

    # Secondary pass: one-to-one fallback for still-unmatched lines.
    for offset, (removed_item, added_item) in enumerate(zip(removed, added)):
        ri, ai = offset, offset
        if ri in matched_removed or ai in matched_added:
            continue
        old_line, old_text = removed_item
        new_line, new_text = added_item
        old_code = old_text.lstrip(" \t")
        new_code = new_text.lstrip(" \t")
        if old_code.strip() == "" and new_code.strip() == "":
            continue
        old_indent = leading_indent_len(old_text)
        new_indent = leading_indent_len(new_text)
        if old_indent != new_indent:
            kind = "indent-only" if old_code == new_code else "indent+content"
            reason = (
                "same content, different indentation"
                if old_code == new_code
                else "indentation and content changed together"
            )
            findings.append(
                IndentFinding(
                    path=path,
                    old_line=old_line,
                    new_line=new_line,
                    old_text=old_text.rstrip("\n"),
                    new_text=new_text.rstrip("\n"),
                    kind=kind,
                    reason=reason,
                )
            )

    return findings


def parse_diff(diff_text: str) -> List[IndentFinding]:
    hunk_header = re.compile(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
    findings: List[IndentFinding] = []

    current_file = None
    removed_block: List[Tuple[int, str]] = []
    added_block: List[Tuple[int, str]] = []
    old_line = 0
    new_line = 0
    in_hunk = False

    def flush_blocks():
        nonlocal removed_block, added_block
        if removed_block and added_block:
            findings.extend(detect_indent_changes(removed_block, added_block, current_file))
        removed_block = []
        added_block = []

    for raw in diff_text.splitlines():
        if raw.startswith("diff --git"):
            flush_blocks()
            in_hunk = False
            current_file = None
            continue
        if raw.startswith("--- "):
            continue
        if raw.startswith("+++ "):
            # Format: +++ b/path
            path = raw[4:].split("\t", 1)[0]
            current_file = path[2:] if path.startswith("b/") else path
            continue

        match = hunk_header.match(raw)
        if match:
            flush_blocks()
            in_hunk = True
            old_line = int(match.group(1))
            new_line = int(match.group(3))
            continue

        if not in_hunk or current_file is None:
            continue

        if raw == r"\ No newline at end of file":
            continue

        if not raw:
            continue

        line_type = raw[0]
        content = raw[1:]
        if line_type == "-":
            removed_block.append((old_line, content))
            old_line += 1
            continue
        if line_type == "+":
            added_block.append((new_line, content))
            new_line += 1
            continue
        if line_type == " ":
            flush_blocks()
            old_line += 1
            new_line += 1
            continue

    flush_blocks()
    return findings


def main() -> int:
    p = argparse.ArgumentParser(description="review indentation changes in a commit")
    p.add_argument("commit", help="commit hash, e.g. HEAD~1")
    p.add_argument(
        "--base",
        help="explicit base commit; default is <commit>^",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="print findings as JSON-like lines",
    )
    p.add_argument(
        "--all-indent-changes",
        action="store_true",
        help="include lines where content changed together with indentation (default: only pure indentation changes)",
    )
    args = p.parse_args()

    base = args.base if args.base else f"{args.commit}^"
    diff = run(["git", "diff", "--unified=0", base, args.commit])
    findings = parse_diff(diff)
    if not args.all_indent_changes:
        findings = [f for f in findings if f.kind == "indent-only"]

    if not findings:
        if args.json:
            print("[]")
        else:
            print(f"No indentation changes detected between {base} and {args.commit}.")
        return 0

    if args.json:
        import json

        print(
            json.dumps(
                [
                    {
                        "path": f.path,
                        "old_line": f.old_line,
                        "new_line": f.new_line,
                        "kind": f.kind,
                        "reason": f.reason,
                        "old_text": f.old_text,
                        "new_text": f.new_text,
                    }
                    for f in findings
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(f"Indentation changes between {base} and {args.commit}:")
        for f in findings:
            print(f"{f.path}: -{f.old_line} +{f.new_line} [{f.kind}] {f.reason}")
            print(f"  before: {f.old_text}")
            print(f"  after:  {f.new_text}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
