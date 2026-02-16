#!/usr/bin/env python3
"""
Generate a full-workspace NATS subject inventory (static scan).

This script scans Python sources for:
This script scans Python source code for:
  - core.subject.format_subject(...) calls (standardized subject generator)
  - publish_event / publish_core calls
  - subscribe_cmd / subscribe_core calls
  - Literal or f-string subject expressions like cg.<ver>... (best-effort parsing)

Scan results are output to a Markdown file under docs/REFERENCE/ (reproducible and deterministic order).
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "docs" / "REFERENCE" / "nats_subjects.generated.md"

SCAN_DIRS = ("core", "infra", "services", "scripts", "examples", "tests")
EXCLUDE_DIR_NAMES = {".git", ".venv", "__pycache__", "card-box-cg"}


def _iter_python_files() -> Iterable[Path]:
    for base in SCAN_DIRS:
        root = REPO_ROOT / base
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.py")):
            if any(part in EXCLUDE_DIR_NAMES for part in path.parts):
                continue
            yield path


def _render_str(node: ast.AST, *, resolve_name: Optional[Dict[str, str]] = None) -> Optional[str]:
    resolve_name = resolve_name or {}

    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value

    if isinstance(node, ast.JoinedStr):
        parts: List[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
                continue
            if isinstance(v, ast.FormattedValue):
                if isinstance(v.value, ast.Name) and v.value.id in resolve_name:
                    parts.append(resolve_name[v.value.id])
                    continue
                try:
                    expr = ast.unparse(v.value).strip()
                except Exception:
                    expr = "expr"
                parts.append("{" + expr + "}")
                continue
            return None
        return "".join(parts)

    if isinstance(node, ast.Name):
        return resolve_name.get(node.id)

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _render_str(node.left, resolve_name=resolve_name)
        right = _render_str(node.right, resolve_name=resolve_name)
        if left is None or right is None:
            return None
        return left + right

    return None


def _render_token(node: ast.AST, *, resolve_name: Optional[Dict[str, str]] = None) -> str:
    resolve_name = resolve_name or {}
    s = _render_str(node, resolve_name=resolve_name)
    if s is not None:
        return s
    try:
        return "{" + ast.unparse(node).strip() + "}"
    except Exception:
        return "{expr}"


def _looks_like_cg_subject(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    if not s.startswith("cg."):
        return False

    # Avoid misclassifying incomplete fragments in f-strings (such as "cg.") as subjects.
    # Only treat them as subjects when they clearly contain category segments or wildcard suffixes.
    if any(token in s for token in (".cmd.", ".evt.", ".str.", ".dat.")):
        return True
    if s.endswith(".>") or ".>" in s:
        return True
    return False


@dataclass(frozen=True)
class SubjectHit:
    subject: str
    transport: str  # jetstream | core | unknown
    direction: str  # publish | subscribe | literal
    api: str  # publish_event/publish_core/subscribe_cmd/subscribe_core/format_subject/literal
    file: str
    line: int
    queue: Optional[str] = None
    durable: Optional[str] = None


class _Collector(ast.NodeVisitor):
    def __init__(self, *, rel_path: str) -> None:
        self._rel_path = rel_path
        self.hits: List[SubjectHit] = []

        # Simple resolver per function: supports assignments like name = "cg...." or f"cg...."
        self._resolver_stack: List[Dict[str, str]] = [dict()]

    @property
    def _resolver(self) -> Dict[str, str]:
        return self._resolver_stack[-1]

    def _add(
        self,
        *,
        subject: str,
        transport: str,
        direction: str,
        api: str,
        line: int,
        queue: Optional[str] = None,
        durable: Optional[str] = None,
    ) -> None:
        self.hits.append(
            SubjectHit(
                subject=subject,
                transport=transport,
                direction=direction,
                api=api,
                file=self._rel_path,
                line=line,
                queue=queue,
                durable=durable,
            )
        )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._resolver_stack.append(dict(self._resolver))
        self.generic_visit(node)
        self._resolver_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._resolver_stack.append(dict(self._resolver))
        self.generic_visit(node)
        self._resolver_stack.pop()

    def visit_Assign(self, node: ast.Assign) -> Any:
        value = _render_str(node.value, resolve_name=self._resolver)
        if isinstance(value, str):
            for t in node.targets:
                if not isinstance(t, ast.Name):
                    continue
                name = t.id
                if _looks_like_cg_subject(value) or any(
                    kw in name.lower() for kw in ("subject", "queue", "durable")
                ):
                    self._resolver[name] = value
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> Any:
        if node.value is not None and isinstance(node.target, ast.Name):
            value = _render_str(node.value, resolve_name=self._resolver)
            if isinstance(value, str):
                name = node.target.id
                if _looks_like_cg_subject(value) or any(
                    kw in name.lower() for kw in ("subject", "queue", "durable")
                ):
                    self._resolver[name] = value
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> Any:
        fn_name = None
        if isinstance(node.func, ast.Name):
            fn_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            fn_name = node.func.attr

        if fn_name == "format_subject":
            self._handle_format_subject(node)
        elif fn_name in {"publish_event", "publish_core", "subscribe_cmd", "subscribe_cmd_with_ack", "subscribe_core"}:
            self._handle_nats_api(fn_name, node)

        self.generic_visit(node)

    def _handle_format_subject(self, node: ast.Call) -> None:
        # format_subject(project_id, channel_id, category, component, target, suffix, protocol_version=...)
        args = list(node.args)
        if len(args) < 6:
            return

        project_id = _render_token(args[0], resolve_name=self._resolver)
        channel_id = _render_token(args[1], resolve_name=self._resolver)
        category = _render_token(args[2], resolve_name=self._resolver)
        component = _render_token(args[3], resolve_name=self._resolver)
        target = _render_token(args[4], resolve_name=self._resolver)
        suffix = _render_token(args[5], resolve_name=self._resolver)

        proto = "{PROTOCOL_VERSION}"
        if len(args) >= 7:
            proto = _render_token(args[6], resolve_name=self._resolver)
        else:
            for kw in node.keywords or []:
                if kw.arg == "protocol_version" and kw.value is not None:
                    proto = _render_token(kw.value, resolve_name=self._resolver)
                    break

        subject = ".".join(["cg", proto, project_id, channel_id, category, component, target, suffix])
        self._add(
            subject=subject,
            transport="unknown",
            direction="literal",
            api="format_subject",
            line=getattr(node, "lineno", 1),
        )

    def _handle_nats_api(self, fn_name: str, node: ast.Call) -> None:
        # publish_event(subject, payload, ...)
        # publish_core(subject, payload)
        # subscribe_cmd(subject, queue_group, callback, durable_name=..., ...)
        # subscribe_core(subject, callback)
        if not node.args:
            return

        subject = _render_str(node.args[0], resolve_name=self._resolver)
        if subject is None:
            # Best-effort: if subject is format_subject(...) inline, render via token.
            subject = _render_token(node.args[0], resolve_name=self._resolver)
        if not isinstance(subject, str):
            return

        if not _looks_like_cg_subject(subject):
            return

        transport = "jetstream" if fn_name in {"publish_event", "subscribe_cmd", "subscribe_cmd_with_ack"} else "core"
        direction = "publish" if fn_name.startswith("publish") else "subscribe"

        queue = None
        durable = None
        if fn_name in {"subscribe_cmd", "subscribe_cmd_with_ack"}:
            if len(node.args) >= 2:
                queue = _render_str(node.args[1], resolve_name=self._resolver)
            for kw in node.keywords or []:
                if kw.arg == "durable_name":
                    durable = _render_str(kw.value, resolve_name=self._resolver)
                    break

        self._add(
            subject=subject,
            transport=transport,
            direction=direction,
            api=fn_name,
            line=getattr(node, "lineno", 1),
            queue=queue,
            durable=durable,
        )

    def visit_Constant(self, node: ast.Constant) -> Any:
        if isinstance(node.value, str) and _looks_like_cg_subject(node.value):
            self._add(
                subject=node.value,
                transport="unknown",
                direction="literal",
                api="literal",
                line=getattr(node, "lineno", 1),
            )


def _scan_file(path: Path) -> List[SubjectHit]:
    try:
        source = path.read_text(encoding="utf-8")
    except Exception:
        return []

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    rel = str(path.relative_to(REPO_ROOT))
    collector = _Collector(rel_path=rel)
    collector.visit(tree)
    return collector.hits


def _category_of(subject: str) -> str:
    # cg.{ver}.{project}.{channel}.{category}. ...
    parts = subject.split(".")
    if len(parts) >= 5 and parts[0] == "cg":
        return parts[4]
    return "unknown"


def _md_escape(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", "\\n")


def _render_table(rows: Sequence[SubjectHit]) -> str:
    lines = [
        "| Subject pattern | Transport | Direction | API | Queue | Durable | Sources |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]

    by_key: Dict[Tuple[str, str, str, str, Optional[str], Optional[str]], List[SubjectHit]] = {}
    for r in rows:
        key = (r.subject, r.transport, r.direction, r.api, r.queue, r.durable)
        by_key.setdefault(key, []).append(r)

    def _key_sort(k: Tuple[str, str, str, str, Optional[str], Optional[str]]) -> Tuple[str, str, str, str, str, str]:
        subj, transport, direction, api, queue, durable = k
        return (subj, transport, direction, api, queue or "", durable or "")

    for key in sorted(by_key.keys(), key=_key_sort):
        subj, transport, direction, api, queue, durable = key
        hits = sorted(by_key[key], key=lambda h: (h.file, h.line))
        sources = ", ".join([f"`{h.file}:{h.line}`" for h in hits])
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{_md_escape(subj)}`",
                    transport,
                    direction,
                    api,
                    f"`{_md_escape(queue)}`" if queue else "",
                    f"`{_md_escape(durable)}`" if durable else "",
                    sources,
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def main() -> int:
    hits: List[SubjectHit] = []
    for path in _iter_python_files():
        hits.extend(_scan_file(path))

    # Keep only cg.* subjects (defensive) and dedupe handled later in table grouping.
    hits = [h for h in hits if h.subject.strip().startswith("cg.")]

    # Group by transport/category for readability.
    jet_cmd = [h for h in hits if h.transport == "jetstream" and _category_of(h.subject) == "cmd"]
    jet_evt = [h for h in hits if h.transport == "jetstream" and _category_of(h.subject) == "evt"]
    core_str = [h for h in hits if h.transport == "core" and _category_of(h.subject) == "str"]
    other = [
        h
        for h in hits
        if h not in jet_cmd and h not in jet_evt and h not in core_str
    ]

    out = DEFAULT_OUT
    content_lines: List[str] = []
    content_lines.append("# Auto-generated NATS Subject Inventory")
    content_lines.append("")
    content_lines.append("> ⚠️ This file is auto-generated by the script; do not edit manually. For authoritative semantics/constraints, see: `docs/04_protocol_l0/nats_protocol.md`.")
    content_lines.append("")
    content_lines.append("Generation method:")
    content_lines.append("")
    content_lines.append("```bash")
    content_lines.append("python scripts/admin/generate_nats_subjects_doc.py")
    content_lines.append("```")
    content_lines.append("")
    content_lines.append(f"- Scanned directories: {', '.join('`'+d+'`' for d in SCAN_DIRS)}")
    content_lines.append(f"- Excluded directory names: {', '.join('`'+d+'`' for d in sorted(EXCLUDE_DIR_NAMES))}")
    content_lines.append("")
    content_lines.append("## JetStream / cmd（subscribe_cmd / publish_event）")
    content_lines.append(_render_table(jet_cmd) if jet_cmd else "_(not found)_")
    content_lines.append("")
    content_lines.append("## JetStream / evt（subscribe_cmd / publish_event）")
    content_lines.append(_render_table(jet_evt) if jet_evt else "_(not found)_")
    content_lines.append("")
    content_lines.append("## Core NATS / str（subscribe_core / publish_core）")
    content_lines.append(_render_table(core_str) if core_str else "_(not found)_")
    content_lines.append("")
    content_lines.append("## Other / Uncategorized（literal / format_subject / unknown）")
    content_lines.append(_render_table(other) if other else "_(not found)_")
    content_lines.append("")

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(content_lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
