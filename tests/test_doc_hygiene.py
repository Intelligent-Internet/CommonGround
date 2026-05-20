from __future__ import annotations

import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

PUBLIC_SURFACE_ROOTS = (
    ".github",
    "README.md",
    "README_CN.md",
    "AGENTS.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
    "docs",
    "examples",
    ".env.example",
    "test.env.example",
    "pyproject.toml",
)
ACTIVE_READER_DOC_ROOTS = (
    "README.md",
    "README_CN.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
    "docs/index.md",
    "docs/en",
    "docs/zh",
)
CODE_SURFACE_ROOTS = ("scripts", "tests")
RELEASE_HYGIENE_ROOTS = (
    ".github",
    "AGENTS.md",
    "CONTRIBUTING.md",
    "CommonGround",
    "Integrations",
    "scripts",
    "tests",
    "docs",
    "examples",
    "README.md",
    "README_CN.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
    ".env.example",
    "test.env.example",
    "pyproject.toml",
)

ZH_DOC_PARTS = {("docs", "zh")}
BILINGUAL_DOCS = {"README_CN.md", "docs/index.md"}
HAN_RE = re.compile(r"[\u4e00-\u9fff]")
LOCAL_PATH_RE = re.compile(r"/home/[A-Za-z0-9._-]+")
PATH_TO_PLACEHOLDER_RE = re.compile(r"/path/to/")
PRIVATE_DSN_RE = re.compile("postgresql://postgres:" + r"(?:my_" + "password" + r"|sec" + "ret" + r")@")
INTERNAL_CUTOVER_RE = re.compile(
    r"phase2|Phase 2|#575|#579|hard cut|legacy route-layer|cg_registration_credential"
    r"|SkillClaw|opencode_c|pi_c|gemini-3-flash-preview"
)
TOKEN_RE = re.compile(
    r"(?:"
    r"cga" + r"c_[A-Za-z0-9._-]+"
    r"|xo" + r"x[baprs]-"
    r"|xa" + r"pp-"
    r"|g" + r"h[pousr]_[A-Za-z0-9]+"
    r"|s" + r"k-[A-Za-z0-9]{16,}"
    r"|BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE " + "KEY"
    r")"
)
TOKEN_FILE_READER_RE = re.compile(r"cat\s+/tmp/[^`'\")\s]+token")
MARKDOWN_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
ARCHIVE_REFERENCE_RE = re.compile(r"(?:^|[/(])(?:docs/)?archive/")
OLD_ZH_FOUNDATION_DOC_NAMES = (
    "01_\u5baa\u6cd5.md",
    "02_\u4e09\u5e73\u9762\u6a21\u578b.md",
    "03_\u8bbe\u8ba1\u5ba1\u67e5\u539f\u5219.md",
    "04_Turn\u5de5\u4f5c\u77e5\u8bc6\u7559\u5b58\u4e0e\u6d88\u8d39\u613f\u666f.md",
)
OLD_FOUNDATION_DOC_REFERENCE_RE = re.compile(
    r"(?<!01-)constitution\.md"
    r"|(?<!02-)three-plane-model\.md"
    r"|(?<!03-)design-review-principles\.md"
    r"|work-memory-vision\.md"
    + "|"
    + "|".join(re.escape(name) for name in OLD_ZH_FOUNDATION_DOC_NAMES)
)


def _tracked_files(*roots: str) -> list[Path]:
    completed = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--", *roots],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return sorted(
        path
        for line in completed.stdout.split(b"\0")
        if line
        for path in [ROOT / line.decode("utf-8")]
        if path.exists()
    )


def _is_zh_doc(path: Path) -> bool:
    rel = path.relative_to(ROOT).parts
    return any(rel[: len(parts)] == parts for parts in ZH_DOC_PARTS)


def _allows_han(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return rel in BILINGUAL_DOCS or _is_zh_doc(path)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return ""


def test_non_chinese_public_surface_is_english_only() -> None:
    failures = []
    for path in _tracked_files(*PUBLIC_SURFACE_ROOTS):
        if _allows_han(path):
            continue
        rel = path.relative_to(ROOT).as_posix()
        if HAN_RE.search(rel) or HAN_RE.search(_read_text(path)):
            failures.append(rel)
    assert failures == []


def test_bilingual_docs_have_aligned_paths() -> None:
    en_paths = {
        path.relative_to(ROOT / "docs" / "en").as_posix()
        for path in _tracked_files("docs/en")
        if path.suffix == ".md"
    }
    zh_paths = {
        path.relative_to(ROOT / "docs" / "zh").as_posix()
        for path in _tracked_files("docs/zh")
        if path.suffix == ".md"
    }
    assert sorted(en_paths - zh_paths) == []
    assert sorted(zh_paths - en_paths) == []


def test_code_and_tests_do_not_use_chinese_fixtures() -> None:
    failures = []
    for path in _tracked_files(*CODE_SURFACE_ROOTS):
        rel = path.relative_to(ROOT).as_posix()
        if HAN_RE.search(rel) or HAN_RE.search(_read_text(path)):
            failures.append(rel)
    assert failures == []


def test_public_surface_has_no_local_paths_or_private_examples() -> None:
    failures = []
    for path in _tracked_files(*PUBLIC_SURFACE_ROOTS):
        rel = path.relative_to(ROOT).as_posix()
        body = _read_text(path)
        if (
            LOCAL_PATH_RE.search(body)
            or PATH_TO_PLACEHOLDER_RE.search(body)
            or PRIVATE_DSN_RE.search(body)
            or TOKEN_RE.search(body)
            or TOKEN_FILE_READER_RE.search(body)
        ):
            failures.append(rel)
    assert failures == []


def test_active_markdown_local_links_resolve() -> None:
    failures = []
    markdown_files = [
        path
        for path in _tracked_files(*PUBLIC_SURFACE_ROOTS)
        if path.suffix == ".md"
    ]
    for path in markdown_files:
        body = _read_text(path)
        for match in MARKDOWN_LINK_RE.finditer(body):
            raw_target = match.group(1).strip()
            if (
                not raw_target
                or raw_target.startswith("#")
                or raw_target.startswith(("http://", "https://", "mailto:"))
            ):
                continue
            target = raw_target.split("#", 1)[0]
            if not target:
                continue
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                failures.append(f"{path.relative_to(ROOT).as_posix()} -> {raw_target}")
    assert failures == []


def test_foundation_doc_references_use_numbered_paths() -> None:
    failures = []
    for path in _tracked_files(*PUBLIC_SURFACE_ROOTS):
        if path.suffix != ".md":
            continue
        body = _read_text(path)
        rel = path.relative_to(ROOT).as_posix()
        if OLD_FOUNDATION_DOC_REFERENCE_RE.search(body):
            failures.append(rel)
    assert failures == []


def test_active_reader_docs_do_not_reference_archive() -> None:
    failures = []
    for path in _tracked_files(*ACTIVE_READER_DOC_ROOTS):
        if path.suffix != ".md":
            continue
        body = _read_text(path)
        rel = path.relative_to(ROOT).as_posix()
        if ARCHIVE_REFERENCE_RE.search(body):
            failures.append(rel)
    assert failures == []


def test_release_surface_has_no_internal_cutover_leftovers() -> None:
    failures = []
    for path in _tracked_files(*RELEASE_HYGIENE_ROOTS):
        rel = path.relative_to(ROOT).as_posix()
        if rel == "tests/test_doc_hygiene.py":
            continue
        if INTERNAL_CUTOVER_RE.search(_read_text(path)):
            failures.append(rel)
    assert failures == []
