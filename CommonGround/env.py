from __future__ import annotations

from pathlib import Path


def load_local_env() -> None:
    """Best-effort local .env loading for repo-root development entrypoints."""
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return

    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)

