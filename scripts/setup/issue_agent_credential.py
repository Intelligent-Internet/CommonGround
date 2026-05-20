from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import stat
from typing import Any

from CommonGround.contracts import AgentRef
from CommonGround.infra import PostgresAgentCredentialStore


DEFAULT_PROVENANCE_KIND = "operator_bootstrap"


def issue_agent_credential(
    *,
    pg_dsn: str,
    project_id: str,
    agent_id: str,
    issued_by_agent_id: str | None = None,
    provenance_kind: str = DEFAULT_PROVENANCE_KIND,
    provenance_ref: str | None = None,
    provenance_payload_hash: str | None = None,
    expires_at: datetime | None = None,
    token_file: Path | None = None,
) -> dict[str, Any]:
    store = PostgresAgentCredentialStore(pg_dsn)
    issued = store.issue_agent_credential(
        AgentRef(project_id=project_id, agent_id=agent_id),
        issued_by_agent_id=issued_by_agent_id,
        provenance_kind=provenance_kind,
        provenance_ref=provenance_ref,
        provenance_payload_hash=provenance_payload_hash,
        expires_at=expires_at,
    )
    response: dict[str, Any] = {
        "project_id": project_id,
        "agent_id": agent_id,
        "credential_id": issued.ref.credential_id,
        "provenance_kind": provenance_kind,
        "provenance_ref": provenance_ref,
    }
    if token_file is None:
        response["token"] = issued.token
    else:
        _write_secret_file(token_file, issued.token)
        response["token_file"] = str(token_file)
    return response


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    result = issue_agent_credential(
        pg_dsn=args.pg_dsn or _require_pg_dsn(),
        project_id=args.project_id,
        agent_id=args.agent_id,
        issued_by_agent_id=args.issued_by_agent_id,
        provenance_kind=args.provenance_kind,
        provenance_ref=args.provenance_ref,
        provenance_payload_hash=args.provenance_payload_hash,
        expires_at=_parse_expires_at(args.expires_at),
        token_file=None if args.token_file is None else Path(args.token_file),
    )
    print(json.dumps(result, sort_keys=True))


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Issue a CommonGround Agent credential for an existing enabled Agent.")
    parser.add_argument("--pg-dsn", default=None, help="PostgreSQL DSN. Defaults to PG_DSN.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--issued-by-agent-id", default=None)
    parser.add_argument("--provenance-kind", default=DEFAULT_PROVENANCE_KIND)
    parser.add_argument("--provenance-ref", default=None)
    parser.add_argument("--provenance-payload-hash", default=None)
    parser.add_argument("--expires-at", default=None, help="Optional ISO datetime.")
    parser.add_argument("--token-file", default=None, help="Write plaintext token to this file instead of stdout JSON.")
    return parser.parse_args(argv)


def _require_pg_dsn() -> str:
    pg_dsn = os.environ.get("PG_DSN")
    if not pg_dsn:
        raise SystemExit("PG_DSN is required")
    return pg_dsn


def _parse_expires_at(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _write_secret_file(path: Path, token: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, stat.S_IRUSR | stat.S_IWUSR)
    try:
        os.fchmod(fd, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(token)
            handle.write("\n")
    except Exception:
        if fd >= 0:
            os.close(fd)
        raise


if __name__ == "__main__":
    main()
