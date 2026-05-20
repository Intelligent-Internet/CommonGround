#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <wheel-or-sdist>" >&2
  exit 2
fi

artifact="$1"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
artifact_url="$(python3 - <<'PY' "$artifact"
from pathlib import Path
import sys

print(Path(sys.argv[1]).resolve().as_uri())
PY
)"

python3 -m venv "$workdir/venv"
"$workdir/venv/bin/pip" install "commonground-kernel[server] @ ${artifact_url}" >/dev/null

version_output="$("$workdir/venv/bin/cg" --version)"
if [[ "$version_output" == "cg 0+unknown" ]]; then
  echo "Installed CLI reported fallback version: $version_output" >&2
  exit 1
fi

"$workdir/venv/bin/cg" --help >/dev/null
"$workdir/venv/bin/cg" setup project seed -h >/dev/null
"$workdir/venv/bin/python" - <<'PY'
from Integrations.admin_service.project_setup import _cardbox_schema_sql

sql = _cardbox_schema_sql()
if "create table" not in sql.lower():
    raise SystemExit("cardbox schema resource did not load correctly")
PY
echo "Smoke test passed for $artifact ($version_output)"
