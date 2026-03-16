#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

export CG_API_HOST_PORT="${CG_API_HOST_PORT:-8099}"
export API_URL="${API_URL:-http://127.0.0.1:${CG_API_HOST_PORT}}"
export PG_DSN="${PG_DSN:-postgresql://postgres:postgres@localhost:5433/cardbox}"
export NATS_URL="${NATS_URL:-nats://localhost:4222}"
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-.venv_local}"
export CG_CONFIG_TOML="${CG_CONFIG_TOML:-$ROOT_DIR/config.toml}"
export PROJECT_ID="${PROJECT_ID:-proj_mvp_001}"

usage() {
  cat <<'EOF'
Usage:
  scripts/setup/cg_stack.sh up
  scripts/setup/cg_stack.sh env
  scripts/setup/cg_stack.sh run <command...>

Commands:
  up   Recreate compose stack with a unified local profile.
  env  Print effective environment variables.
  run  Execute a command with the unified environment.

Defaults:
  CG_API_HOST_PORT=8099
  API_URL=http://127.0.0.1:${CG_API_HOST_PORT}
  PG_DSN=postgresql://postgres:postgres@localhost:5433/cardbox
  NATS_URL=nats://localhost:4222
  UV_PROJECT_ENVIRONMENT=.venv_local
EOF
}

cmd="${1:-}"
case "$cmd" in
  up)
    docker compose up -d --build --force-recreate \
      jaeger otel-collector nats postgres db-init api pmo agent-worker ui-worker mock-search
    echo "[cg_stack] stack is up. health: ${API_URL}/health"
    curl -fsS "${API_URL}/health" >/dev/null && echo "[cg_stack] API healthy" || echo "[cg_stack] API not ready yet"
    ;;
  env)
    cat <<EOF
CG_API_HOST_PORT=${CG_API_HOST_PORT}
API_URL=${API_URL}
PG_DSN=${PG_DSN}
NATS_URL=${NATS_URL}
UV_PROJECT_ENVIRONMENT=${UV_PROJECT_ENVIRONMENT}
CG_CONFIG_TOML=${CG_CONFIG_TOML}
PROJECT_ID=${PROJECT_ID}
EOF
    ;;
  run)
    shift
    if [ $# -eq 0 ]; then
      echo "missing command"
      usage
      exit 2
    fi
    exec "$@"
    ;;
  *)
    usage
    exit 2
    ;;
esac
