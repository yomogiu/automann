#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

POOL="${1:-mini-process}"
TYPE="${2:-process}"

if [[ ! -d .venv ]]; then
  echo "Missing .venv. Run scripts/setup_local_stack.sh first." >&2
  exit 1
fi

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

source .venv/bin/activate
export PREFECT_API_URL="${LIFE_PREFECT_API_URL:-http://127.0.0.1:4200/api}"

exec prefect worker start --pool "$POOL" --type "$TYPE"
