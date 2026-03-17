#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

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
export PREFECT_PROFILE="${LIFE_PREFECT_PROFILE_NAME:-life-system}"
DEFAULT_PREFECT_DB_URL="sqlite+aiosqlite:///$ROOT/data/runtime/prefect.db"
export PREFECT_API_DATABASE_CONNECTION_URL="${PREFECT_API_DATABASE_CONNECTION_URL:-${LIFE_PREFECT_DATABASE_URL:-$DEFAULT_PREFECT_DB_URL}}"

python3 - <<'PY'
from pathlib import Path
from sqlalchemy.engine.url import make_url
import os

url = os.environ["PREFECT_API_DATABASE_CONNECTION_URL"]
parsed = make_url(url)
if parsed.get_backend_name() == "sqlite" and parsed.database and parsed.database != ":memory:":
    Path(parsed.database).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
PY

exec prefect server start --host "${LIFE_PREFECT_SERVER_HOST:-127.0.0.1}" --port "${LIFE_PREFECT_SERVER_PORT:-4200}"
