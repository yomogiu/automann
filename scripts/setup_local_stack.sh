#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  cp .env.example .env
fi

set -a
source .env
set +a

RUNTIME_DIR="${LIFE_RUNTIME_ROOT:-$(python3 - <<'PY'
from libs.config import get_settings
print(get_settings().runtime_root)
PY
)}"
mkdir -p "$RUNTIME_DIR"

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"

python -m scripts.bootstrap_life_system

if [[ ! -f "$RUNTIME_DIR/prefect-server.pid" ]] || ! kill -0 "$(cat "$RUNTIME_DIR/prefect-server.pid")" 2>/dev/null; then
  nohup scripts/run_prefect_server.sh >"$RUNTIME_DIR/prefect-server.log" 2>&1 &
  echo $! >"$RUNTIME_DIR/prefect-server.pid"
fi

until curl -fsS "${LIFE_PREFECT_UI_URL:-http://127.0.0.1:4200}/api/health" >/dev/null 2>&1; do
  sleep 2
done

prefect work-pool inspect mini-process >/dev/null 2>&1 || prefect work-pool create mini-process --type process
prefect work-pool inspect browser-process >/dev/null 2>&1 || prefect work-pool create browser-process --type process
prefect work-pool inspect mbp-process >/dev/null 2>&1 || prefect work-pool create mbp-process --type process

if [[ ! -f "$RUNTIME_DIR/prefect-serve.pid" ]] || ! kill -0 "$(cat "$RUNTIME_DIR/prefect-serve.pid")" 2>/dev/null; then
  nohup scripts/run_prefect_serve.sh >"$RUNTIME_DIR/prefect-serve.log" 2>&1 &
  echo $! >"$RUNTIME_DIR/prefect-serve.pid"
fi

if [[ ! -f "$RUNTIME_DIR/mini-process-worker.pid" ]] || ! kill -0 "$(cat "$RUNTIME_DIR/mini-process-worker.pid")" 2>/dev/null; then
  nohup scripts/run_prefect_worker.sh mini-process process >"$RUNTIME_DIR/mini-process-worker.log" 2>&1 &
  echo $! >"$RUNTIME_DIR/mini-process-worker.pid"
fi

if [[ ! -f "$RUNTIME_DIR/api.pid" ]] || ! kill -0 "$(cat "$RUNTIME_DIR/api.pid")" 2>/dev/null; then
  nohup scripts/run_api.sh >"$RUNTIME_DIR/api.log" 2>&1 &
  echo $! >"$RUNTIME_DIR/api.pid"
fi

cat <<EOF
Auto Mann bootstrap complete.

Services:
  - Prefect UI: ${LIFE_PREFECT_UI_URL:-http://127.0.0.1:4200}
  - API: http://${LIFE_API_HOST:-127.0.0.1}:${LIFE_API_PORT:-8000}
  - TUI: source .venv/bin/activate && automann-tui
  - Life DB: ${LIFE_DATABASE_URL:-$(python3 - <<'PY'
from libs.config import get_settings
print(get_settings().life_database_url)
PY
)}
  - Prefect DB: ${LIFE_PREFECT_DATABASE_URL:-$(python3 - <<'PY'
from libs.config import get_settings
print(get_settings().prefect_database_url)
PY
)}

Logs:
  - $RUNTIME_DIR/prefect-server.log
  - $RUNTIME_DIR/prefect-serve.log
  - $RUNTIME_DIR/mini-process-worker.log
  - $RUNTIME_DIR/api.log
EOF
