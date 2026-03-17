#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUNTIME_DIR="${LIFE_RUNTIME_ROOT:-$(
python3 - <<'PY'
from libs.config import get_settings
print(get_settings().runtime_root)
PY
)}"
COMPOSE_FILE="$ROOT/infra/compose/compose.yaml"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

API_PORT="${LIFE_API_PORT:-8000}"
PREFECT_SERVER_PORT="${LIFE_PREFECT_SERVER_PORT:-4200}"

log() {
  printf '%s\n' "$*"
}

warn() {
  printf 'WARN: %s\n' "$*" >&2
}

pid_is_running() {
  local pid="${1:-}"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

command_for_pid() {
  local pid="$1"
  ps -axo pid=,command= | awk -v target="$pid" '
    $1 == target {
      $1 = ""
      sub(/^ +/, "", $0)
      print
      exit
    }
  '
}

matches_expected_command() {
  local command="$1"
  local service="$2"

  case "$service" in
    api)
      [[ "$command" == *"automann-api"* || "$command" == *"uvicorn"* || "$command" == *"apps.api.main:app"* ]]
      ;;
    prefect-server)
      [[ "$command" == *"prefect server start"* ]]
      ;;
    prefect-serve)
      [[ "$command" == *"python -m flows.serve"* || "$command" == *"python3 -m flows.serve"* ]]
      ;;
    worker)
      [[ "$command" == *"prefect worker start"* ]]
      ;;
    *)
      return 1
      ;;
  esac
}

wait_for_exit() {
  local pid="$1"
  local attempts="${2:-10}"
  local delay="${3:-1}"
  local i

  for ((i = 0; i < attempts; i += 1)); do
    if ! pid_is_running "$pid"; then
      return 0
    fi
    sleep "$delay"
  done

  return 1
}

stop_pid() {
  local pid="$1"
  local label="$2"

  if ! pid_is_running "$pid"; then
    return 0
  fi

  log "Stopping $label (pid $pid)..."
  kill "$pid" 2>/dev/null || true

  if wait_for_exit "$pid" 10 1; then
    return 0
  fi

  warn "$label did not exit after SIGTERM; sending SIGKILL to pid $pid."
  kill -KILL "$pid" 2>/dev/null || true

  if ! wait_for_exit "$pid" 5 1; then
    warn "$label is still running after SIGKILL."
    return 1
  fi

  return 0
}

service_name_for_pid_file() {
  local pid_file="$1"
  local name

  name="$(basename "$pid_file")"
  case "$name" in
    api.pid)
      printf 'api\n'
      ;;
    prefect-server.pid)
      printf 'prefect-server\n'
      ;;
    prefect-serve.pid)
      printf 'prefect-serve\n'
      ;;
    *-worker.pid)
      printf 'worker\n'
      ;;
    *)
      printf 'unknown\n'
      ;;
  esac
}

stop_pid_file() {
  local pid_file="$1"
  local service pid command

  [[ -f "$pid_file" ]] || return 0

  service="$(service_name_for_pid_file "$pid_file")"
  pid="$(tr -d '[:space:]' <"$pid_file")"

  if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
    warn "Removing invalid PID file $(basename "$pid_file")."
    rm -f "$pid_file"
    return 0
  fi

  if ! pid_is_running "$pid"; then
    log "Removing stale PID file $(basename "$pid_file")."
    rm -f "$pid_file"
    return 0
  fi

  if [[ "$service" == "unknown" ]]; then
    warn "Skipping unknown PID file $(basename "$pid_file")."
    return 0
  fi

  command="$(command_for_pid "$pid")"
  if [[ -z "$command" ]]; then
    warn "Unable to inspect pid $pid from $(basename "$pid_file"); skipping termination."
    return 1
  fi

  if ! matches_expected_command "$command" "$service"; then
    warn "PID file $(basename "$pid_file") points to an unexpected command: $command"
    rm -f "$pid_file"
    return 0
  fi

  if stop_pid "$pid" "$(basename "$pid_file" .pid)"; then
    rm -f "$pid_file"
    return 0
  fi

  return 1
}

stop_service_on_port() {
  local port="$1"
  local service="$2"
  local label="$3"
  local pid command
  local status=0

  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    command="$(command_for_pid "$pid")"
    if [[ -z "$command" ]]; then
      continue
    fi
    if matches_expected_command "$command" "$service"; then
      if ! stop_pid "$pid" "$label"; then
        status=1
      fi
    fi
  done < <(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)

  return "$status"
}

stop_legacy_compose() {
  if [[ ! -f "$COMPOSE_FILE" ]] || ! command -v docker >/dev/null 2>&1; then
    return 0
  fi

  local running_ids
  running_ids="$(docker compose -f "$COMPOSE_FILE" ps -q 2>/dev/null || true)"
  if [[ -z "$running_ids" ]]; then
    return 0
  fi

  log "Stopping legacy Docker services from infra/compose/compose.yaml..."
  if ! docker compose -f "$COMPOSE_FILE" down >/dev/null 2>&1; then
    warn "Docker compose shutdown failed for $COMPOSE_FILE."
    return 1
  fi

  return 0
}

shopt -s nullglob

status=0

if ! stop_pid_file "$RUNTIME_DIR/api.pid"; then
  status=1
fi

if ! stop_pid_file "$RUNTIME_DIR/prefect-serve.pid"; then
  status=1
fi

worker_pid_files=("$RUNTIME_DIR"/*-worker.pid)
for pid_file in "${worker_pid_files[@]}"; do
  if ! stop_pid_file "$pid_file"; then
    status=1
  fi
done

if ! stop_pid_file "$RUNTIME_DIR/prefect-server.pid"; then
  status=1
fi

if ! stop_service_on_port "$API_PORT" api "api listener on port $API_PORT"; then
  status=1
fi

if ! stop_service_on_port "$PREFECT_SERVER_PORT" prefect-server "prefect server listener on port $PREFECT_SERVER_PORT"; then
  status=1
fi

if ! stop_legacy_compose; then
  status=1
fi

if [[ "$status" -eq 0 ]]; then
  log "Shutdown complete."
else
  warn "Shutdown completed with warnings."
fi

exit "$status"
