#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

RUNTIME_DIR="data/runtime"

for pid_file in \
  "$RUNTIME_DIR/api.pid" \
  "$RUNTIME_DIR/mini-process-worker.pid" \
  "$RUNTIME_DIR/prefect-serve.pid" \
  "$RUNTIME_DIR/prefect-server.pid"
do
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
    fi
    rm -f "$pid_file"
  fi
done
