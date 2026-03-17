# Life System

Life System is a local Prefect-first automation stack for:

- daily brief generation
- paper review ingestion and summarization
- browser-assisted jobs
- draft writing and publication artifacts

## Stack overview

- `apps/api`: FastAPI control plane for commands, runs, reports, and artifacts
- `apps/tui`: operator UI that submits commands and inspects outputs
- `flows/*`: Prefect flows and deployment wiring
- `workers/*`: typed adapters for ingestion, analysis, drafting, browser, Codex, and publishing jobs
- `libs/*`: shared contracts, DB helpers, retrieval + prompts, and GitHub publish utilities
- `infra/*`: legacy Postgres bootstrap references and Prefect-related infrastructure notes
- `scripts/*`: local stack setup and runtime helpers

## Requirements

- Python 3.11+
- `bash`/`zsh` compatible shell

## Quick start

1. Create and activate a virtual environment, then install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

If you are migrating an existing Postgres `life` database into SQLite, install the
optional migration dependencies instead:

```bash
pip install -e ".[dev,migration]"
```

2. Copy environment defaults:

```bash
cp .env.example .env
```

3. Start the full local stack. This bootstraps the local `life` SQLite database,
   starts Prefect against its own SQLite metadata file, registers deployments,
   launches the default worker, and starts the API:

```bash
scripts/setup_local_stack.sh
```

4. Open the operator tools:

- API Swagger: `http://127.0.0.1:8000/docs`
- Prefect UI: value of `LIFE_PREFECT_UI_URL` in `.env` (default `http://127.0.0.1:4200`)
- TUI:

```bash
source .venv/bin/activate
life-tui
```

To shut everything down cleanly:

```bash
scripts/stop_local_stack.sh
```

Default database locations:

- `life`: `data/runtime/life.db`
- Prefect metadata: `data/runtime/prefect.db`

## Manual startup (optional)

The bootstrap script starts everything for convenience. For finer control:

```bash
source .venv/bin/activate
source .env
python -m scripts.bootstrap_life_system
scripts/run_prefect_server.sh
scripts/run_prefect_serve.sh
scripts/run_prefect_worker.sh mini-process process
scripts/run_api.sh
```

Useful script variants:

- `scripts/run_prefect_worker.sh mini-docker docker` for the browser lane
- `scripts/run_prefect_worker.sh mbp-process process` for ad hoc laptop workers

## API commands

All writes are accepted by `POST /commands/*`; run history and artifacts are in
`/runs`, `/reports`, and `/artifacts`.

Examples:

```bash
curl -X POST http://127.0.0.1:8000/commands/daily-brief \
  -H "Content-Type: application/json" \
  -d '{"include_news":true,"include_arxiv":true,"publish":false}'

curl -X POST http://127.0.0.1:8000/commands/paper-review \
  -H "Content-Type: application/json" \
  -d '{"paper_id":"2206.12345","source_url":"https://arxiv.org/abs/2206.12345"}'

curl -X POST http://127.0.0.1:8000/commands/browser-job \
  -H "Content-Type: application/json" \
  -d '{"job_name":"seed-tracker","target_url":"https://example.com","capture_html":true,"capture_screenshots":true}'

curl http://127.0.0.1:8000/runs?limit=25
curl http://127.0.0.1:8000/reports?limit=25
curl http://127.0.0.1:8000/artifacts?limit=25
```

Request schemas and richer docs are available in the interactive FastAPI docs at
`/docs` and `/redoc`.

## Repository pointers

- [Architecture notes](docs/architecture.md)
- [Implementation TODOs](docs/IMPLEMENTATION_TODO.md)
- [SQLite cutover runbook](docs/sqlite-cutover.md)
- [Flow registry](flows/registry.py)
- [TUI and bridges](apps/tui/main.py)

## Development checks

- Run tests:

```bash
python3 -m pytest
```

- Smoke compile check:

```bash
python3 -m compileall apps libs workers flows scripts
```

Common commands exposed by the package:

- `life-api` (starts `apps.api.main:run`)
- `life-tui` (starts `apps.tui.main:main`)
