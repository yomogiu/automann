# Architecture

## Control Plane

- FastAPI is the front door.
- Clients issue commands like `daily-brief`, `paper-review`, `browser-job`, and `draft-article`.
- The API submits Prefect deployments when available and falls back to local execution for bootstrap simplicity.

## Orchestration

- Prefect owns schedules, work pools, deployment registration, and custom events.
- Workers do not call each other directly.
- Shared persistence happens through local SQLite files and the artifact store under `data/`.

## Data Layer

- `data/runtime/prefect.db` stores Prefect orchestration state.
- `data/runtime/life.db` stores domain entities, runs, artifacts, chunks, observations, reports, and citation links.
- SQLite FTS5 backs lexical chunk recall through a `chunk_fts` sidecar index.
- Embeddings are persisted as JSON for compatibility, but semantic ranking is intentionally deferred during the SQLite cutover.

## Operator UX

- `apps/tui` is a thin operator console for runs, reports, artifacts, and manual triggers.
- `../TODO-agentboard` remains available for a richer taskboard workflow through an explicit bridge command.

## Execution Lanes

- `mini-process`: default Python subprocess work on the always-on host.
- `mini-docker`: isolated jobs for containerized collectors and future Playwright work.
- `mbp-process`: optional laptop worker for ad hoc reruns.
