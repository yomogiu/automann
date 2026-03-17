# Architecture

## Control Plane

- FastAPI is the front door.
- Clients issue commands like `daily-brief`, `paper-review`, `browser-job`, `research-report`, `search-report`, and `draft-article`.
- The API submits Prefect deployments when available and falls back to local execution for bootstrap simplicity.
- `browser_job_flow` is an exception: it requires Prefect deployment execution on the dedicated browser pool and fails closed if unavailable.
- Public request envelopes are flow-oriented; worker-specific request contracts are internal and are built inside flows before adapter execution.

## Orchestration

- Prefect owns schedules, work pools, deployment registration, and custom events.
- Flows coordinate work across adapters. Some workers may call shared internal utility runners such as the Codex CLI runner, but public orchestration still happens at the flow layer.
- `research-report` is the stateful deep-research lane. It loads the current report revision by `report_key`, retrieves local chunk context, runs the Codex-backed research worker as a child run, materializes Markdown/JSON/CSV artifacts, and persists a new revision.
- `codex_search_report_flow` is the manual operator search lane. It builds a strict JSON Codex prompt, runs the dedicated `CodexSearchSessionRunner` in fresh or resume mode, persists Markdown/memo/manifest/events artifacts, and auto-promotes a new `search_report` revision.
- `search_report_flow` still exists as the automation-oriented search lane. It is useful for planner-driven digests, but it is still a degraded path relative to the manual Codex session wrapper and does not yet provide shared ingestion or robust browser-worker handoff.
- `draft-article` is now publication-oriented. It prefers a stored research report revision (`source_report_id` or `source_revision_id`) and uses the Codex-backed draft worker to transform research material into publication copy.
- Human checkpointing is wired for the research lane through a single `promote_revision` checkpoint that pauses the parent run before a newly generated revision becomes current.
- Shared persistence happens through local SQLite files and the artifact store under `data/`.

## Data Layer

- `data/runtime/prefect.db` stores Prefect orchestration state.
- `data/runtime/life.db` stores domain entities, runs, artifacts, chunks, observations, reports, and citation links.
- Report rows now support revision history through `report_series_id`, `revision_number`, `supersedes_report_id`, and `is_current`.
- SQLite FTS5 backs lexical chunk recall through a `chunk_fts` sidecar index.
- Embeddings are persisted as JSON for compatibility, but semantic ranking is intentionally deferred during the SQLite cutover.
- Report taxonomy and artifact-link behavior follows [`taxonomy-database.md`](./taxonomy-database.md).

## Operator UX

- `apps/tui` is a thin operator console for runs, reports, artifacts, and manual triggers.
- `../TODO-agentboard` remains available for a richer taskboard workflow through an explicit bridge command.

## Execution Lanes

- `mini-process`: default Python subprocess work on the always-on host.
- `browser-process`: dedicated host process worker for Playwright/CDP browser automation.
- `mbp-process`: optional laptop worker for ad hoc reruns.
