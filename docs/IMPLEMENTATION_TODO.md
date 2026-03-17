# Implementation TODO

This file is updated as major pieces of the system land.

## Completed

- [x] Establish repo structure for apps, workers, libs, flows, infra, docs, and scripts.
- [x] Shared contracts, configuration, and project metadata.
- [x] SQLite-first `life` bootstrap with SQLAlchemy-managed schema and FTS5 chunk recall.
- [x] FastAPI control API with router-per-capability layout.
- [x] Worker adapter layer for Codex, ingest, analysis, drafting, publishing, and browser jobs.
- [x] Prefect flows for daily brief, paper review, browser jobs, and article drafting.
- [x] Manual `search-report` command backed by Codex CLI sessions with session resume, memo fallback, and revisioned report persistence.
- [x] Browser runtime wiring: `browser-process` pool target, fail-closed browser flow submission, and browser/CDP environment settings.
- [x] Thin operator TUI with `../TODO-agentboard` bridge hooks.
- [x] Local setup/bootstrap script for SQLite, Prefect, work pools, schema install, and service startup.
- [x] Smoke tests and compile-time verification.
- [x] One-time `life` migration tooling for Postgres-to-SQLite cutover.

## Deferred By Design

- [ ] Deep Playwright browser automation for authenticated/profile-bound jobs.
- [ ] Prefect event automation objects beyond emitted custom events and deployment scaffolding.
- [ ] Reintroduce semantic/vector chunk ranking on top of SQLite if local retrieval quality requires it.

## Verification

- [x] `python3 -m compileall apps libs workers flows scripts tests`
- [x] `python3 -m unittest discover -s tests`
