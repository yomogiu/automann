# Taxonomy and Database Contracts

This document defines how worker outputs are mapped into the local SQLite DB and how report taxonomy is derived for frontend consumption.

## 1) Output folders are standardized

All worker file writes must be rooted under `settings.artifact_root` so outputs stay off the repo root.

### Run directory contract

- Path pattern: `artifact_root / <worker_key> / <timestamp>-<short-id>/`
- Created via `workers.common.ensure_worker_dir(settings, worker_key)`
- `timestamp_slug()` format changed to include a short UUID suffix to avoid collisions in rapid runs.

### Structured output path contract

`workers.codex_runner.CodexCliRunner` resolves structured output paths through:

- `resolve_worker_output_path(run_dir, output_path)`
  - absolute paths are preserved,
  - relative paths are written under the current run directory.
- Codex runs now execute in the run directory by default (`cwd=run_dir` unless caller passes `cwd`), so artifact roots stay consistent.

## 2) DB object model for worker outputs

Worker outputs are persisted through `LifeRepository.persist_adapter_result(...)` into:

- `run`: execution envelope (`status`, `input_payload`, `structured_outputs`, etc.)
- `artifact`: files written by workers (`path`, `storage_uri`, `sha256`, `media_type`)
- `report`: one or more report objects (`report_type`, `title`, `summary`, `content_markdown`, `metadata`)
- `report_taxonomy_link`: derived filter/tag assignments

### Report artifact linkage

- `ReportRecord.artifact_path` is matched against stored artifact paths.
- Matching now tolerates path-shape variance by looking up both raw and resolved (`Path(...).resolve()`) keys.
- This keeps `source_artifact_id` associations stable even when absolute/relative serialization differs.

## 3) Taxonomy source of truth

`libs.report_taxonomy.classify_report_taxonomy(...)` is the canonical function used during persist time.

Inputs:

- `report_type`
- `title`
- `summary`
- `metadata`

Derivation:

- Explicit metadata classification:
  - `metadata.taxonomy.filters` and `metadata.taxonomy.tags` (validated to known keys)
- Type-based defaults:
  - `daily_brief` => `daily`, tags `synthesis`, `scrapes`
  - `paper_review` => `report`, tag `arxiv_paper_analysis`
  - `substack_draft` => `report`, tag `deep_research`
- Keyword heuristics for investment/semiconductor/comparative signals
- Fallback: always include `report` when nothing else matches

Unknown taxonomy keys are intentionally ignored to avoid blocking ingestion on unsupported labels.

### Canonical taxonomy keys

- Filter keys: `report`, `daily`, `investment`, `semiconductor`
- Tag keys: `deep_research`, `arxiv_paper_analysis`, `comparative_search`, `synthesis`, `scrapes`

## 4) Frontend contract

API `/reports` consumes DB-stored taxonomy links and exposes:

- `filter_keys`
- `tag_keys`
- `metadata` (worker-provided fields such as `presentation_mode`, artifact ids, etc.)
- `source_artifact_id` for download/view behavior

The web UI uses these keys for filter pills and preview behavior.

## 5) Worker checklist for compatibility

When creating `AdapterResult`:

1. Build artifacts through `build_file_artifact(...)` when possible so `path`, `storage_uri`, `sha256` are normalized.
2. Put output files under `ensure_worker_dir(...)` outputs.
3. For reports, use stable `metadata` shape:
   - include any required display fields in `metadata` (for example `presentation_mode`, `source_kind`),
   - include `metadata.taxonomy.filters` / `metadata.taxonomy.tags` only with known keys.
4. Keep `report_type` values aligned with taxonomy expectations (`paper_review`, `daily_brief`, `substack_draft`, etc.).
