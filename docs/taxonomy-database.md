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

### Report revision contract

Report persistence now supports logical report series:

- `report_series_id`: stable lineage key for all revisions in the same report
- `revision_number`: monotonic revision number within the series
- `supersedes_report_id`: previous revision id when a revision replaces or augments an older one
- `is_current`: marks the revision surfaced by default from `/reports`

`/reports/{id}/revisions` returns the full history for a report series, while `/reports` remains current-revision oriented.

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
  - `research_report` => `report`, tag `deep_research`
  - `search_report` => `report`, tag `search_report`
  - `substack_draft` => `report`, tag `deep_research`
- Keyword heuristics for investment/semiconductor/comparative signals
- Fallback: always include `report` when nothing else matches

Unknown taxonomy keys are intentionally ignored to avoid blocking ingestion on unsupported labels.

### Canonical taxonomy dictionary

Use this as the definitive map for inferable taxonomy terms. These are the only keys that are persisted in `report_taxonomy_link`.

| Kind | Key | Label | Parent | Meaning |
|---|---|---|---|---|
| filter | `report` | Report | (root) | Long-form research and analysis outputs. |
| filter | `daily` | Daily | (root) | Recurring synthesis / daily pipeline output. |
| filter | `investment` | Investment | (root) | Output inferred as investment- or markets-oriented. |
| filter | `semiconductor` | Semiconductor | (root) | Chip/foundry/GPU/semiconductor-industry output. |
| tag | `deep_research` | Deep Research | `report` | Long-form or synthesis-heavy research outputs. |
| tag | `arxiv_paper_analysis` | arXiv Paper Analysis | `report` | Paper-level reviews and analyses. |
| tag | `search_report` | Search Report | `report` | Search-driven report outputs. |
| tag | `comparative_search` | Comparative Search | `report` | Comparative/benchmark-style research. |
| tag | `synthesis` | Synthesis | `daily` | Daily synthesis output. |
| tag | `scrapes` | Scrapes | `daily` | Scraped input included in daily output. |

### Report-type labels vs taxonomy keys

- `report_type` values are primary report labels coming from workers/flows.
- Canonical taxonomy keys are derived in `classify_report_taxonomy(...)` during persistence.
- Only canonical keys from `REPORT_TAXONOMY_TERMS` survive normalization in DB links.
- `paper_batch` and `search_report` are workflow `report_type` values:
  - `paper_batch` flows produce a parent summary over multiple paper-review runs.
  - `search_report` flows produce either Codex-session-backed manual reports or automation-driven search digests.
- `paper_batch` is intentionally not canonical in taxonomy; it remains a report label + report_type.
- `search_report` is now canonical as tag `search_report`.
- Both report types still persist with `filter_keys` including `report` by default.

### DB placement

- Canonical terms are seeded into `report_taxonomy_term` by [bootstrap](/Users/dhalgren/github/automann/libs/db/bootstrap.py) from `REPORT_TAXONOMY_TERMS`.
- Persisted assignments land in `report_taxonomy_link` via [repository sync](/Users/dhalgren/github/automann/libs/db/repository.py).
- `/reports/taxonomy` is the authoritative catalog endpoint and returns the active term dictionary grouped by `filters`/`tags`.

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
3. Build a typed worker output model first when a worker contract exists, then store `structured_outputs=output.model_dump(mode="json")`.
4. For reports, use stable `metadata` shape:
   - include any required display fields in `metadata` (for example `presentation_mode`, `source_kind`),
   - keep `daily_brief` metadata aligned with `brief_date`, `news_count`, `paper_count`, `browser_status`, and `previous_report_title`,
   - keep `research_report` metadata aligned with `report_key`, `edit_mode`, lineage/revision context, and any artifact manifest details,
   - keep `search_report` metadata aligned with Codex session context (`codex_session_id`, `resume_source`), lineage/revision context, and follow-up hints such as `suggested_followup_prompt`,
   - keep `substack_draft` metadata aligned with `theme` plus any source-report identifiers used to assemble the draft,
   - include `metadata.taxonomy.filters` / `metadata.taxonomy.tags` only with known keys.
5. Keep `report_type` values aligned with taxonomy expectations (`paper_review`, `daily_brief`, `research_report`, `search_report`, `substack_draft`, etc.).
