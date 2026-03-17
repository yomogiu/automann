# SQLite Cutover Runbook

## Summary

This repository now runs locally on two SQLite databases:

- `data/runtime/life.db` for application state
- `data/runtime/prefect.db` for Prefect metadata

The `life` database can be migrated from the legacy Postgres schema with
`scripts/migrate_life_postgres_to_sqlite.py`. Prefect metadata is not migrated.

## Before Cutover

1. Install migration dependencies:

   ```bash
   source .venv/bin/activate
   pip install -e ".[migration]"
   ```

2. Snapshot source row counts from the Postgres `life` database.
3. Stop the API, workers, and Prefect:

   ```bash
   scripts/stop_local_stack.sh
   ```

4. Keep the old Postgres database available in read-only mode until the SQLite stack is verified.

## Migration Command

Run the one-time `life` migration with an explicit source URL and a SQLite target URL:

```bash
python -m scripts.migrate_life_postgres_to_sqlite \
  --source-url "postgresql+psycopg://life:life@127.0.0.1:5432/life" \
  --target-url "sqlite+pysqlite:////absolute/path/to/automann/data/runtime/life.db" \
  --overwrite
```

The script:

- bootstraps the target SQLite schema
- copies the `life` tables in foreign-key-safe order
- preserves IDs, timestamps, JSON payloads, and historical `prefect_flow_run_id` values
- runs `PRAGMA foreign_key_check`
- rebuilds the `chunk_fts` index
- emits per-table source and target counts plus sample IDs

## Prefect Reset

Prefect metadata is intentionally recreated on SQLite. Historical
`prefect_flow_run_id` values stored in `life.run` remain informational only after cutover.

Start the new local stack after the `life` migration completes:

```bash
scripts/setup_local_stack.sh
```

This recreates Prefect work pools and deployments against `data/runtime/prefect.db`.

## Standardized Output Folders (Cutover-Time Check)

After cutover, worker artifacts should no longer be written to repository root.

Runbook checks:

1. Confirm `worker_settings.artifact_root` points to `data/runtime/artifacts`.
2. Ensure smoke-run outputs are under run directories matching:
   - `data/runtime/artifacts/<worker_key>/<timestamp>-<short-id>/...`
3. Confirm no new run artifacts are created at repository root during worker execution.

This aligns with the taxonomy/report persistence contract used by the frontend:
- reports should link to source artifacts via `source_artifact_id`,
- taxonomy labels should be visible as `filter_keys`/`tag_keys` in `/reports`.

## Validation

After startup:

1. Confirm Prefect UI is available.
2. Run a smoke `daily-brief` and `paper-review`.
3. Verify `/commands/query-knowledge` returns chunk hits from migrated content.
4. Verify report payloads:
   - `/reports` returns expected `filter_keys` and `tag_keys`.
   - reports include non-null `source_artifact_id` when a source file was produced.
5. Compare SQLite row counts with the pre-cutover Postgres snapshot.
6. Review the worker artifact directories for the expected `run_key`/`timestamp` folder naming.
7. Check `data/runtime/*.log` for startup or worker errors.

## Rollback

If validation fails:

1. Stop the SQLite-backed stack.
2. Keep `life.db` for inspection.
3. Point your environment back at the legacy Postgres setup.
4. Restart the old stack using the previous runtime flow.

Rollback does not require migrating Prefect metadata because the SQLite Prefect store is disposable.
