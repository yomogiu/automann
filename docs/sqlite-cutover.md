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

## Validation

After startup:

1. Confirm Prefect UI is available.
2. Run a smoke `daily-brief` and `paper-review`.
3. Verify `/commands/query-knowledge` returns chunk hits from migrated content.
4. Compare SQLite row counts with the pre-cutover Postgres snapshot.
5. Check `data/runtime/*.log` for startup or worker errors.

## Rollback

If validation fails:

1. Stop the SQLite-backed stack.
2. Keep `life.db` for inspection.
3. Point your environment back at the legacy Postgres setup.
4. Restart the old stack using the previous runtime flow.

Rollback does not require migrating Prefect metadata because the SQLite Prefect store is disposable.
