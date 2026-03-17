from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, func, select
from sqlalchemy import create_engine as create_source_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from libs.config import get_settings
from libs.db import bootstrap_life_database, engine_for_url, rebuild_chunk_fts
from libs.db.bootstrap import bootstrap_report_taxonomy


TABLE_ORDER = [
    "task_spec",
    "run",
    "artifact",
    "entity",
    "report",
    "observation",
    "chunk",
    "citation_link",
    "interaction",
]


def _sqlite_database_path(database_url: str) -> Path | None:
    parsed = make_url(database_url)
    if parsed.get_backend_name() != "sqlite" or not parsed.database or parsed.database == ":memory:":
        return None
    return Path(parsed.database).expanduser().resolve()


def _prepare_target_path(target_url: str, *, overwrite: bool) -> None:
    path = _sqlite_database_path(target_url)
    if path is None:
        return
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"Target SQLite database already exists: {path}")
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)


def _bootstrap_target_database(target_url: str) -> None:
    settings = get_settings().model_copy(update={"life_database_url": target_url})
    bootstrap_life_database(settings)


def _normalize_value(value: Any) -> Any:
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, tuple):
        return list(value)
    return value


def _table_count(connection, table) -> int:
    return int(connection.execute(select(func.count()).select_from(table)).scalar_one())


def _sample_ids(connection, table, *, limit: int = 3) -> list[str]:
    if "id" not in table.c:
        return []
    return [str(item) for item in connection.execute(select(table.c.id).limit(limit)).scalars()]


def validate_migration_counts(summaries: dict[str, dict[str, Any]]) -> None:
    mismatches = {
        table_name: counts
        for table_name, counts in summaries.items()
        if counts["source_count"] != counts["target_count"]
    }
    if mismatches:
        raise RuntimeError(f"Row count validation failed: {mismatches}")


def migrate_life_data(source_engine: Engine, target_engine: Engine) -> dict[str, dict[str, Any]]:
    source_metadata = MetaData()
    source_metadata.reflect(bind=source_engine)

    target_metadata = MetaData()
    target_metadata.reflect(bind=target_engine)

    summaries: dict[str, dict[str, Any]] = {}

    with source_engine.connect() as source_connection, target_engine.connect() as target_connection:
        if target_engine.dialect.name == "sqlite":
            target_connection.exec_driver_sql("PRAGMA foreign_keys=OFF")
            target_connection.commit()

        transaction = target_connection.begin()
        try:
            for table_name in TABLE_ORDER:
                source_table = source_metadata.tables.get(table_name)
                target_table = target_metadata.tables.get(table_name)
                if source_table is None or target_table is None:
                    raise KeyError(f"Missing table during migration: {table_name}")

                target_columns = [column.name for column in target_table.columns]
                source_columns = set(source_table.columns.keys())
                common_columns = [column for column in target_columns if column in source_columns]

                rows = [
                    {column: _normalize_value(row[column]) for column in common_columns}
                    for row in source_connection.execute(
                        select(*(source_table.c[column] for column in common_columns))
                    ).mappings()
                ]
                if rows:
                    target_connection.execute(target_table.insert(), rows)

                summaries[table_name] = {
                    "source_count": _table_count(source_connection, source_table),
                    "target_count": _table_count(target_connection, target_table),
                    "sample_ids": _sample_ids(target_connection, target_table),
                }
            transaction.commit()
        except Exception:
            transaction.rollback()
            raise
        finally:
            if target_engine.dialect.name == "sqlite":
                target_connection.exec_driver_sql("PRAGMA foreign_keys=ON")
                target_connection.commit()

        if target_engine.dialect.name == "sqlite":
            fk_issues = list(target_connection.exec_driver_sql("PRAGMA foreign_key_check").mappings())
            if fk_issues:
                raise RuntimeError(f"Foreign key validation failed: {fk_issues}")

    rebuild_chunk_fts(target_engine)
    bootstrap_report_taxonomy(target_engine)
    validate_migration_counts(summaries)
    return summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate the life database from Postgres to SQLite.")
    parser.add_argument("--source-url", required=True, help="Legacy Postgres SQLAlchemy URL for the life database.")
    parser.add_argument("--target-url", required=True, help="Target SQLite SQLAlchemy URL for the life database.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite the target SQLite file if it exists.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source_backend = make_url(args.source_url).get_backend_name()
    target_backend = make_url(args.target_url).get_backend_name()

    if source_backend != "postgresql":
        raise ValueError(f"--source-url must be a PostgreSQL URL, got {source_backend}")
    if target_backend != "sqlite":
        raise ValueError(f"--target-url must be a SQLite URL, got {target_backend}")

    _prepare_target_path(args.target_url, overwrite=args.overwrite)
    _bootstrap_target_database(args.target_url)

    source_engine = create_source_engine(args.source_url, future=True)
    target_engine = engine_for_url(args.target_url)
    summary = migrate_life_data(source_engine, target_engine)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
