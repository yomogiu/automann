from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from libs.config import Settings
from libs.report_taxonomy import REPORT_TAXONOMY_TERMS

from .models import Base, ReportTaxonomyTerm
from .repository import LifeRepository
from .session import engine_for_url


SQLITE_SUPPORTING_DDL = (
    "CREATE INDEX IF NOT EXISTS ix_chunk_artifact_ordinal ON chunk (artifact_id, ordinal)",
    "CREATE VIRTUAL TABLE IF NOT EXISTS chunk_fts USING fts5(text, content='chunk', content_rowid='rowid')",
    """
    CREATE TRIGGER IF NOT EXISTS chunk_fts_ai
    AFTER INSERT ON chunk
    BEGIN
        INSERT INTO chunk_fts(rowid, text) VALUES (new.rowid, new.text);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS chunk_fts_ad
    AFTER DELETE ON chunk
    BEGIN
        INSERT INTO chunk_fts(chunk_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS chunk_fts_au
    AFTER UPDATE OF text ON chunk
    BEGIN
        INSERT INTO chunk_fts(chunk_fts, rowid, text) VALUES ('delete', old.rowid, old.text);
        INSERT INTO chunk_fts(rowid, text) VALUES (new.rowid, new.text);
    END
    """,
)

SQLITE_REPORT_REVISION_DDL = (
    "ALTER TABLE report ADD COLUMN report_series_id VARCHAR(255)",
    "ALTER TABLE report ADD COLUMN revision_number INTEGER DEFAULT 1 NOT NULL",
    "ALTER TABLE report ADD COLUMN supersedes_report_id VARCHAR(36)",
    "ALTER TABLE report ADD COLUMN is_current BOOLEAN DEFAULT 1 NOT NULL",
    "CREATE INDEX IF NOT EXISTS ix_report_series_revision ON report (report_series_id, revision_number)",
    "CREATE INDEX IF NOT EXISTS ix_report_series_current ON report (report_series_id, is_current)",
)

SQLITE_ARTIFACT_SOURCE_DOCUMENT_DDL = (
    "ALTER TABLE artifact ADD COLUMN source_document_id VARCHAR(36)",
    "CREATE INDEX IF NOT EXISTS ix_artifact_source_document_id ON artifact (source_document_id)",
)

SQLITE_TASK_SPEC_AUTOMATION_DDL = (
    "ALTER TABLE task_spec ADD COLUMN flow_name VARCHAR(255)",
    "ALTER TABLE task_spec ADD COLUMN timezone VARCHAR(100)",
    "ALTER TABLE task_spec ADD COLUMN work_pool VARCHAR(255)",
    "ALTER TABLE task_spec ADD COLUMN prompt_path VARCHAR(1024)",
    "ALTER TABLE task_spec ADD COLUMN prefect_deployment_id VARCHAR(36)",
    "ALTER TABLE task_spec ADD COLUMN prefect_deployment_name VARCHAR(255)",
    "ALTER TABLE task_spec ADD COLUMN prefect_deployment_path VARCHAR(255)",
    "ALTER TABLE task_spec ADD COLUMN prefect_deployment_url VARCHAR(2048)",
)

SQLITE_RUN_CODEX_SESSION_DDL = (
    "ALTER TABLE run ADD COLUMN codex_thread_id VARCHAR(255)",
    "ALTER TABLE run ADD COLUMN codex_active_turn_id VARCHAR(255)",
    "ALTER TABLE run ADD COLUMN codex_session_key VARCHAR(255)",
    "ALTER TABLE run ADD COLUMN codex_state VARCHAR(64)",
    "ALTER TABLE run ADD COLUMN codex_pending_request_id VARCHAR(255)",
    "ALTER TABLE run ADD COLUMN codex_last_event_at DATETIME",
)


def _is_sqlite_url(database_url: str) -> bool:
    return make_url(database_url).get_backend_name() == "sqlite"


def _ensure_sqlite_parent(database_url: str) -> None:
    if not _is_sqlite_url(database_url):
        return
    database = make_url(database_url).database
    if not database or database == ":memory:":
        return
    Path(database).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def rebuild_chunk_fts(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as connection:
        connection.exec_driver_sql("INSERT INTO chunk_fts(chunk_fts) VALUES ('rebuild')")


def _bootstrap_sqlite_support(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.begin() as connection:
        for statement in SQLITE_SUPPORTING_DDL:
            connection.exec_driver_sql(statement)
    rebuild_chunk_fts(engine)


def _bootstrap_report_revisions(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        LifeRepository(engine).backfill_report_revisions()
        return

    with engine.begin() as connection:
        existing_columns = {
            row["name"]
            for row in connection.exec_driver_sql("PRAGMA table_info('report')").mappings()
        }
        for statement in SQLITE_REPORT_REVISION_DDL:
            if statement.startswith("ALTER TABLE report ADD COLUMN report_series_id") and "report_series_id" in existing_columns:
                continue
            if statement.startswith("ALTER TABLE report ADD COLUMN revision_number") and "revision_number" in existing_columns:
                continue
            if statement.startswith("ALTER TABLE report ADD COLUMN supersedes_report_id") and "supersedes_report_id" in existing_columns:
                continue
            if statement.startswith("ALTER TABLE report ADD COLUMN is_current") and "is_current" in existing_columns:
                continue
            connection.exec_driver_sql(statement)
    LifeRepository(engine).backfill_report_revisions()


def _bootstrap_artifact_source_documents(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        existing_columns = {
            row["name"]
            for row in connection.exec_driver_sql("PRAGMA table_info('artifact')").mappings()
        }
        for statement in SQLITE_ARTIFACT_SOURCE_DOCUMENT_DDL:
            if statement.startswith("ALTER TABLE artifact ADD COLUMN source_document_id") and "source_document_id" in existing_columns:
                continue
            connection.exec_driver_sql(statement)


def _bootstrap_task_spec_automation_fields(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return

    with engine.begin() as connection:
        existing_columns = {
            row["name"]
            for row in connection.exec_driver_sql("PRAGMA table_info('task_spec')").mappings()
        }
        for statement in SQLITE_TASK_SPEC_AUTOMATION_DDL:
            column_name = statement.split(" ADD COLUMN ", maxsplit=1)[1].split(" ", maxsplit=1)[0]
            if column_name in existing_columns:
                continue
            connection.exec_driver_sql(statement)


def _bootstrap_run_codex_session_fields(engine: Engine) -> None:
    if engine.dialect.name == "sqlite":
        with engine.begin() as connection:
            existing_columns = {
                row["name"]
                for row in connection.exec_driver_sql("PRAGMA table_info('run')").mappings()
            }
            for statement in SQLITE_RUN_CODEX_SESSION_DDL:
                column_name = statement.split(" ADD COLUMN ", maxsplit=1)[1].split(" ", maxsplit=1)[0]
                if column_name in existing_columns:
                    continue
                connection.exec_driver_sql(statement)
        return

    with engine.begin() as connection:
        existing_columns = {column["name"] for column in inspect(connection).get_columns("run")}
        for statement in SQLITE_RUN_CODEX_SESSION_DDL:
            column_name = statement.split(" ADD COLUMN ", maxsplit=1)[1].split(" ", maxsplit=1)[0]
            if column_name in existing_columns:
                continue
            connection.exec_driver_sql(statement)


def bootstrap_report_taxonomy(engine: Engine) -> None:
    with engine.begin() as connection:
        existing_keys = set(connection.execute(select(ReportTaxonomyTerm.key)).scalars())
        for term in REPORT_TAXONOMY_TERMS:
            payload = {
                "label": term.label,
                "kind": term.kind,
                "parent_key": term.parent_key,
                "description": term.description,
                "sort_order": term.sort_order,
                "active": True,
            }
            if term.key in existing_keys:
                connection.execute(
                    ReportTaxonomyTerm.__table__.update()
                    .where(ReportTaxonomyTerm.key == term.key)
                    .values(**payload)
                )
            else:
                connection.execute(
                    ReportTaxonomyTerm.__table__.insert().values(key=term.key, **payload)
                )
    LifeRepository(engine).backfill_report_taxonomy()


def bootstrap_life_database(settings: Settings) -> None:
    _ensure_sqlite_parent(settings.life_database_url)
    engine = engine_for_url(settings.life_database_url)
    Base.metadata.create_all(engine)
    _bootstrap_sqlite_support(engine)
    _bootstrap_task_spec_automation_fields(engine)
    _bootstrap_run_codex_session_fields(engine)
    _bootstrap_artifact_source_documents(engine)
    _bootstrap_report_revisions(engine)
    bootstrap_report_taxonomy(engine)


def bootstrap_databases(settings: Settings) -> None:
    bootstrap_life_database(settings)
