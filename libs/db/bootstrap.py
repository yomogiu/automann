from __future__ import annotations

from pathlib import Path

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
    bootstrap_report_taxonomy(engine)


def bootstrap_databases(settings: Settings) -> None:
    bootstrap_life_database(settings)
