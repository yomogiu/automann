from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session


def engine_for_url(database_url: str, *, echo: bool = False) -> Engine:
    url = make_url(database_url)
    connect_args: dict[str, object] = {}
    pool_pre_ping = True

    if url.get_backend_name() == "sqlite":
        connect_args = {
            "check_same_thread": False,
            "timeout": 30,
        }
        pool_pre_ping = False

    engine = create_engine(
        database_url,
        future=True,
        pool_pre_ping=pool_pre_ping,
        echo=echo,
        connect_args=connect_args,
    )

    if url.get_backend_name() == "sqlite":

        @event.listens_for(engine, "connect")
        def _configure_sqlite(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
            except Exception:
                pass
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

    return engine


@contextmanager
def session_scope(engine: Engine) -> Iterator[Session]:
    session = Session(engine, expire_on_commit=False)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
