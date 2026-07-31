import logging
import sqlite3
import time
from collections.abc import AsyncGenerator

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings

logger = logging.getLogger(__name__)

_SLOW_TXN_THRESHOLD_SECONDS = 2.0


def _set_sqlite_pragmas(dbapi_connection: sqlite3.Connection, connection_record: object) -> None:
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA busy_timeout=15000;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


_engine: AsyncEngine | None = None
_session_maker: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.database_url,
            echo=False,
            connect_args={"timeout": 15},
        )
        event.listen(_engine.sync_engine, "connect", _set_sqlite_pragmas)

        # Transaction duration observability
        @event.listens_for(_engine.sync_engine, "begin")
        def _on_begin(conn: object) -> None:
            # Store start time on the connection info dict
            if hasattr(conn, "info"):
                conn.info["_txn_start"] = time.monotonic()  # type: ignore[union-attr]

        @event.listens_for(_engine.sync_engine, "commit")
        def _on_commit(conn: object) -> None:
            _log_slow_txn(conn, "commit")

        @event.listens_for(_engine.sync_engine, "rollback")
        def _on_rollback(conn: object) -> None:
            _log_slow_txn(conn, "rollback")

    return _engine


def _log_slow_txn(conn: object, action: str) -> None:
    """Log a warning when a transaction exceeds the slow threshold."""
    if hasattr(conn, "info"):
        start = conn.info.pop("_txn_start", None)  # type: ignore[union-attr]
        if start is not None:
            elapsed = time.monotonic() - start
            if elapsed > _SLOW_TXN_THRESHOLD_SECONDS:
                logger.warning(
                    "Slow SQLite transaction: %.2fs before %s", elapsed, action
                )


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    global _session_maker
    if _session_maker is None:
        engine = get_engine()
        _session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    return _session_maker


def AsyncSessionLocal() -> AsyncSession:
    return get_session_maker()()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    session_maker = get_session_maker()
    async with session_maker() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
