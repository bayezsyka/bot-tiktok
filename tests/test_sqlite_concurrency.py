"""Tests for SQLite concurrency: worker and reconciler can coexist without database locks."""
import asyncio
import os
import tempfile

import pytest
from app.database.models import Base, DownloadItem, DownloadJob, utc_now
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


def _make_engine_and_session(db_path: str):
    """Create a fresh engine+session_maker for a temporary SQLite database."""
    url = f"sqlite+aiosqlite:///{db_path}"
    engine = create_async_engine(url, echo=False, connect_args={"timeout": 15})

    @event.listens_for(engine.sync_engine, "connect")
    def _pragmas(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA busy_timeout=15000;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA foreign_keys=ON;")
        cur.close()

    sm = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    return engine, sm


@pytest.fixture
async def real_sqlite():
    """Provide a real temporary SQLite database (not shared with conftest)."""
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    engine, sm = _make_engine_and_session(path)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine, sm
    await engine.dispose()
    if os.path.exists(path):
        os.unlink(path)


@pytest.mark.asyncio
async def test_short_sessions_do_not_block_each_other(real_sqlite):
    """Two coroutines using short sessions should not cause 'database is locked'."""
    engine, sm = real_sqlite

    # Seed a job and item
    async with sm() as s:
        job = DownloadJob(
            id="conc-1", status="gateway_queued", sender_number="628000000001",
            inbound_message_id="inb-conc-1", webhook_event_id="wh-conc-1",
            original_url="http://example.com",
        )
        s.add(job)
        item = DownloadItem(
            job_id="conc-1", position=1, media_type="video", status="gateway_queued",
            gateway_message_id="msg-conc-1",
        )
        s.add(item)
        await s.commit()
        item_id = item.id

    errors: list[Exception] = []

    async def writer_a():
        """Simulates reconciler: read → sleep (network) → write."""
        try:
            # Read phase
            async with sm() as s:
                result = await s.execute(select(DownloadItem.id).where(DownloadItem.id == item_id))
                _ = result.scalar_one()
            # Simulate network delay (no session open)
            await asyncio.sleep(0.1)
            # Write phase
            async with sm() as s:
                result = await s.execute(select(DownloadItem).where(DownloadItem.id == item_id))
                it = result.scalar_one()
                it.gateway_delivery_status = "delivered"
                it.status = "completed"
                await s.commit()
        except Exception as e:
            errors.append(e)

    async def writer_b():
        """Simulates worker: independent short write."""
        try:
            await asyncio.sleep(0.05)  # Start slightly later
            async with sm() as s:
                job_result = await s.execute(select(DownloadJob).where(DownloadJob.id == "conc-1"))
                j = job_result.scalar_one()
                j.status = "sending"
                j.updated_at = utc_now()
                await s.commit()
        except Exception as e:
            errors.append(e)

    await asyncio.gather(writer_a(), writer_b())

    assert errors == [], f"Unexpected errors during concurrent writes: {errors}"

    # Verify both writes landed
    async with sm() as s:
        it = (await s.execute(select(DownloadItem).where(DownloadItem.id == item_id))).scalar_one()
        assert it.status == "completed"
        assert it.gateway_delivery_status == "delivered"

        j = (await s.execute(select(DownloadJob).where(DownloadJob.id == "conc-1"))).scalar_one()
        assert j.status == "sending"


@pytest.mark.asyncio
async def test_operational_error_triggers_rollback(real_sqlite):
    """When a write fails, rollback should be called and session should remain usable via a new one."""
    engine, sm = real_sqlite

    async with sm() as s:
        job = DownloadJob(
            id="conc-2", status="queued", sender_number="628000000002",
            inbound_message_id="inb-conc-2", webhook_event_id="wh-conc-2",
            original_url="http://example.com",
        )
        s.add(job)
        await s.commit()

    # Simulate a constraint violation (duplicate job ID)
    rollback_called = False
    async with sm() as s:
        try:
            dup_job = DownloadJob(
                id="conc-2", status="queued", sender_number="628000000003",
                inbound_message_id="inb-conc-2x", webhook_event_id="wh-conc-2x",
                original_url="http://example2.com",
            )
            s.add(dup_job)
            await s.flush()
        except Exception:
            await s.rollback()
            rollback_called = True

    assert rollback_called, "Rollback should have been called after constraint violation"

    # A new session should work fine
    async with sm() as s:
        result = await s.execute(select(DownloadJob).where(DownloadJob.id == "conc-2"))
        j = result.scalar_one()
        assert j.sender_number == "628000000002"


@pytest.mark.asyncio
async def test_concurrent_writers_with_wal(real_sqlite):
    """Multiple concurrent short writes should all succeed with WAL mode."""
    engine, sm = real_sqlite

    # Seed jobs
    async with sm() as s:
        for i in range(5):
            s.add(DownloadJob(
                id=f"wal-{i}", status="queued", sender_number=f"62800000{i:04d}",
                inbound_message_id=f"inb-wal-{i}", webhook_event_id=f"wh-wal-{i}",
                original_url="http://example.com",
            ))
        await s.commit()

    errors: list[Exception] = []

    async def update_job(job_id: str):
        try:
            async with sm() as s:
                result = await s.execute(select(DownloadJob).where(DownloadJob.id == job_id))
                j = result.scalar_one()
                j.status = "completed"
                j.updated_at = utc_now()
                await s.commit()
        except Exception as e:
            errors.append(e)

    # Run 5 concurrent updates
    await asyncio.gather(*(update_job(f"wal-{i}") for i in range(5)))

    assert errors == [], f"Concurrent WAL writes failed: {errors}"

    # Verify all updated
    async with sm() as s:
        for i in range(5):
            result = await s.execute(select(DownloadJob).where(DownloadJob.id == f"wal-{i}"))
            j = result.scalar_one()
            assert j.status == "completed"


@pytest.mark.asyncio
async def test_wal_mode_is_enabled(real_sqlite):
    """Verify WAL mode is actually set on our test database."""
    engine, sm = real_sqlite
    async with sm() as s:
        result = await s.execute(text("PRAGMA journal_mode;"))
        mode = result.scalar()
        assert mode == "wal", f"Expected WAL mode, got {mode}"


@pytest.mark.asyncio
async def test_failed_session_not_reused(real_sqlite):
    """After a session fails and is rolled back, a new session should work fine."""
    engine, sm = real_sqlite

    async with sm() as s:
        s.add(DownloadJob(
            id="noreuse-1", status="queued", sender_number="628000000010",
            inbound_message_id="inb-noreuse", webhook_event_id="wh-noreuse",
            original_url="http://example.com",
        ))
        await s.commit()

    # Session 1: fail
    async with sm() as s:
        try:
            s.add(DownloadJob(
                id="noreuse-1", status="queued", sender_number="628000000011",
                inbound_message_id="inb-noreuse-dup", webhook_event_id="wh-noreuse-dup",
                original_url="http://example.com",
            ))
            await s.flush()
            pytest.fail("Should have raised an error")
        except Exception:
            await s.rollback()

    # Session 2: should work perfectly
    async with sm() as s:
        result = await s.execute(select(DownloadJob).where(DownloadJob.id == "noreuse-1"))
        j = result.scalar_one()
        j.status = "completed"
        await s.commit()

    async with sm() as s:
        result = await s.execute(select(DownloadJob).where(DownloadJob.id == "noreuse-1"))
        j = result.scalar_one()
        assert j.status == "completed"
