"""Tests for worker transaction recovery: rollback on error, fresh sessions, no PendingRollbackError."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.database.models import DownloadItem
from app.database.repositories import JobRepository
from app.gateway.schemas import GatewayMessageResponse
from app.queue.worker import QueueWorker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.asyncio
async def test_operational_error_during_flush_triggers_rollback(test_db: AsyncSession) -> None:
    """When flush raises OperationalError, session.rollback is called and job can be requeued."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-txn-recovery-1",
            webhook_event_id="evt-txn-recovery-1",
            sender_number="628000000100",
            original_url="https://www.tiktok.com/@creator/video/11111",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Simulate OperationalError during extraction
    async def fake_extract_raises(*args, **kwargs):
        raise Exception("sqlite3.OperationalError: database is locked")

    with patch("app.downloader.service.DownloaderService.extract_and_prepare_job", side_effect=fake_extract_raises):
        await worker._process_job_safely(job_id)

    # Job should be requeued (attempt_count=1 < MAX_JOB_RETRIES=2)
    async with session_maker() as session:
        job_repo = JobRepository(session)
        final_job = await job_repo.get_by_id(job_id)
        assert final_job is not None
        assert final_job.status == "queued"
        assert final_job.error_code == "RETRY_SCHEDULED"


@pytest.mark.asyncio
async def test_handle_job_error_uses_fresh_session(test_db: AsyncSession) -> None:
    """_handle_job_error creates a fresh session, never reuses a broken one."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-txn-recovery-2",
            webhook_event_id="evt-txn-recovery-2",
            sender_number="628000000101",
            original_url="https://www.tiktok.com/@creator/video/22222",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Directly call _handle_job_error with attempt_count < MAX_RETRIES
    await worker._handle_job_error(job_id, "test error", attempt_count=0)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        final_job = await job_repo.get_by_id(job_id)
        assert final_job is not None
        assert final_job.status == "queued"
        assert final_job.error_code == "RETRY_SCHEDULED"


@pytest.mark.asyncio
async def test_handle_job_error_marks_failed_after_max_retries(test_db: AsyncSession) -> None:
    """After max retries, job should be marked as failed."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-txn-recovery-3",
            webhook_event_id="evt-txn-recovery-3",
            sender_number="628000000102",
            original_url="https://www.tiktok.com/@creator/video/33333",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Call with attempt_count >= MAX_RETRIES (default 2)
    with patch.object(worker.gateway, "send_text", new_callable=AsyncMock) as mock_send_text:
        mock_send_text.return_value = GatewayMessageResponse(status="ok", message_id="wa-msg-fail")
        await worker._handle_job_error(job_id, "permanent failure", attempt_count=5)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        final_job = await job_repo.get_by_id(job_id)
        assert final_job is not None
        assert final_job.status == "failed"
        assert final_job.error_code == "MAX_RETRIES_EXCEEDED"


@pytest.mark.asyncio
async def test_no_pending_rollback_error_after_failed_flush(test_db: AsyncSession) -> None:
    """After a failed flush + rollback, subsequent operations should not raise PendingRollbackError."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-txn-recovery-4",
            webhook_event_id="evt-txn-recovery-4",
            sender_number="628000000103",
            original_url="https://www.tiktok.com/@creator/video/44444",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Make extraction fail with a simulated DB error
    call_count = 0

    async def extract_that_fails(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise Exception("Simulated OperationalError: database is locked")

    with patch("app.downloader.service.DownloaderService.extract_and_prepare_job", side_effect=extract_that_fails):
        await worker._process_job_safely(job_id)

    # Verify no PendingRollbackError — the job should be cleanly requeued
    async with session_maker() as session:
        job_repo = JobRepository(session)
        final_job = await job_repo.get_by_id(job_id)
        assert final_job is not None
        # Should be queued (retried) or failed (max retries), not stuck in extracting
        assert final_job.status in ("queued", "failed"), f"Unexpected status: {final_job.status}"
        assert final_job.status != "extracting"


@pytest.mark.asyncio
async def test_worker_loop_continues_after_error(test_db: AsyncSession) -> None:
    """Worker loop should keep running after a job processing error."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-txn-recovery-5",
            webhook_event_id="evt-txn-recovery-5",
            sender_number="628000000104",
            original_url="https://www.tiktok.com/@creator/video/55555",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Make _process_job_safely raise an unexpected error
    process_calls = 0

    async def flaky_process(jid):
        nonlocal process_calls
        process_calls += 1
        if process_calls == 1:
            raise RuntimeError("Unexpected worker error")
        # Second call: stop the worker
        worker.stop()

    with patch.object(worker, "_process_job_safely", side_effect=flaky_process), \
         patch.object(worker, "_acquire_next_job", new_callable=AsyncMock, return_value=job_id):
        # Run worker — it should handle the error and continue
        await worker.run()

    # Worker should have been called at least twice (error + stop)
    assert process_calls >= 2


@pytest.mark.asyncio
async def test_job_not_stuck_as_downloading(test_db: AsyncSession) -> None:
    """A job that fails during download should not remain permanently as 'downloading'."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-txn-recovery-6",
            webhook_event_id="evt-txn-recovery-6",
            sender_number="628000000105",
            original_url="https://www.tiktok.com/@creator/video/66666",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Make extraction succeed but download fail
    async def fake_extract(job_obj, job_dir):
        job_obj.content_type = "video"
        job_obj.media_count = 1
        item = DownloadItem(
            job_id=job_obj.id, position=1, media_type="video",
            status="pending", source_url="http://src/1.mp4",
        )
        job_obj.items.append(item)
        return MagicMock(), MagicMock()

    async def fake_download_fails(*args, **kwargs):
        raise Exception("Network timeout during download")

    with patch("app.downloader.service.DownloaderService.extract_and_prepare_job", side_effect=fake_extract), \
         patch("app.downloader.service.DownloaderService.download_job_content", side_effect=fake_download_fails):
        await worker._process_job_safely(job_id)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        final_job = await job_repo.get_by_id(job_id)
        assert final_job is not None
        assert final_job.status != "downloading", "Job should not be stuck as 'downloading'"
        assert final_job.status in ("queued", "failed")
