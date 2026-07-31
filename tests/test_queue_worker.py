import os
import tempfile
from unittest.mock import AsyncMock, patch

import pytest
from app.database.repositories import JobRepository
from app.downloader.dtos import (
    ProcessedItemResult,
    ProcessedJobResult,
)
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.gateway.schemas import GatewayMessageResponse
from app.queue.worker import QueueWorker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.asyncio
async def test_queue_worker_single_job_lifecycle(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    # Insert a queued job
    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-worker-01",
            webhook_event_id="evt-worker-01",
            sender_number="628123456789",
            original_url="https://www.tiktok.com/@creator/video/1111111111",
            canonical_url="https://www.tiktok.com/@creator/video/1111111111",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Create dummy physical file
    dummy_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    dummy_file.write(b"dummy video bytes")
    dummy_file.close()

    try:
        fake_meta = TikTokContentMetadata(
            content_type="video",
            title="Test Video",
            author="Creator",
            duration_seconds=15,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://src/1.mp4", media_type="video")],
        )
        async def fake_process(items, job_dir):
            return ProcessedJobResult(
                items=tuple(
                    ProcessedItemResult(
                        item_id=item.id,
                        status="pending",
                        local_filename=dummy_file.name,
                        final_size_bytes=100,
                    )
                    for item in items
                ),
                final_size_bytes=100,
            )

        with patch("app.downloader.service.YtDlpProvider.extract_metadata", new_callable=AsyncMock, return_value=fake_meta), \
             patch("app.downloader.service.YtDlpProvider.download_content", new_callable=AsyncMock, return_value=fake_meta.model_copy(update={"items": [fake_meta.items[0].model_copy(update={"local_path": dummy_file.name})]})), \
             patch("app.media.processor.MediaProcessor.process_job_media", side_effect=fake_process), \
             patch.object(worker.gateway, "send_media", new_callable=AsyncMock) as mock_send, \
             patch.object(worker.gateway, "send_text", new_callable=AsyncMock) as mock_send_text:

            mock_send.return_value = GatewayMessageResponse(status="ok", message_id="wa-msg-123")
            mock_send_text.return_value = GatewayMessageResponse(status="ok", message_id="wa-msg-fail")

            # Run process_job_safely once directly
            await worker._process_job_safely(job_id)

        # Verify job state after worker
        async with session_maker() as session:
            job_repo = JobRepository(session)
            finished_job = await job_repo.get_by_id(job_id)
            assert finished_job is not None
            assert finished_job.status == "gateway_queued"
            assert finished_job.sent_count == finished_job.media_count
    finally:
        if os.path.exists(dummy_file.name):
            os.unlink(dummy_file.name)

@pytest.mark.asyncio
async def test_queue_worker_handles_202_with_message_id(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-worker-202a",
            webhook_event_id="evt-worker-202a",
            sender_number="628123456789",
            original_url="https://www.tiktok.com/@creator/video/111",
            canonical_url="https://www.tiktok.com/@creator/video/111",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)
    dummy_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    dummy_file.write(b"dummy")
    dummy_file.close()

    try:
        fake_meta = TikTokContentMetadata(
            content_type="video",
            title="Test",
            author="Creator",
            duration_seconds=10,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://src/1.mp4", media_type="video")],
        )

        async def fake_process(items, job_dir):
            return ProcessedJobResult(
                items=tuple(
                    ProcessedItemResult(item_id=item.id, status="pending", local_filename=dummy_file.name, final_size_bytes=100)
                    for item in items
                ),
                final_size_bytes=100,
            )

        with patch("app.downloader.service.YtDlpProvider.extract_metadata", new_callable=AsyncMock, return_value=fake_meta), \
             patch("app.downloader.service.YtDlpProvider.download_content", new_callable=AsyncMock, return_value=fake_meta.model_copy(update={"items": [fake_meta.items[0].model_copy(update={"local_path": dummy_file.name})]})), \
             patch("app.media.processor.MediaProcessor.process_job_media", side_effect=fake_process), \
             patch.object(worker.gateway, "send_media", new_callable=AsyncMock) as mock_send:

            # Return 202 with message_id
            mock_send.return_value = GatewayMessageResponse(
                status="ok",
                message_id="msg-id-202",
                queue_status="queued",
                http_status=202
            )

            await worker._process_job_safely(job_id)

        async with session_maker() as session:
            job_repo = JobRepository(session)
            finished_job = await job_repo.get_by_id(job_id)
            assert finished_job is not None
            assert finished_job.status == "gateway_queued"
            assert finished_job.items[0].gateway_message_id == "msg-id-202"
    finally:
        if os.path.exists(dummy_file.name):
            os.unlink(dummy_file.name)

@pytest.mark.asyncio
async def test_queue_worker_handles_202_without_message_id_failure(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-worker-202b",
            webhook_event_id="evt-worker-202b",
            sender_number="628123456789",
            original_url="https://www.tiktok.com/@creator/video/222",
            canonical_url="https://www.tiktok.com/@creator/video/222",
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)
    dummy_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    dummy_file.write(b"dummy")
    dummy_file.close()

    try:
        fake_meta = TikTokContentMetadata(
            content_type="video",
            title="Test",
            author="Creator",
            duration_seconds=10,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://src/1.mp4", media_type="video")],
        )

        async def fake_process(items, job_dir):
            return ProcessedJobResult(
                items=tuple(
                    ProcessedItemResult(item_id=item.id, status="pending", local_filename=dummy_file.name, final_size_bytes=100)
                    for item in items
                ),
                final_size_bytes=100,
            )

        with patch("app.downloader.service.YtDlpProvider.extract_metadata", new_callable=AsyncMock, return_value=fake_meta), \
             patch("app.downloader.service.YtDlpProvider.download_content", new_callable=AsyncMock, return_value=fake_meta.model_copy(update={"items": [fake_meta.items[0].model_copy(update={"local_path": dummy_file.name})]})), \
             patch("app.media.processor.MediaProcessor.process_job_media", side_effect=fake_process), \
             patch.object(worker.gateway, "send_media", new_callable=AsyncMock) as mock_send, \
             patch.object(worker.gateway, "send_text", new_callable=AsyncMock) as mock_send_text:

            # Return 202 with no message_id
            mock_send.return_value = GatewayMessageResponse(
                status="ok",
                message_id=None,
                queue_status="queued",
                http_status=202
            )
            mock_send_text.return_value = GatewayMessageResponse(status="ok", message_id="wa-fail-notify")

            await worker._process_job_safely(job_id)

        async with session_maker() as session:
            job_repo = JobRepository(session)
            finished_job = await job_repo.get_by_id(job_id)
            assert finished_job is not None
            assert finished_job.status == "failed"
            assert finished_job.items[0].status == "failed"
            assert finished_job.items[0].gateway_error_code == "GATEWAY_INVALID_RESPONSE"
            assert finished_job.items[0].gateway_message_id is None
    finally:
        if os.path.exists(dummy_file.name):
            os.unlink(dummy_file.name)
