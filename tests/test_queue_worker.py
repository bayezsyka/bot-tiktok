import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.database.models import DownloadItem
from app.database.repositories import JobRepository
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
        mock_provider = MagicMock()
        fake_meta = TikTokContentMetadata(
            content_type="video",
            title="Test Video",
            author="Creator",
            duration_seconds=15,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://src/1.mp4", media_type="video", local_path=dummy_file.name)],
        )

        async def fake_extract(job_obj, job_dir):
            job_obj.content_type = "video"
            job_obj.media_count = 1
            job_obj.duration_seconds = 15
            item = DownloadItem(
                job_id=job_obj.id,
                position=1,
                media_type="video",
                status="pending",
                source_url="http://src/1.mp4",
                local_filename=dummy_file.name,
                source_size_bytes=100,
                final_size_bytes=100,
            )
            job_obj.items.append(item)
            return mock_provider, fake_meta

        with patch("app.downloader.service.DownloaderService.extract_and_prepare_job", side_effect=fake_extract), \
             patch("app.downloader.service.DownloaderService.download_job_content", new_callable=AsyncMock), \
             patch("app.media.processor.MediaProcessor.process_job_media", new_callable=AsyncMock), \
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
            assert finished_job.status == "completed"
            assert finished_job.sent_count == finished_job.media_count
    finally:
        if os.path.exists(dummy_file.name):
            os.unlink(dummy_file.name)
