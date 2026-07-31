import os
import tempfile
from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem
from app.database.repositories import JobRepository
from app.downloader.dtos import (
    DownloadedContentResult,
    DownloadedItemResult,
    ExtractedMetadataResult,
    ProcessedItemResult,
    ProcessedJobResult,
)
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.service import DownloaderService
from app.gateway.schemas import GatewayMessageResponse
from app.queue.worker import QueueWorker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.asyncio
async def test_extract_new_item_is_attached_to_job_relationship(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-reg-a",
            webhook_event_id="evt-reg-a",
            sender_number="628111222333",
            original_url="https://www.tiktok.com/@creator/video/11111",
            canonical_url="https://www.tiktok.com/@creator/video/11111",
        )
        await session.commit()

        # Job items should be initially empty
        assert len(job.items) == 0

        dummy_meta = TikTokContentMetadata(
            content_type="video",
            title="Test Video",
            author="Creator",
            duration_seconds=15,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://src/video.mp4", media_type="video")],
        )

        worker = QueueWorker(session_maker)
        await worker._save_extracted_metadata(
            job.id,
            ExtractedMetadataResult(
                canonical_url=job.canonical_url or job.original_url,
                provider=DownloaderService().yt_dlp,
                metadata=dummy_meta,
            ),
        )

        reloaded_job = await job_repo.get_by_id(job.id)
        assert reloaded_job is not None
        assert len(reloaded_job.items) == 1
        assert reloaded_job.items[0].position == 1
        assert reloaded_job.items[0].media_type == "video"
        assert reloaded_job.items[0].status == "pending"


@pytest.mark.asyncio
async def test_download_updates_new_item_in_same_session(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    dummy_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    dummy_file.write(b"dummy video content for download test")
    dummy_file.close()

    try:
        async with session_maker() as session:
            job_repo = JobRepository(session)
            job = await job_repo.create_job(
                inbound_message_id="msg-reg-b",
                webhook_event_id="evt-reg-b",
                sender_number="628111222333",
                original_url="https://www.tiktok.com/@creator/video/22222",
                canonical_url="https://www.tiktok.com/@creator/video/22222",
            )
            await session.commit()
            dummy_meta = TikTokContentMetadata(
                content_type="video",
                title="Test Video",
                author="Creator",
                duration_seconds=15,
                items=[TikTokMediaItemMetadata(position=1, source_url="http://src/video.mp4", media_type="video")],
            )
            worker = QueueWorker(session_maker)
            await worker._save_extracted_metadata(
                job.id,
                ExtractedMetadataResult(
                    canonical_url=job.canonical_url or job.original_url,
                    provider=DownloaderService().yt_dlp,
                    metadata=dummy_meta,
                ),
            )
            await worker._save_downloaded_results(
                job.id,
                DownloadedContentResult(
                    items=(
                        DownloadedItemResult(
                            position=1,
                            source_url="http://src/video.mp4",
                            media_type="video",
                            local_filename=dummy_file.name,
                            source_size_bytes=os.path.getsize(dummy_file.name),
                        ),
                    ),
                    source_size_bytes=os.path.getsize(dummy_file.name),
                ),
            )

        async with session_maker() as session:
            job_repo = JobRepository(session)
            reloaded_job = await job_repo.get_by_id(job.id)
            assert reloaded_job is not None
            assert len(reloaded_job.items) == 1
            assert reloaded_job.items[0].local_filename == dummy_file.name
            assert reloaded_job.items[0].source_size_bytes is not None and reloaded_job.items[0].source_size_bytes > 0
            assert reloaded_job.source_size_bytes is not None and reloaded_job.source_size_bytes > 0
            assert reloaded_job.source_size_bytes == reloaded_job.items[0].source_size_bytes
    finally:
        if os.path.exists(dummy_file.name):
            os.unlink(dummy_file.name)


@pytest.mark.asyncio
async def test_get_by_id_refreshes_stale_items(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-reg-c",
            webhook_event_id="evt-reg-c",
            sender_number="628111222333",
            original_url="https://www.tiktok.com/@creator/video/33333",
            canonical_url="https://www.tiktok.com/@creator/video/33333",
        )
        await session.commit()

        # Initial load shows 0 items
        job_first_load = await job_repo.get_by_id(job.id)
        assert job_first_load is not None
        assert len(job_first_load.items) == 0

        # Insert item directly in the same session without appending to job_first_load.items
        new_item = DownloadItem(
            job_id=job.id,
            position=1,
            media_type="video",
            status="pending",
            source_url="http://src/c.mp4",
        )
        session.add(new_item)
        await session.flush()

        # Call get_by_id again in the same session
        job_second_load = await job_repo.get_by_id(job.id)
        assert job_second_load is not None
        # It must be refreshed with populate_existing=True + selectinload and show the newly added item
        assert len(job_second_load.items) == 1
        assert job_second_load.items[0].position == 1


@pytest.mark.asyncio
async def test_worker_full_video_pipeline_calls_gateway(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    dummy_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    dummy_file.write(b"fake physical video data")
    dummy_file.close()

    try:
        async with session_maker() as session:
            job_repo = JobRepository(session)
            job = await job_repo.create_job(
                inbound_message_id="msg-reg-d",
                webhook_event_id="evt-reg-d",
                sender_number="628111222333",
                original_url="https://www.tiktok.com/@creator/video/44444",
                canonical_url="https://www.tiktok.com/@creator/video/44444",
            )
            await session.commit()
            job_id = job.id

        worker = QueueWorker(session_maker)

        dummy_meta = TikTokContentMetadata(
            content_type="video",
            title="Test Video",
            author="Creator",
            duration_seconds=15,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://src/video.mp4", media_type="video")],
        )
        downloaded_meta = TikTokContentMetadata(
            content_type="video",
            title="Test Video",
            author="Creator",
            duration_seconds=15,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://src/video.mp4", media_type="video", local_path=dummy_file.name)],
        )

        with patch.object(worker.gateway, "send_media", new_callable=AsyncMock) as mock_send, \
             patch.object(worker.gateway, "send_text", new_callable=AsyncMock) as mock_send_text, \
             patch("app.downloader.service.YtDlpProvider.extract_metadata", new_callable=AsyncMock, return_value=dummy_meta), \
             patch("app.downloader.service.YtDlpProvider.download_content", new_callable=AsyncMock, return_value=downloaded_meta), \
             patch("app.media.processor.MediaProcessor.process_job_media", new_callable=AsyncMock) as mock_proc:

            async def fake_proc(items, job_dir):
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

            mock_proc.side_effect = fake_proc
            mock_send.return_value = GatewayMessageResponse(status="ok", message_id="wa-msg-reg-d")
            mock_send_text.return_value = GatewayMessageResponse(status="ok", message_id="wa-msg-fail")

            await worker._process_job_safely(job_id)

            mock_send.assert_called_once()

        async with session_maker() as session:
            job_repo = JobRepository(session)
            finished_job = await job_repo.get_by_id(job_id)
            assert finished_job is not None
            assert finished_job.status == "gateway_queued"
            assert len(finished_job.items) == 1
            assert finished_job.items[0].status == "gateway_queued"
            assert finished_job.items[0].gateway_message_id == "wa-msg-reg-d"
    finally:
        if os.path.exists(dummy_file.name):
            os.unlink(dummy_file.name)


@pytest.mark.asyncio
async def test_worker_does_not_mark_empty_items_as_send_failed(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-reg-e",
            webhook_event_id="evt-reg-e",
            sender_number="628111222333",
            original_url="https://www.tiktok.com/@creator/video/55555",
            canonical_url="https://www.tiktok.com/@creator/video/55555",
        )
        job.media_count = 0
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    with patch.object(worker.gateway, "send_text", new_callable=AsyncMock) as mock_send_text:
        mock_send_text.return_value = GatewayMessageResponse(status="ok", message_id="wa-msg-fail")
        await worker._send_all_media_items(job_id)

    async with session_maker() as session:
        job_repo = JobRepository(session)
        failed_job = await job_repo.get_by_id(job_id)
        assert failed_job is not None
        assert failed_job.status == "failed"
        assert failed_job.error_code in ("NO_MEDIA_ITEMS", "INTERNAL_STATE_ERROR")
        assert failed_job.error_code != "SEND_FAILED"


@pytest.mark.asyncio
async def test_missing_local_filename_is_not_classified_as_gateway_failure(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-reg-f",
            webhook_event_id="evt-reg-f",
            sender_number="628111222333",
            original_url="https://www.tiktok.com/@creator/video/66666",
            canonical_url="https://www.tiktok.com/@creator/video/66666",
        )
        job.media_count = 1
        item = DownloadItem(
            job_id=job.id,
            position=1,
            media_type="video",
            status="pending",
            source_url="http://src/video.mp4",
            local_filename=None,  # Missing local filename
        )
        session.add(item)
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    fake_meta = TikTokContentMetadata(
        content_type="video",
        title="Test",
        author="Creator",
        duration_seconds=10,
        items=[TikTokMediaItemMetadata(position=1, source_url="http://src/video.mp4", media_type="video")],
    )

    with patch("app.downloader.service.YtDlpProvider.extract_metadata", new_callable=AsyncMock, return_value=fake_meta), \
         patch("app.downloader.service.DownloaderService.download_content", new_callable=AsyncMock, return_value=DownloadedContentResult(items=(), source_size_bytes=0)), \
         patch.object(worker.gateway, "send_media", new_callable=AsyncMock) as mock_send, \
         patch.object(worker.gateway, "send_text", new_callable=AsyncMock) as mock_send_text:
        mock_send_text.return_value = GatewayMessageResponse(status="ok", message_id="wa-msg-fail")

        # When worker continues to processing step, validation before processing should catch missing local_filename
        await worker._process_job_safely(job_id)

        mock_send.assert_not_called()

    async with session_maker() as session:
        job_repo = JobRepository(session)
        failed_job = await job_repo.get_by_id(job_id)
        assert failed_job is not None
        assert failed_job.status == "failed"
        assert failed_job.error_code != "SEND_FAILED"
        assert failed_job.error_code in ("DOWNLOAD_FAILED", "INTERNAL_STATE_ERROR")



@pytest.mark.asyncio
async def test_existing_sent_item_not_duplicated_or_resent(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    async with session_maker() as session:
        job_repo = JobRepository(session)
        job = await job_repo.create_job(
            inbound_message_id="msg-reg-g",
            webhook_event_id="evt-reg-g",
            sender_number="628111222333",
            original_url="https://www.tiktok.com/@creator/video/77777",
            canonical_url="https://www.tiktok.com/@creator/video/77777",
        )
        job.media_count = 1
        item = DownloadItem(
            job_id=job.id,
            position=1,
            media_type="video",
            status="sent",
            source_url="http://src/video.mp4",
            local_filename="/tmp/sent_video.mp4",
            source_size_bytes=500,
            final_size_bytes=500,
            gateway_message_id="old-gateway-msg-id-123",
        )
        session.add(item)
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)

    # Test metadata persistence doesn't duplicate position 1 or reset sent state
    async with session_maker() as session:
        job_repo = JobRepository(session)
        reloaded_job = await job_repo.get_by_id(job_id)
        assert reloaded_job is not None
        dummy_meta = TikTokContentMetadata(
            content_type="video",
            title="Test Video",
            author="Creator",
            duration_seconds=15,
            items=[TikTokMediaItemMetadata(position=1, source_url="http://new_src/video.mp4", media_type="video")],
        )
        await worker._save_extracted_metadata(
            job_id,
            ExtractedMetadataResult(
                canonical_url=reloaded_job.canonical_url or reloaded_job.original_url,
                provider=DownloaderService().yt_dlp,
                metadata=dummy_meta,
            ),
        )

        reloaded_job = await job_repo.get_by_id(job_id)
        assert reloaded_job is not None
        assert len(reloaded_job.items) == 1
        assert reloaded_job.items[0].status == "sent"
        assert reloaded_job.items[0].gateway_message_id == "old-gateway-msg-id-123"
        assert reloaded_job.items[0].source_size_bytes == 500

    # Test _send_all_media_items doesn't resend sent item
    with patch.object(worker.gateway, "send_media", new_callable=AsyncMock) as mock_send:
        await worker._send_all_media_items(job_id)
        mock_send.assert_not_called()

    # Job should complete right away since all items are sent
    async with session_maker() as session:
        job_repo = JobRepository(session)
        final_job = await job_repo.get_by_id(job_id)
        assert final_job is not None
        assert final_job.status == "sent"
        assert final_job.items[0].gateway_message_id == "old-gateway-msg-id-123"
