import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.database.repositories import JobRepository
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.service import DownloaderService
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_downloader_service_resolves_canonical_url_in_worker(test_db: AsyncSession) -> None:
    job_repo = JobRepository(test_db)
    job = await job_repo.create_job(
        inbound_message_id="msg-worker-res-01",
        webhook_event_id="evt-worker-res-01",
        sender_number="628111222333",
        original_url="https://vt.tiktok.com/ZS12345ab/",
        canonical_url=None,
    )
    await test_db.commit()

    # Re-fetch with relationships eager-loaded
    refetched_job = await job_repo.get_by_id(job.id)
    assert refetched_job is not None


    downloader = DownloaderService(test_db)
    dummy_meta = TikTokContentMetadata(
        content_type="video",
        title="Test Video",
        author="Creator",
        duration_seconds=15,
        items=[TikTokMediaItemMetadata(position=1, source_url="http://src/1.mp4", media_type="video")],
    )

    with patch("app.downloader.service.resolve_canonical_tiktok_url", new_callable=AsyncMock) as mock_resolve, \
         patch.object(downloader.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_extract:
        mock_resolve.return_value = "https://www.tiktok.com/@creator/video/1234567890123456789"
        mock_extract.return_value = dummy_meta

        with tempfile.TemporaryDirectory() as tmp_dir:
            provider, meta = await downloader.extract_and_prepare_job(refetched_job, Path(tmp_dir))

        mock_resolve.assert_called_once_with("https://vt.tiktok.com/ZS12345ab/")
        assert refetched_job.canonical_url == "https://www.tiktok.com/@creator/video/1234567890123456789"

