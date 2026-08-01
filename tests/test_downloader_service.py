import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.database.repositories import JobRepository
from app.downloader.dtos import JobDownloadSnapshot
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

    refetched_job = await job_repo.get_by_id(job.id)
    assert refetched_job is not None

    downloader = DownloaderService()
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
            result = await downloader.extract_metadata(
                JobDownloadSnapshot(
                    id=refetched_job.id,
                    original_url=refetched_job.original_url,
                    canonical_url=refetched_job.canonical_url,
                    platform=refetched_job.platform,
                    items=(),
                ),
                Path(tmp_dir),
            )

        mock_resolve.assert_called_once_with("https://vt.tiktok.com/ZS12345ab/")
        assert result.canonical_url == "https://www.tiktok.com/@creator/video/1234567890123456789"
        assert result.metadata == dummy_meta


@pytest.mark.asyncio
async def test_photo_url_routes_to_gallery_dl_first_and_skips_ytdlp(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_photo_route",
        original_url="https://vt.tiktok.com/ZS4htbqUV/",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        platform="tiktok",
        items=(),
    )

    fake_meta = TikTokContentMetadata(
        content_type="photo",
        title="Gallery-dl photo",
        author="User",
        duration_seconds=0,
        items=[TikTokMediaItemMetadata(position=1, source_url="https://p16-common-sign.tiktokcdn.com/slide1.jpg", media_type="photo")],
    )

    with patch.object(downloader.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(downloader.gallery_dl, "extract_metadata", new_callable=AsyncMock) as mock_gdl, \
         patch.object(downloader.photo_provider, "extract_metadata", new_callable=AsyncMock) as mock_html:

        mock_gdl.return_value = fake_meta

        res = await downloader.extract_metadata(snapshot, tmp_path)

        assert res.metadata == fake_meta
        assert res.provider == downloader.gallery_dl
        # yt-dlp MUST NOT be called for /photo/ URLs!
        mock_ytdlp.assert_not_called()
        mock_gdl.assert_called_once()
        mock_html.assert_not_called()


@pytest.mark.asyncio
async def test_photo_url_fallback_to_html_parser_if_gallery_dl_returns_none(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_photo_fallback",
        original_url="https://vt.tiktok.com/ZS4htbqUV/",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        platform="tiktok",
        items=(),
    )

    fake_html_meta = TikTokContentMetadata(
        content_type="photo",
        title="HTML Fallback Photo",
        author="User",
        duration_seconds=0,
        items=[TikTokMediaItemMetadata(position=1, source_url="https://p16-common-sign.tiktokcdn.com/slide1.jpg", media_type="photo")],
    )

    with patch.object(downloader.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(downloader.gallery_dl, "extract_metadata", new_callable=AsyncMock) as mock_gdl, \
         patch.object(downloader.photo_provider, "extract_metadata", new_callable=AsyncMock) as mock_html:

        mock_gdl.return_value = None  # gallery-dl returned None
        mock_html.return_value = fake_html_meta

        res = await downloader.extract_metadata(snapshot, tmp_path)

        assert res.metadata == fake_html_meta
        assert res.provider == downloader.photo_provider
        mock_ytdlp.assert_not_called()
        mock_gdl.assert_called_once()
        mock_html.assert_called_once()


@pytest.mark.asyncio
async def test_video_url_routes_to_ytdlp_first(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_video_route",
        original_url="https://www.tiktok.com/@user/video/7123456789012345678",
        canonical_url="https://www.tiktok.com/@user/video/7123456789012345678",
        platform="tiktok",
        items=(),
    )

    video_meta = TikTokContentMetadata(
        content_type="video",
        title="TikTok Video",
        author="User",
        duration_seconds=15,
        items=[TikTokMediaItemMetadata(position=1, source_url="https://video.mp4", media_type="video")],
    )

    with patch.object(downloader.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(downloader.gallery_dl, "extract_metadata", new_callable=AsyncMock) as mock_gdl:

        mock_ytdlp.return_value = video_meta

        res = await downloader.extract_metadata(snapshot, tmp_path)

        assert res.metadata == video_meta
        assert res.provider == downloader.yt_dlp
        mock_ytdlp.assert_called_once()
        mock_gdl.assert_not_called()
