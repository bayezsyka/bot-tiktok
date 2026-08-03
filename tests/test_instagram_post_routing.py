from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.downloader.dtos import JobDownloadSnapshot
from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata
from app.downloader.service import DownloaderService

# --- Tests 8-10: Routing /p/ -> Post provider, /reel/ -> Reel provider ---

@pytest.mark.asyncio
async def test_p_url_calls_only_post_provider(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_p_route",
        original_url="https://www.instagram.com/p/DbgFWkXMQXa/",
        canonical_url="https://www.instagram.com/p/DbgFWkXMQXa/",
        platform="instagram",
        items=(),
    )
    fake_meta = MediaContentMetadata(
        content_type="photo",
        title="IG Post",
        author="user",
        duration_seconds=0,
        items=[MediaItemMetadata(position=1, source_url="https://instagram.fsrg6-1.fna.fbcdn.net/v/img.jpg", media_type="photo")],
    )

    with patch.object(downloader.ig_post_provider, "extract_metadata", new_callable=AsyncMock) as mock_post, \
         patch.object(downloader.ig_provider, "extract_metadata", new_callable=AsyncMock) as mock_reel:
        mock_post.return_value = fake_meta
        res = await downloader.extract_metadata(snapshot, tmp_path)

        assert res.metadata == fake_meta
        assert res.provider == downloader.ig_post_provider
        mock_post.assert_called_once()
        mock_reel.assert_not_called()


@pytest.mark.asyncio
async def test_p_url_does_not_call_reel_provider(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_p_no_reel",
        original_url="https://www.instagram.com/p/Abc123/",
        canonical_url="https://www.instagram.com/p/Abc123/",
        platform="instagram",
        items=(),
    )
    fake_meta = MediaContentMetadata(
        content_type="photo",
        title="IG Post",
        author="u",
        duration_seconds=0,
        items=[MediaItemMetadata(position=1, source_url="https://instagram.fsrg6-1.fna.fbcdn.net/v/img.jpg", media_type="photo")],
    )

    with patch.object(downloader.ig_post_provider, "extract_metadata", new_callable=AsyncMock) as mock_post, \
         patch.object(downloader.ig_provider, "extract_metadata", new_callable=AsyncMock) as mock_reel:
        mock_post.return_value = fake_meta
        await downloader.extract_metadata(snapshot, tmp_path)
        mock_reel.assert_not_called()


@pytest.mark.asyncio
async def test_reel_url_does_not_call_post_provider(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_reel_no_post",
        original_url="https://www.instagram.com/reel/C123/",
        canonical_url="https://www.instagram.com/reel/C123/",
        platform="instagram",
        items=(),
    )
    fake_meta = MediaContentMetadata(
        content_type="video",
        title="IG Reel",
        author="u",
        duration_seconds=15,
        items=[MediaItemMetadata(position=1, source_url="https://www.instagram.com/reel/C123/", media_type="video")],
    )

    with patch.object(downloader.ig_post_provider, "extract_metadata", new_callable=AsyncMock) as mock_post, \
         patch.object(downloader.ig_provider, "extract_metadata", new_callable=AsyncMock) as mock_reel:
        mock_reel.return_value = fake_meta
        res = await downloader.extract_metadata(snapshot, tmp_path)

        assert res.metadata == fake_meta
        assert res.provider == downloader.ig_provider
        mock_reel.assert_called_once()
        mock_post.assert_not_called()


@pytest.mark.asyncio
async def test_reels_url_does_not_call_post_provider(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_reels_no_post",
        original_url="https://www.instagram.com/reels/C456/",
        canonical_url="https://www.instagram.com/reels/C456/",
        platform="instagram",
        items=(),
    )
    fake_meta = MediaContentMetadata(
        content_type="video",
        title="IG Reels",
        author="u",
        duration_seconds=15,
        items=[MediaItemMetadata(position=1, source_url="https://www.instagram.com/reels/C456/", media_type="video")],
    )

    with patch.object(downloader.ig_post_provider, "extract_metadata", new_callable=AsyncMock) as mock_post, \
         patch.object(downloader.ig_provider, "extract_metadata", new_callable=AsyncMock) as mock_reel:
        mock_reel.return_value = fake_meta
        await downloader.extract_metadata(snapshot, tmp_path)
        mock_post.assert_not_called()
