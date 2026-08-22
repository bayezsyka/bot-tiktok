from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.downloader.dtos import JobDownloadSnapshot
from app.downloader.exceptions import DownloadError, DownloadSizeLimitExceededError
from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata
from app.downloader.service import DownloaderService
from app.downloader.tikwm_tiktok_video_provider import TikwmTikTokVideoProvider


@pytest.fixture
def tikwm_sample_video_response_hdplay() -> dict:
    return {
        "code": 0,
        "msg": "success",
        "processed_time": 0.12,
        "data": {
            "id": "7646821158519672085",
            "region": "ID",
            "title": "Aesthetic video post #dance",
            "duration": 45,
            "play": "https://www.tikwm.com/video/media/play/7646821158519672085.mp4",
            "hdplay": "https://www.tikwm.com/video/media/hdplay/7646821158519672085.mp4",
            "author": {
                "id": "7891011",
                "unique_id": "lolydchan",
                "nickname": "Loly Dchan",
            },
        },
    }


@pytest.fixture
def tikwm_sample_video_response_play_only() -> dict:
    return {
        "code": 0,
        "msg": "success",
        "processed_time": 0.10,
        "data": {
            "id": "7646821158519672085",
            "title": "Standard quality video",
            "duration": 20,
            "play": "https://www.tikwm.com/video/media/play/7646821158519672085.mp4",
            "author": {
                "unique_id": "test_creator",
                "nickname": "Test Creator",
            },
        },
    }


@pytest.mark.asyncio
async def test_tikwm_video_extract_metadata_hdplay_success(
    tikwm_sample_video_response_hdplay: dict, tmp_path: Path
) -> None:
    provider = TikwmTikTokVideoProvider()
    canonical_url = "https://www.tiktok.com/@lolydchan/video/7646821158519672085"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = tikwm_sample_video_response_hdplay
        mock_client.get.return_value = mock_resp

        metadata = await provider.extract_metadata(canonical_url, tmp_path)

        assert metadata is not None
        assert metadata.content_type == "video"
        assert metadata.title == "Aesthetic video post #dance"
        assert metadata.author == "Loly Dchan"
        assert metadata.duration_seconds == 45
        assert len(metadata.items) == 1
        assert metadata.items[0].position == 1
        assert (
            metadata.items[0].source_url
            == "https://www.tikwm.com/video/media/hdplay/7646821158519672085.mp4"
        )
        assert metadata.items[0].media_type == "video"


@pytest.mark.asyncio
async def test_tikwm_video_fallback_to_play_when_hdplay_absent(
    tikwm_sample_video_response_play_only: dict, tmp_path: Path
) -> None:
    provider = TikwmTikTokVideoProvider()
    canonical_url = "https://www.tiktok.com/@lolydchan/video/7646821158519672085"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = tikwm_sample_video_response_play_only
        mock_client.get.return_value = mock_resp

        metadata = await provider.extract_metadata(canonical_url, tmp_path)

        assert metadata is not None
        assert metadata.content_type == "video"
        assert (
            metadata.items[0].source_url
            == "https://www.tikwm.com/video/media/play/7646821158519672085.mp4"
        )


@pytest.mark.asyncio
async def test_tikwm_video_nonzero_code(tmp_path: Path) -> None:
    provider = TikwmTikTokVideoProvider()
    canonical_url = "https://www.tiktok.com/@user/video/7646821158519672085"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"code": -1, "msg": "video not found"}
        mock_client.get.return_value = mock_resp

        metadata = await provider.extract_metadata(canonical_url, tmp_path)
        assert metadata is None


@pytest.mark.asyncio
async def test_tikwm_video_missing_url(tmp_path: Path) -> None:
    provider = TikwmTikTokVideoProvider()
    canonical_url = "https://www.tiktok.com/@user/video/7646821158519672085"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"code": 0, "data": {"id": "123"}}
        mock_client.get.return_value = mock_resp

        metadata = await provider.extract_metadata(canonical_url, tmp_path)
        assert metadata is None


class MockStreamResponse:
    def __init__(
        self,
        chunks: list[bytes],
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.chunks = chunks
        self.status_code = status_code
        self.headers = headers or {"content-type": "video/mp4"}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise DownloadError(f"HTTP {self.status_code}")

    async def aiter_bytes(self, chunk_size: int = 65536):
        for chunk in self.chunks:
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.mark.asyncio
async def test_tikwm_video_download_success(tmp_path: Path) -> None:
    provider = TikwmTikTokVideoProvider()
    metadata = MediaContentMetadata(
        content_type="video",
        title="Test Video",
        author="Tester",
        duration_seconds=10,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/play/123.mp4",
                media_type="video",
            )
        ],
    )

    fake_video_bytes = b"\x00\x00\x00\x1cftypisom\x00\x00\x02\x00isomiso2mp41" + b"\x00" * 500

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        mock_client.stream = MagicMock(return_value=MockStreamResponse([fake_video_bytes]))

        result = await provider.download_content(
            "https://www.tiktok.com/@user/video/123", metadata, tmp_path
        )

        assert result.items[0].local_path is not None
        saved_path = Path(result.items[0].local_path)
        assert saved_path.exists()
        assert saved_path.name == "video_source.mp4"
        assert saved_path.stat().st_size == len(fake_video_bytes)


@pytest.mark.asyncio
async def test_tikwm_video_download_content_length_limit_exceeded(tmp_path: Path) -> None:
    provider = TikwmTikTokVideoProvider()
    metadata = MediaContentMetadata(
        content_type="video",
        title="Test Video",
        author="Tester",
        duration_seconds=10,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/play/123.mp4",
                media_type="video",
            )
        ],
    )

    max_bytes = provider.settings.MAX_SOURCE_DOWNLOAD_MB * 1024 * 1024
    oversized_length = str(max_bytes + 1024)

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        mock_client.stream = MagicMock(
            return_value=MockStreamResponse(
                chunks=[],
                headers={"content-type": "video/mp4", "content-length": oversized_length},
            )
        )

        with pytest.raises(DownloadSizeLimitExceededError):
            await provider.download_content(
                "https://www.tiktok.com/@user/video/123", metadata, tmp_path
            )

        assert list(tmp_path.glob("video_source*")) == []


@pytest.mark.asyncio
async def test_tikwm_video_download_stream_size_limit_exceeded(tmp_path: Path) -> None:
    provider = TikwmTikTokVideoProvider()
    metadata = MediaContentMetadata(
        content_type="video",
        title="Test Video",
        author="Tester",
        duration_seconds=10,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/play/123.mp4",
                media_type="video",
            )
        ],
    )

    chunk_1 = b"\x00\x00\x00\x1cftypisom" + b"\x00" * 100
    # Simulate a chunk that pushes total streamed over the limit
    oversized_chunk = b"\x00" * (provider.settings.MAX_SOURCE_DOWNLOAD_MB * 1024 * 1024 + 1024)

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        mock_client.stream = MagicMock(
            return_value=MockStreamResponse(
                chunks=[chunk_1, oversized_chunk],
                headers={"content-type": "video/mp4"},
            )
        )

        with pytest.raises(DownloadSizeLimitExceededError):
            await provider.download_content(
                "https://www.tiktok.com/@user/video/123", metadata, tmp_path
            )

        assert list(tmp_path.glob("video_source*")) == []


@pytest.mark.asyncio
async def test_tikwm_video_download_rejects_html_content_type(tmp_path: Path) -> None:
    provider = TikwmTikTokVideoProvider()
    metadata = MediaContentMetadata(
        content_type="video",
        title="Test Video",
        author="Tester",
        duration_seconds=10,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/play/123.mp4",
                media_type="video",
            )
        ],
    )

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        mock_client.stream = MagicMock(
            return_value=MockStreamResponse(
                chunks=[b"<!DOCTYPE html><html><body>Error page</body></html>"],
                headers={"content-type": "text/html"},
            )
        )

        with pytest.raises(DownloadError) as exc_info:
            await provider.download_content(
                "https://www.tiktok.com/@user/video/123", metadata, tmp_path
            )

        assert "Server TikWM mengembalikan response non-video" in str(exc_info.value)
        assert list(tmp_path.glob("video_source*")) == []


@pytest.mark.asyncio
async def test_tikwm_video_download_rejects_html_body_header(tmp_path: Path) -> None:
    provider = TikwmTikTokVideoProvider()
    metadata = MediaContentMetadata(
        content_type="video",
        title="Test Video",
        author="Tester",
        duration_seconds=10,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/play/123.mp4",
                media_type="video",
            )
        ],
    )

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        # Server returned video/mp4 header but HTML body
        mock_client.stream = MagicMock(
            return_value=MockStreamResponse(
                chunks=[b"<!DOCTYPE html><html><body>Error</body></html>"],
                headers={"content-type": "video/mp4"},
            )
        )

        with pytest.raises(DownloadError) as exc_info:
            await provider.download_content(
                "https://www.tiktok.com/@user/video/123", metadata, tmp_path
            )

        assert "File video hasil unduhan bukan stream video yang valid" in str(exc_info.value)
        assert list(tmp_path.glob("video_source*")) == []


@pytest.mark.asyncio
async def test_canonical_video_ytdlp_success_does_not_call_tikwm(tmp_path: Path) -> None:
    service = DownloaderService()
    canonical_url = "https://www.tiktok.com/@user/video/7646821158519672085"

    snapshot = JobDownloadSnapshot(
        id="job-v1",
        original_url=canonical_url,
        canonical_url=canonical_url,
        platform="tiktok",
        items=(),
    )

    fake_metadata = MediaContentMetadata(
        content_type="video",
        title="yt-dlp Video",
        author="Creator",
        duration_seconds=30,
        items=[
            MediaItemMetadata(
                position=1,
                source_url=canonical_url,
                media_type="video",
            )
        ],
    )

    with patch.object(service.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(service.tikwm_video_provider, "extract_metadata", new_callable=AsyncMock) as mock_tikwm_video:

        mock_ytdlp.return_value = fake_metadata

        result = await service.extract_metadata(snapshot, tmp_path)

        assert result.metadata == fake_metadata
        assert result.provider == service.yt_dlp
        mock_ytdlp.assert_called_once()
        mock_tikwm_video.assert_not_called()


@pytest.mark.asyncio
async def test_canonical_video_ytdlp_exception_falls_back_to_tikwm(tmp_path: Path) -> None:
    service = DownloaderService()
    canonical_url = "https://www.tiktok.com/@lolydchan/video/7646821158519672085"

    snapshot = JobDownloadSnapshot(
        id="job-v2",
        original_url=canonical_url,
        canonical_url=canonical_url,
        platform="tiktok",
        items=(),
    )

    fake_tikwm_metadata = MediaContentMetadata(
        content_type="video",
        title="TikWM Fallback Video",
        author="Loly Dchan",
        duration_seconds=45,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/hdplay/7646821158519672085.mp4",
                media_type="video",
            )
        ],
    )

    with patch.object(service.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(service.tikwm_video_provider, "extract_metadata", new_callable=AsyncMock) as mock_tikwm_video:

        # yt-dlp raises error (e.g. "Unexpected response from webpage request")
        mock_ytdlp.side_effect = DownloadError("Unexpected response from webpage request")
        mock_tikwm_video.return_value = fake_tikwm_metadata

        result = await service.extract_metadata(snapshot, tmp_path)

        assert result.metadata == fake_tikwm_metadata
        assert result.provider == service.tikwm_video_provider
        mock_ytdlp.assert_called_once()
        mock_tikwm_video.assert_called_once()


@pytest.mark.asyncio
async def test_canonical_video_ytdlp_empty_result_falls_back_to_tikwm(tmp_path: Path) -> None:
    service = DownloaderService()
    canonical_url = "https://www.tiktok.com/@lolydchan/video/7646821158519672085"

    snapshot = JobDownloadSnapshot(
        id="job-v3",
        original_url=canonical_url,
        canonical_url=canonical_url,
        platform="tiktok",
        items=(),
    )

    fake_tikwm_metadata = MediaContentMetadata(
        content_type="video",
        title="TikWM Fallback Video",
        author="Loly Dchan",
        duration_seconds=45,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/hdplay/7646821158519672085.mp4",
                media_type="video",
            )
        ],
    )

    with patch.object(service.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(service.tikwm_video_provider, "extract_metadata", new_callable=AsyncMock) as mock_tikwm_video:

        mock_ytdlp.return_value = None
        mock_tikwm_video.return_value = fake_tikwm_metadata

        result = await service.extract_metadata(snapshot, tmp_path)

        assert result.metadata == fake_tikwm_metadata
        assert result.provider == service.tikwm_video_provider
        mock_ytdlp.assert_called_once()
        mock_tikwm_video.assert_called_once()


@pytest.mark.asyncio
async def test_canonical_video_tikwm_failure_after_ytdlp_failure_raises_error(tmp_path: Path) -> None:
    service = DownloaderService()
    canonical_url = "https://www.tiktok.com/@user/video/7646821158519672085"

    snapshot = JobDownloadSnapshot(
        id="job-v4",
        original_url=canonical_url,
        canonical_url=canonical_url,
        platform="tiktok",
        items=(),
    )

    with patch.object(service.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(service.tikwm_video_provider, "extract_metadata", new_callable=AsyncMock) as mock_tikwm_video:

        mock_ytdlp.side_effect = DownloadError("yt-dlp extraction error")
        mock_tikwm_video.return_value = None

        with pytest.raises(DownloadError) as exc_info:
            await service.extract_metadata(snapshot, tmp_path)

        assert "yt-dlp extraction error" in str(exc_info.value)
        mock_ytdlp.assert_called_once()
        mock_tikwm_video.assert_called_once()
