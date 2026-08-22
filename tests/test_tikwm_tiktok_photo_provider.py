from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.downloader.dtos import JobDownloadSnapshot
from app.downloader.exceptions import DownloadError
from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata
from app.downloader.service import DownloaderService
from app.downloader.tikwm_tiktok_photo_provider import TikwmTikTokPhotoProvider


@pytest.fixture
def tikwm_sample_photo_response() -> dict:
    return {
        "code": 0,
        "msg": "success",
        "processed_time": 0.15,
        "data": {
            "id": "7668360024648846599",
            "region": "ID",
            "title": "Photo slideshow post #fashion",
            "duration": 0,
            "author": {
                "id": "123456",
                "unique_id": "ade_meliora",
                "nickname": "Ade Meliora",
            },
            "images": [
                "https://www.tikwm.com/video/media/hdpack/slide1.jpeg",
                "https://www.tikwm.com/video/media/hdpack/slide2.jpeg",
                "https://www.tikwm.com/video/media/hdpack/slide3.jpeg",
            ],
        },
    }


@pytest.mark.asyncio
async def test_tikwm_extract_metadata_success(tikwm_sample_photo_response: dict, tmp_path: Path) -> None:
    provider = TikwmTikTokPhotoProvider()
    canonical_url = "https://www.tiktok.com/@ade_meliora/photo/7668360024648846599"

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = tikwm_sample_photo_response
        mock_client.get.return_value = mock_resp

        metadata = await provider.extract_metadata(canonical_url, tmp_path)

        assert metadata is not None
        assert metadata.content_type == "photo"
        assert metadata.title == "Photo slideshow post #fashion"
        assert metadata.author == "Ade Meliora"
        assert len(metadata.items) == 3
        assert metadata.items[0].position == 1
        assert metadata.items[0].source_url == "https://www.tikwm.com/video/media/hdpack/slide1.jpeg"
        assert metadata.items[0].media_type == "photo"

        # Verify query params passed
        mock_client.get.assert_called_once()
        _, kwargs = mock_client.get.call_args
        assert kwargs.get("params") == {"url": canonical_url, "hd": "1"}


@pytest.mark.asyncio
async def test_tikwm_extract_metadata_empty_images(tmp_path: Path) -> None:
    provider = TikwmTikTokPhotoProvider()
    canonical_url = "https://www.tiktok.com/@user/photo/7668360024648846599"

    payload = {
        "code": 0,
        "msg": "success",
        "data": {
            "id": "7668360024648846599",
            "images": [],
        },
    }

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload
        mock_client.get.return_value = mock_resp

        metadata = await provider.extract_metadata(canonical_url, tmp_path)
        assert metadata is None


@pytest.mark.asyncio
async def test_tikwm_extract_metadata_nonzero_code(tmp_path: Path) -> None:
    provider = TikwmTikTokPhotoProvider()
    canonical_url = "https://www.tiktok.com/@user/photo/7668360024648846599"

    payload = {
        "code": -1,
        "msg": "URL error or post deleted",
    }

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload
        mock_client.get.return_value = mock_resp

        metadata = await provider.extract_metadata(canonical_url, tmp_path)
        assert metadata is None


@pytest.mark.asyncio
async def test_tikwm_download_content_success(tmp_path: Path) -> None:
    provider = TikwmTikTokPhotoProvider()
    metadata = MediaContentMetadata(
        content_type="photo",
        title="Test Photo",
        author="Tester",
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/hdpack/slide1.jpeg",
                media_type="photo",
            )
        ],
    )

    valid_jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 100

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = valid_jpeg
        mock_client.get.return_value = mock_resp

        result = await provider.download_content(
            "https://www.tiktok.com/@user/photo/7668360024648846599", metadata, tmp_path
        )

        assert len(result.items) == 1
        assert result.items[0].local_path is not None
        assert Path(result.items[0].local_path).exists()
        assert Path(result.items[0].local_path).name == "photo_001.jpg"


@pytest.mark.asyncio
async def test_tikwm_download_content_cleanup_on_failure(tmp_path: Path) -> None:
    provider = TikwmTikTokPhotoProvider()
    metadata = MediaContentMetadata(
        content_type="photo",
        title="Test Photo",
        author="Tester",
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://www.tikwm.com/video/media/hdpack/slide1.jpeg",
                media_type="photo",
            ),
            MediaItemMetadata(
                position=2,
                source_url="https://www.tikwm.com/video/media/hdpack/slide2.jpeg",
                media_type="photo",
            ),
        ],
    )

    valid_jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 100

    class MockResp:
        def __init__(self, content: bytes) -> None:
            self.content = content
            self.status_code = 200

        def raise_for_status(self) -> None:
            pass

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_client.get.side_effect = [MockResp(valid_jpeg), Exception("Network download dropped")]

        with pytest.raises(DownloadError) as exc_info:
            await provider.download_content(
                "https://www.tiktok.com/@user/photo/7668360024648846599", metadata, tmp_path
            )

        assert "Gagal mengunduh foto slide #2" in str(exc_info.value)
        # Ensure temporary files are cleaned up
        assert list(tmp_path.glob("photo_*")) == []


@pytest.mark.asyncio
async def test_photo_route_falls_back_to_tikwm_when_native_fail(
    tikwm_sample_photo_response: dict, tmp_path: Path
) -> None:
    service = DownloaderService()
    canonical_url = "https://www.tiktok.com/@ade_meliora/photo/7668360024648846599"

    snapshot = JobDownloadSnapshot(
        id="job-1",
        original_url=canonical_url,
        canonical_url=canonical_url,
        platform="tiktok",
        items=(),
    )

    # 1. Native gallery-dl returns None / fails
    # 2. Native TikTokPhotoProvider returns None / fails
    # 3. TikWM succeeds
    with patch.object(service.gallery_dl, "extract_metadata", new_callable=AsyncMock) as mock_gdl, \
         patch.object(service.photo_provider, "extract_metadata", new_callable=AsyncMock) as mock_native, \
         patch.object(service.tikwm_provider, "extract_metadata", new_callable=AsyncMock) as mock_tikwm:

        mock_gdl.return_value = None
        mock_native.return_value = None
        mock_tikwm.return_value = MediaContentMetadata(
            content_type="photo",
            title="TikWM Title",
            author="Ade Meliora",
            items=[
                MediaItemMetadata(
                    position=1,
                    source_url="https://www.tikwm.com/slide1.jpeg",
                    media_type="photo",
                )
            ],
        )

        result = await service.extract_metadata(snapshot, tmp_path)

        assert result.metadata is not None
        assert result.metadata.title == "TikWM Title"
        assert result.provider == service.tikwm_provider
        mock_gdl.assert_called_once()
        mock_native.assert_called_once()
        mock_tikwm.assert_called_once()


@pytest.mark.asyncio
async def test_video_route_does_not_switch_to_tikwm(tmp_path: Path) -> None:
    service = DownloaderService()
    canonical_url = "https://www.tiktok.com/@user/video/7123456789012345678"

    snapshot = JobDownloadSnapshot(
        id="job-2",
        original_url=canonical_url,
        canonical_url=canonical_url,
        platform="tiktok",
        items=(),
    )

    with patch.object(service.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(service.tikwm_provider, "extract_metadata", new_callable=AsyncMock) as mock_tikwm:

        mock_ytdlp.return_value = None

        with pytest.raises(DownloadError) as exc_info:
            await service.extract_metadata(snapshot, tmp_path)

        assert "yt-dlp tidak menghasilkan metadata untuk canonical URL /video/" in str(exc_info.value)
        # TikWM must never be called for classified video URLs
        mock_tikwm.assert_not_called()
