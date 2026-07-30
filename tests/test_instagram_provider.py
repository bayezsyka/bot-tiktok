import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.downloader.exceptions import (
    ContentNotSupportedError,
)
from app.downloader.instagram_provider import InstagramReelProvider


@pytest.mark.asyncio
async def test_instagram_provider_metadata_success(tmp_path: Path) -> None:
    provider = InstagramReelProvider()

    dummy_json = {
        "id": "C1234567890",
        "title": "Amazing Instagram Reel Video",
        "uploader": "insta_creator",
        "duration": 45,
        "ext": "mp4",
        "vcodec": "h264",
    }

    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate.return_value = (json.dumps(dummy_json).encode("utf-8"), b"")

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        metadata = await provider.extract_metadata("https://www.instagram.com/reel/C1234567890/", tmp_path)
        assert metadata is not None
        assert metadata.content_type == "video"
        assert metadata.title == "Amazing Instagram Reel Video"
        assert metadata.author == "insta_creator"
        assert metadata.duration_seconds == 45
        assert len(metadata.items) == 1
        assert metadata.items[0].media_type == "video"


@pytest.mark.asyncio
async def test_instagram_provider_private_and_login_required(tmp_path: Path) -> None:
    provider = InstagramReelProvider()

    mock_proc = AsyncMock()
    mock_proc.returncode = 1
    mock_proc.communicate.return_value = (b"", b"ERROR: [Instagram] C123: This post is private or login required.")

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        with pytest.raises(ContentNotSupportedError) as exc_info:
            await provider.extract_metadata("https://www.instagram.com/reel/C1234567890/", tmp_path)
        assert "publik" in exc_info.value.user_friendly_message


@pytest.mark.asyncio
async def test_instagram_provider_reject_live_and_carousel(tmp_path: Path) -> None:
    provider = InstagramReelProvider()

    # 1. Live stream
    dummy_live = {"id": "C123", "is_live": True, "duration": 10}
    mock_proc1 = AsyncMock()
    mock_proc1.returncode = 0
    mock_proc1.communicate.return_value = (json.dumps(dummy_live).encode("utf-8"), b"")

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc1):
        with pytest.raises(ContentNotSupportedError) as exc:
            await provider.extract_metadata("https://www.instagram.com/reel/C123/", tmp_path)
        assert "live" in exc.value.message.lower()

    # 2. Carousel / playlist entries
    dummy_carousel = {"id": "C123", "_type": "playlist", "entries": [{}]}
    mock_proc2 = AsyncMock()
    mock_proc2.returncode = 0
    mock_proc2.communicate.return_value = (json.dumps(dummy_carousel).encode("utf-8"), b"")

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc2):
        with pytest.raises(ContentNotSupportedError) as exc:
            await provider.extract_metadata("https://www.instagram.com/reel/C123/", tmp_path)
        assert "carousel" in exc.value.message.lower() or "playlist" in exc.value.message.lower()


@pytest.mark.asyncio
async def test_instagram_provider_download_content_success(tmp_path: Path) -> None:
    provider = InstagramReelProvider()

    from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata

    metadata = MediaContentMetadata(
        content_type="video",
        title="Test Reel",
        author="creator",
        duration_seconds=15,
        items=[MediaItemMetadata(position=1, source_url="https://www.instagram.com/reel/C123/", media_type="video")],
    )

    # Mock subprocess creating physical output file in tmp_path
    video_file = tmp_path / "video_source.mp4"
    video_file.write_bytes(b"dummy video bytes")

    mock_proc = AsyncMock()
    mock_proc.returncode = 0
    mock_proc.communicate.return_value = (b"", b"")

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        updated_meta = await provider.download_content("https://www.instagram.com/reel/C123/", metadata, tmp_path)
        assert updated_meta.items[0].local_path == str(video_file.resolve())


@pytest.mark.asyncio
async def test_instagram_provider_cookie_flag_handling(tmp_path: Path) -> None:
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")

    provider = InstagramReelProvider()
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)
    args = provider._get_base_args()
    assert "--cookies" in args
    assert str(cookie_file) in args



