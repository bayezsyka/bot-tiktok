import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.downloader.dtos import JobDownloadSnapshot
from app.downloader.exceptions import (
    DownloadError,
    DownloadSizeLimitExceededError,
    TikTokChallengeError,
)
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.service import DownloaderService
from app.downloader.tiktok_photo_provider import TikTokPhotoProvider, _load_netscape_cookies


@pytest.mark.asyncio
async def test_cookies_loading_netscape_and_httponly(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text(
        "# Netscape HTTP Cookie File\n"
        "# This is a standard comment\n"
        ".tiktok.com\tTRUE\t/\tFALSE\t1900000000\tregular_cookie\tsecret_regular_value\n"
        "#HttpOnly_.tiktok.com\tTRUE\t/\tTRUE\t1900000000\tsessionid\tsecret_session_value\n"
        "#HttpOnly_www.tiktok.com\tFALSE\t/\tTRUE\t1000000000\texpired_cookie\tsecret_expired_value\n"
        "# Malformed line without tabs\n"
        "invalid_line_without_tabs\n"
    )

    with caplog.at_level(logging.DEBUG):
        cookies = _load_netscape_cookies(str(cookie_file))

    assert cookies is not None
    assert cookies.get("regular_cookie") == "secret_regular_value"
    assert cookies.get("sessionid") == "secret_session_value"
    assert cookies.get("expired_cookie") == "secret_expired_value"

    # Verify secret cookie values do NOT appear in log output
    assert "secret_regular_value" not in caplog.text
    assert "secret_session_value" not in caplog.text
    assert "secret_expired_value" not in caplog.text

    # Non-existent file
    assert _load_netscape_cookies(str(tmp_path / "non_existent.txt")) is None


@pytest.mark.asyncio
async def test_cookies_passed_to_fetch_html_fallback_and_download(tmp_path: Path) -> None:
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text(
        "# Netscape HTTP Cookie File\n"
        ".tiktok.com\tTRUE\t/\tFALSE\t1900000000\tttwid\tmy_ttwid_value\n"
    )

    provider = TikTokPhotoProvider()
    provider.settings.TIKTOK_COOKIES_FILE = str(cookie_file)

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        class MockResp:
            status_code = 200
            headers = {"content-type": "text/html"}
            text = "<html><title>Test</title></html>"
            content = b"<html><title>Test</title></html>"

            def raise_for_status(self) -> None:
                pass

        mock_get.return_value = MockResp()
        await provider._fetch_html("https://www.tiktok.com/@user/photo/7668360024648846599")
        assert mock_get.called


@pytest.mark.asyncio
async def test_challenge_page_raises_tiktok_challenge_error(tmp_path: Path) -> None:
    provider = TikTokPhotoProvider()
    challenge_html = """
    <html>
    <head><title>TikTok - Make Your Day</title></head>
    <body><div class="captcha-container">Verify captcha</div></body>
    </html>
    """

    with patch.object(provider, "_fetch_html", new_callable=AsyncMock) as mock_fetch_html, \
         patch.object(provider, "_fetch_fallback_item_detail", new_callable=AsyncMock) as mock_fallback:
        mock_fetch_html.return_value = challenge_html
        mock_fallback.return_value = None

        with pytest.raises(TikTokChallengeError) as exc_info:
            await provider.extract_metadata(
                "https://www.tiktok.com/@ade_meliora/photo/7668360024648846599", tmp_path
            )

        assert "TikTok sementara menolak akses downloader" in exc_info.value.user_friendly_message


@pytest.mark.asyncio
async def test_unsupported_ytdlp_transitions_to_photo_provider(tmp_path: Path) -> None:
    service = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="test_job_1",
        original_url="https://vt.tiktok.com/ZS4htbqUV/",
        canonical_url="https://www.tiktok.com/@ade_meliora/v/7668360024648846599",
        platform="tiktok",
        items=(),
    )

    fake_metadata = TikTokContentMetadata(
        content_type="photo",
        title="Test Photo Post",
        author="Ade Meliora",
        duration_seconds=0,
        items=[
            TikTokMediaItemMetadata(
                position=1,
                source_url="https://p16-sign-va.tiktokcdn.com/photo1.jpg",
                media_type="photo",
            )
        ],
    )

    with patch.object(service.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_ytdlp, \
         patch.object(service.gallery_dl, "extract_metadata", new_callable=AsyncMock) as mock_gdl, \
         patch.object(service.photo_provider, "extract_metadata", new_callable=AsyncMock) as mock_photo:
        mock_ytdlp.return_value = None  # yt-dlp passed/unsupported
        mock_gdl.return_value = None
        mock_photo.return_value = fake_metadata

        res = await service.extract_metadata(snapshot, tmp_path)
        assert res.metadata == fake_metadata
        assert res.provider == service.photo_provider
        mock_ytdlp.assert_called_once()
        mock_gdl.assert_called_once()
        mock_photo.assert_called_once()


@pytest.mark.asyncio
async def test_download_content_valid_and_cleanup_on_failure(tmp_path: Path) -> None:
    provider = TikTokPhotoProvider()
    metadata = TikTokContentMetadata(
        content_type="photo",
        title="Test Slide",
        author="Tester",
        duration_seconds=0,
        items=[
            TikTokMediaItemMetadata(
                position=1,
                source_url="https://p16-sign-va.tiktokcdn.com/slide1.jpg",
                media_type="photo",
            ),
            TikTokMediaItemMetadata(
                position=2,
                source_url="https://p16-sign-va.tiktokcdn.com/slide2.jpg",
                media_type="photo",
            ),
        ],
    )

    # JPEG header bytes
    valid_jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 100

    class MockResponse:
        def __init__(self, content: bytes, status_code: int = 200) -> None:
            self.content = content
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code != 200:
                raise Exception("HTTP Error")

    # Scenario 1: All valid downloads
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = MockResponse(valid_jpeg)

        result_meta = await provider.download_content(
            "https://www.tiktok.com/@user/photo/7668360024648846599", metadata, tmp_path
        )
        assert Path(result_meta.items[0].local_path or "").exists()
        assert Path(result_meta.items[1].local_path or "").exists()
        assert Path(result_meta.items[0].local_path or "").name == "photo_001.jpg"

    # Scenario 2: Slide 2 fails -> temp files cleaned up
    job_dir_fail = tmp_path / "job_fail"
    job_dir_fail.mkdir()

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        # Slide 1 succeeds, slide 2 fails
        mock_get.side_effect = [MockResponse(valid_jpeg), Exception("Network error slide 2")]

        with pytest.raises(DownloadError) as exc_info:
            await provider.download_content(
                "https://www.tiktok.com/@user/photo/7668360024648846599", metadata, job_dir_fail
            )

        assert "Gagal mengunduh foto slide #2" in exc_info.value.message
        # Verify no photo files remain in job_dir_fail
        remaining = list(job_dir_fail.glob("photo_*"))
        assert len(remaining) == 0


@pytest.mark.asyncio
async def test_invalid_mime_and_size_limit(tmp_path: Path) -> None:
    provider = TikTokPhotoProvider()
    metadata = TikTokContentMetadata(
        content_type="photo",
        title="Test Slide",
        author="Tester",
        duration_seconds=0,
        items=[
            TikTokMediaItemMetadata(
                position=1,
                source_url="https://p16-sign-va.tiktokcdn.com/slide1.html",
                media_type="photo",
            ),
        ],
    )

    class MockResponse:
        def __init__(self, content: bytes) -> None:
            self.content = content

        def raise_for_status(self) -> None:
            pass

    # Invalid MIME (HTML string instead of image bytes)
    invalid_content = b"<html><body>Access Denied</body></html>"
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = MockResponse(invalid_content)

        with pytest.raises(DownloadError) as exc_info:
            await provider.download_content(
                "https://www.tiktok.com/@user/photo/7668360024648846599", metadata, tmp_path
            )
        assert "rusak atau bukan gambar valid" in exc_info.value.message

    # Exceed size limit
    oversized_jpeg = b"\xff\xd8\xff\xe0" + b"\x00" * (501 * 1024 * 1024)  # > 500MB
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = MockResponse(oversized_jpeg)

        with pytest.raises(DownloadSizeLimitExceededError) as exc_info:
            await provider.download_content(
                "https://www.tiktok.com/@user/photo/7668360024648846599", metadata, tmp_path
            )
        assert "melebihi batas unduhan" in exc_info.value.message
