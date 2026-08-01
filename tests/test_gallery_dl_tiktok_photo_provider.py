import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.downloader.exceptions import (
    DownloadError,
    DownloadTimeoutError,
    TikTokChallengeError,
)
from app.downloader.gallery_dl_tiktok_photo_provider import (
    GalleryDlTikTokPhotoProvider,
    parse_gallery_dl_tiktok_json,
    sanitize_stderr,
)
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata


@pytest.fixture
def gallery_dl_sample_json() -> str:
    fixture_path = Path(__file__).parent / "fixtures" / "gallery_dl_tiktok_photo_sample.json"
    return fixture_path.read_text(encoding="utf-8")


def test_parse_real_gallery_dl_json_fixture(gallery_dl_sample_json: str) -> None:
    metadata = parse_gallery_dl_tiktok_json(
        gallery_dl_sample_json,
        "https://www.tiktok.com/@ade_meliora/photo/7668360024648846599",
        expected_item_id="7668360024648846599",
    )
    assert metadata is not None
    assert metadata.content_type == "photo"
    assert metadata.author == "ade_meliora"
    assert "#stylish" in (metadata.title or "")
    assert len(metadata.items) == 7

    for idx, item in enumerate(metadata.items, start=1):
        assert item.position == idx
        assert item.media_type == "photo"
        assert f"slide{idx}_high.jpeg" in item.source_url


def test_item_id_mismatch(gallery_dl_sample_json: str) -> None:
    metadata = parse_gallery_dl_tiktok_json(
        gallery_dl_sample_json,
        "https://www.tiktok.com/@user/photo/9999999999999999999",
        expected_item_id="9999999999999999999",
    )
    assert metadata is None


def test_duplicate_urls_deduplicated_preserving_order() -> None:
    dup_data = [
        [2, {"id": "7668360024648846599", "user": "test"}],
        [3, "https://p16-common-sign.tiktokcdn.com/slide1.jpg", {"id": "7668360024648846599"}],
        [3, "https://p16-common-sign.tiktokcdn.com/slide1.jpg", {"id": "7668360024648846599"}],  # Duplicate
        [3, "https://p16-common-sign.tiktokcdn.com/slide2.jpg", {"id": "7668360024648846599"}],
    ]
    json_str = json.dumps(dup_data)
    metadata = parse_gallery_dl_tiktok_json(
        json_str,
        "https://www.tiktok.com/@user/photo/7668360024648846599",
        expected_item_id="7668360024648846599",
    )
    assert metadata is not None
    assert len(metadata.items) == 2
    assert metadata.items[0].source_url == "https://p16-common-sign.tiktokcdn.com/slide1.jpg"
    assert metadata.items[1].source_url == "https://p16-common-sign.tiktokcdn.com/slide2.jpg"


def test_covers_audio_video_ignored() -> None:
    data_with_extras = [
        [2, {"id": "7668360024648846599"}],
        [3, "https://p16-common-sign.tiktokcdn.com/slide1.jpg", {"id": "7668360024648846599"}],
        [3, "https://p16-common-sign.tiktokcdn.com/cover.jpg", {"id": "7668360024648846599"}],  # Ignored non-CDN or cover if domain invalid
        [3, "https://example.com/audio.mp3", {"id": "7668360024648846599"}],  # Non-allowed host domain
    ]
    metadata = parse_gallery_dl_tiktok_json(
        json.dumps(data_with_extras),
        "https://www.tiktok.com/@user/photo/7668360024648846599",
        expected_item_id="7668360024648846599",
    )
    assert metadata is not None
    assert len(metadata.items) == 1
    assert metadata.items[0].source_url == "https://p16-common-sign.tiktokcdn.com/slide1.jpg"


def test_malformed_json() -> None:
    with pytest.raises(DownloadError) as exc_info:
        parse_gallery_dl_tiktok_json(
            "invalid { json",
            "https://www.tiktok.com/@user/photo/7668360024648846599",
        )
    assert "Output metadata gallery-dl rusak" in exc_info.value.message


def test_sanitize_stderr() -> None:
    raw_stderr = (
        "[tiktok][error] Challenge page encountered\n"
        "Cookie loaded from /private/tmp/secret_path/cookies.txt with sessionid=secret_session\n"
        "Failed to fetch data from https://www.tiktok.com/api/item/detail/?itemId=123&signature=secret_sig"
    )
    sanitized = sanitize_stderr(raw_stderr)
    assert "secret_path" not in sanitized
    assert "secret_session" not in sanitized
    assert "secret_sig" not in sanitized
    assert "[tiktok][error] Challenge page encountered" in sanitized


@pytest.mark.asyncio
async def test_binary_available_and_command_no_shell(tmp_path: Path) -> None:
    provider = GalleryDlTikTokPhotoProvider()
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File\n.tiktok.com\tTRUE\t/\tFALSE\t1900000000\tfoo\tbar\n")
    provider.settings.TIKTOK_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            fake_json = json.dumps([
                [2, {"id": "7668360024648846599"}],
                [3, "https://p16-common-sign.tiktokcdn.com/photo1.jpg", {"id": "7668360024648846599"}]
            ])
            return fake_json.encode("utf-8"), b""

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = MockProcess()

        metadata = await provider.extract_metadata("https://www.tiktok.com/@user/photo/7668360024648846599", tmp_path)
        assert metadata is not None
        assert len(metadata.items) == 1

        # Verify command arguments passed as array (not shell string interpolation!)
        call_args = mock_exec.call_args[0]
        assert call_args[0] == provider.settings.GALLERY_DL_BINARY
        assert "--dump-json" in call_args
        assert "--cookies" in call_args
        assert str(cookie_file) in call_args
        assert "https://www.tiktok.com/@user/photo/7668360024648846599" in call_args


@pytest.mark.asyncio
async def test_cookies_not_sent_when_file_missing(tmp_path: Path) -> None:
    provider = GalleryDlTikTokPhotoProvider()
    provider.settings.TIKTOK_COOKIES_FILE = str(tmp_path / "non_existent_cookies.txt")

    class MockProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            fake_json = json.dumps([
                [2, {"id": "7668360024648846599"}],
                [3, "https://p16-common-sign.tiktokcdn.com/photo1.jpg", {"id": "7668360024648846599"}]
            ])
            return fake_json.encode("utf-8"), b""

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = MockProcess()

        await provider.extract_metadata("https://www.tiktok.com/@user/photo/7668360024648846599", tmp_path)
        call_args = mock_exec.call_args[0]
        assert "--cookies" not in call_args


@pytest.mark.asyncio
async def test_gallery_dl_binary_unavailable(tmp_path: Path) -> None:
    provider = GalleryDlTikTokPhotoProvider()
    with patch("shutil.which", return_value=None), \
         patch("os.path.exists", return_value=False):
        res = await provider.extract_metadata("https://www.tiktok.com/@user/photo/7668360024648846599", tmp_path)
        assert res is None


@pytest.mark.asyncio
async def test_subprocess_timeout_terminate_and_kill(tmp_path: Path) -> None:
    provider = GalleryDlTikTokPhotoProvider()

    class MockTimedOutProcess:
        def __init__(self) -> None:
            self.terminated = False
            self.killed = False
            self.returncode = None

        async def communicate(self) -> tuple[bytes, bytes]:
            raise TimeoutError("Subprocess timeout")

        def terminate(self) -> None:
            self.terminated = True

        async def wait(self) -> None:
            raise TimeoutError()

        def kill(self) -> None:
            self.killed = True

    mock_proc = MockTimedOutProcess()
    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
        with pytest.raises(DownloadTimeoutError) as exc_info:
            await provider.extract_metadata("https://www.tiktok.com/@user/photo/7668360024648846599", tmp_path)

        assert "Waktu pengunduhan metadata" in exc_info.value.message
        assert mock_proc.terminated is True
        assert mock_proc.killed is True


@pytest.mark.asyncio
async def test_challenge_error_raises_tiktok_challenge_error(tmp_path: Path) -> None:
    provider = GalleryDlTikTokPhotoProvider()

    class MockChallengeProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"[tiktok][error] Challenge captcha required (HTTP 403)"

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockChallengeProcess()):
        with pytest.raises(TikTokChallengeError) as exc_info:
            await provider.extract_metadata("https://www.tiktok.com/@user/photo/7668360024648846599", tmp_path)

        assert "TikTok sementara menolak akses downloader" in exc_info.value.user_friendly_message


@pytest.mark.asyncio
async def test_download_slides_and_cleanup_on_single_failure(tmp_path: Path) -> None:
    provider = GalleryDlTikTokPhotoProvider()
    metadata = TikTokContentMetadata(
        content_type="photo",
        title="Test Slides",
        author="Tester",
        duration_seconds=0,
        items=[
            TikTokMediaItemMetadata(position=1, source_url="https://p16-common-sign.tiktokcdn.com/slide1.jpg", media_type="photo"),
            TikTokMediaItemMetadata(position=2, source_url="https://p16-common-sign.tiktokcdn.com/slide2.jpg", media_type="photo"),
        ],
    )

    valid_jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 100

    class MockResponse:
        def __init__(self, content: bytes, status_code: int = 200) -> None:
            self.content = content
            self.status_code = status_code

        def raise_for_status(self) -> None:
            if self.status_code != 200:
                raise Exception("HTTP error")

    # All valid downloads
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = MockResponse(valid_jpeg)
        result = await provider.download_content("https://www.tiktok.com/@user/photo/7668360024648846599", metadata, tmp_path)
        assert Path(result.items[0].local_path or "").exists()
        assert Path(result.items[1].local_path or "").exists()
        assert Path(result.items[0].local_path or "").name == "photo_001.jpg"

    # Single slide failure -> all temp files cleaned up
    job_dir_fail = tmp_path / "job_fail"
    job_dir_fail.mkdir()
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = [MockResponse(valid_jpeg), Exception("Slide 2 failed")]
        with pytest.raises(DownloadError) as exc_info:
            await provider.download_content("https://www.tiktok.com/@user/photo/7668360024648846599", metadata, job_dir_fail)

        assert "Gagal mengunduh foto slide #2" in exc_info.value.message
        remaining = list(job_dir_fail.glob("photo_*"))
        assert len(remaining) == 0
