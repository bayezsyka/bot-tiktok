import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.downloader.exceptions import (
    ContentNotSupportedError,
    DownloadError,
    DownloadTimeoutError,
)
from app.downloader.gallery_dl_instagram_post_provider import (
    GalleryDlInstagramPostProvider,
    parse_gallery_dl_instagram_post_json,
    sanitize_instagram_stderr,
)
from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def ig_post_sample_json() -> str:
    return (FIXTURES_DIR / "gallery_dl_instagram_post_sample.json").read_text(encoding="utf-8")


@pytest.fixture
def ig_carousel_photos_json() -> str:
    return (FIXTURES_DIR / "gallery_dl_instagram_carousel_photos_sample.json").read_text(encoding="utf-8")


@pytest.fixture
def ig_carousel_mixed_json() -> str:
    return (FIXTURES_DIR / "gallery_dl_instagram_carousel_mixed_sample.json").read_text(encoding="utf-8")


# --- Tests 11-17: Parsing ---

def test_parse_real_fixture_single_photo(ig_post_sample_json: str) -> None:
    metadata = parse_gallery_dl_instagram_post_json(
        ig_post_sample_json,
        "https://www.instagram.com/p/DbgFWkXMQXa/",
    )
    assert metadata is not None
    assert metadata.content_type == "photo"
    assert metadata.author == "Manchester United"
    assert len(metadata.items) == 1
    assert metadata.items[0].position == 1
    assert metadata.items[0].media_type == "photo"
    assert "fbcdn.net" in metadata.items[0].source_url


def test_parse_single_photo_produces_one_item(ig_post_sample_json: str) -> None:
    metadata = parse_gallery_dl_instagram_post_json(
        ig_post_sample_json,
        "https://www.instagram.com/p/DbgFWkXMQXa/",
    )
    assert metadata is not None
    assert len(metadata.items) == 1


def test_carousel_photos_preserve_order(ig_carousel_photos_json: str) -> None:
    metadata = parse_gallery_dl_instagram_post_json(
        ig_carousel_photos_json,
        "https://www.instagram.com/p/CarPh1a2b3/",
    )
    assert metadata is not None
    assert len(metadata.items) == 3
    assert metadata.content_type == "photo"
    for idx, item in enumerate(metadata.items, start=1):
        assert item.position == idx
        assert item.media_type == "photo"
        assert f"car{idx}_" in item.source_url


def test_mixed_carousel_preserves_type_order(ig_carousel_mixed_json: str) -> None:
    metadata = parse_gallery_dl_instagram_post_json(
        ig_carousel_mixed_json,
        "https://www.instagram.com/p/MixCar1X2Y/",
    )
    assert metadata is not None
    assert len(metadata.items) == 3
    assert metadata.content_type == "carousel"
    assert metadata.items[0].media_type == "photo"
    assert metadata.items[1].media_type == "video"
    assert metadata.items[2].media_type == "photo"


def test_duplicate_urls_deduplicated_stable() -> None:
    data = [
        [2, {"post_shortcode": "Dup1Abc", "type": "post"}],
        [3, "https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/dup_a.jpg", {"num": 1, "extension": "jpg", "video_url": None}],
        [3, "https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/dup_a.jpg", {"num": 2, "extension": "jpg", "video_url": None}],
        [3, "https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/dup_b.jpg", {"num": 3, "extension": "jpg", "video_url": None}],
    ]
    metadata = parse_gallery_dl_instagram_post_json(
        json.dumps(data),
        "https://www.instagram.com/p/Dup1Abc/",
    )
    assert metadata is not None
    assert len(metadata.items) == 2
    assert metadata.items[0].source_url.endswith("dup_a.jpg")
    assert metadata.items[1].source_url.endswith("dup_b.jpg")
    assert metadata.items[0].position == 1
    assert metadata.items[1].position == 2


def test_empty_output_not_success() -> None:
    data = [
        [2, {"post_shortcode": "Empty1", "type": "post"}],
    ]
    metadata = parse_gallery_dl_instagram_post_json(
        json.dumps(data),
        "https://www.instagram.com/p/Empty1/",
    )
    assert metadata is None


def test_malformed_json_raises_download_error() -> None:
    with pytest.raises(DownloadError) as exc_info:
        parse_gallery_dl_instagram_post_json(
            "not valid { json",
            "https://www.instagram.com/p/Bad1/",
        )
    assert "rusak" in exc_info.value.message.lower() or "tidak valid" in exc_info.value.message.lower()


def test_shortcode_mismatch_returns_none(ig_post_sample_json: str) -> None:
    metadata = parse_gallery_dl_instagram_post_json(
        ig_post_sample_json,
        "https://www.instagram.com/p/WRONGCODE/",
    )
    assert metadata is None


def test_sanitize_stderr_strips_cookies_and_paths() -> None:
    raw = (
        "[instagram][error] something\n"
        "Cookie loaded from /secret/path/cookies.txt sessionid=secret123\n"
        "csrf token=csrftoken_secret\n"
        "Failed to fetch https://cdn.example.com/img.jpg?token=signed_sig"
    )
    sanitized = sanitize_instagram_stderr(raw)
    assert "secret123" not in sanitized
    assert "csrftoken_secret" not in sanitized
    assert "/secret/path" not in sanitized
    assert "signed_sig" not in sanitized


# --- Tests 18-20: Cookie flag & subprocess ---

@pytest.mark.asyncio
async def test_cookie_option_sent_when_available(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File\n.instagram.com\tTRUE\t/\tFALSE\t1900000000\tsessionid\tabc\n")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            fake_json = json.dumps([
                [2, {"post_shortcode": "Cc1Test", "type": "post"}],
                [3, "https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/img1.jpg", {"num": 1, "extension": "jpg", "video_url": None}],
            ])
            return fake_json.encode("utf-8"), b""

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = MockProcess()
        metadata = await provider.extract_metadata("https://www.instagram.com/p/Cc1Test/", tmp_path)
        assert metadata is not None
        assert len(metadata.items) == 1

        call_args = mock_exec.call_args[0]
        assert "--cookies" in call_args
        assert str(cookie_file) in call_args
        assert "--dump-json" in call_args


@pytest.mark.asyncio
async def test_cookie_option_not_sent_when_missing(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    provider.settings.INSTAGRAM_COOKIES_FILE = str(tmp_path / "nonexistent.txt")

    class MockProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            fake_json = json.dumps([
                [2, {"post_shortcode": "Nc2Test", "type": "post"}],
                [3, "https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/img1.jpg", {"num": 1, "extension": "jpg", "video_url": None}],
            ])
            return fake_json.encode("utf-8"), b""

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = MockProcess()
        await provider.extract_metadata("https://www.instagram.com/p/Nc2Test/", tmp_path)
        call_args = mock_exec.call_args[0]
        assert "--cookies" not in call_args


@pytest.mark.asyncio
async def test_subprocess_does_not_use_shell(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            fake_json = json.dumps([
                [2, {"post_shortcode": "Sh1Test", "type": "post"}],
                [3, "https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/img1.jpg", {"num": 1, "extension": "jpg", "video_url": None}],
            ])
            return fake_json.encode("utf-8"), b""

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = MockProcess()
        await provider.extract_metadata("https://www.instagram.com/p/Sh1Test/", tmp_path)
        # create_subprocess_exec receives positional args (no shell=True, no string command)
        call_args = mock_exec.call_args[0]
        assert isinstance(call_args[0], str)
        assert call_args[0] == provider.settings.GALLERY_DL_BINARY


# --- Tests: Error classification ---

@pytest.mark.asyncio
async def test_binary_unavailable_raises_download_error(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    with patch("shutil.which", return_value=None), \
         patch("os.path.exists", return_value=False):
        with pytest.raises(DownloadError) as exc_info:
            await provider.extract_metadata("https://www.instagram.com/p/Bin1T/", tmp_path)
        assert "tidak tersedia" in exc_info.value.message.lower()


@pytest.mark.asyncio
async def test_login_required_raises_content_not_supported(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"ERROR: [instagram] Login required"

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockProcess()):
        with pytest.raises(ContentNotSupportedError):
            await provider.extract_metadata("https://www.instagram.com/p/Log1T/", tmp_path)


@pytest.mark.asyncio
async def test_private_post_raises_content_not_supported(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"ERROR: This post is private"

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockProcess()):
        with pytest.raises(ContentNotSupportedError):
            await provider.extract_metadata("https://www.instagram.com/p/Priv1T/", tmp_path)


@pytest.mark.asyncio
async def test_checkpoint_raises_content_not_supported(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"ERROR: Instagram checkpoint challenge required"

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockProcess()):
        with pytest.raises(ContentNotSupportedError):
            await provider.extract_metadata("https://www.instagram.com/p/Chk1T/", tmp_path)


@pytest.mark.asyncio
async def test_rate_limit_429_raises_download_error(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"ERROR: HTTP 429 Too Many Requests"

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockProcess()):
        with pytest.raises(DownloadError) as exc_info:
            await provider.extract_metadata("https://www.instagram.com/p/Rate1T/", tmp_path)
    assert "429" in exc_info.value.message


@pytest.mark.asyncio
async def test_forbidden_403_raises_download_error(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"ERROR: HTTP 403 Forbidden"

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockProcess()):
        with pytest.raises(DownloadError):
            await provider.extract_metadata("https://www.instagram.com/p/Forb1T/", tmp_path)


@pytest.mark.asyncio
async def test_deleted_post_raises_content_not_supported(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b"ERROR: Post not found / deleted"

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockProcess()):
        with pytest.raises(ContentNotSupportedError):
            await provider.extract_metadata("https://www.instagram.com/p/Del1T/", tmp_path)


@pytest.mark.asyncio
async def test_empty_output_exit_zero_raises_content_not_supported(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

    class MockProcess:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"[]", b""

    with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
         patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=MockProcess()):
        with pytest.raises(ContentNotSupportedError):
            await provider.extract_metadata("https://www.instagram.com/p/Empty2T/", tmp_path)


@pytest.mark.asyncio
async def test_timeout_raises_download_timeout(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    cookie_file = tmp_path / "ig_cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File")
    provider.settings.INSTAGRAM_COOKIES_FILE = str(cookie_file)

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
        with pytest.raises(DownloadTimeoutError):
            await provider.extract_metadata("https://www.instagram.com/p/Time1T/", tmp_path)
    assert mock_proc.terminated is True
    assert mock_proc.killed is True


# --- Tests: Download content ---

@pytest.mark.asyncio
async def test_download_photo_validates_cdn_and_magic_bytes(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    metadata = MediaContentMetadata(
        content_type="photo",
        title="Test",
        author="Tester",
        duration_seconds=0,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/test1.jpg",
                media_type="photo",
            )
        ],
    )
    valid_jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 100

    class MockResponse:
        content = valid_jpeg
        headers = {"content-type": "image/jpeg"}

        def raise_for_status(self) -> None:
            pass

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=MockResponse()):
        result = await provider.download_content("https://www.instagram.com/p/Dl1Test/", metadata, tmp_path)
        assert Path(result.items[0].local_path or "").exists()
        assert Path(result.items[0].local_path or "").name == "instagram_001.jpg"


@pytest.mark.asyncio
async def test_download_photo_rejects_non_cdn_host(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    metadata = MediaContentMetadata(
        content_type="photo",
        title="Test",
        author="Tester",
        duration_seconds=0,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://evil.com/notcdn.jpg",
                media_type="photo",
            )
        ],
    )
    with pytest.raises(DownloadError):
        await provider.download_content("https://www.instagram.com/p/Dl2Test/", metadata, tmp_path)


@pytest.mark.asyncio
async def test_download_photo_rejects_invalid_magic_bytes(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    metadata = MediaContentMetadata(
        content_type="photo",
        title="Test",
        author="Tester",
        duration_seconds=0,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/bad1.jpg",
                media_type="photo",
            )
        ],
    )

    class MockResponse:
        content = b"not an image at all"
        headers = {"content-type": "image/jpeg"}

        def raise_for_status(self) -> None:
            pass

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=MockResponse()):
        with pytest.raises(DownloadError):
            await provider.download_content("https://www.instagram.com/p/Dl3Test/", metadata, tmp_path)


@pytest.mark.asyncio
async def test_download_video_validates_mp4_magic(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    metadata = MediaContentMetadata(
        content_type="video",
        title="Test Video",
        author="Tester",
        duration_seconds=10,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://scontent.fsrg6-1.fna.fbcdn.net/v/t50.2886-16/vid1.mp4",
                media_type="video",
            )
        ],
    )
    valid_mp4 = b"\x00\x00\x00\x1cftypisom" + b"\x00" * 200

    class MockResponse:
        content = valid_mp4
        headers = {"content-type": "video/mp4"}

        def raise_for_status(self) -> None:
            pass

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=MockResponse()):
        result = await provider.download_content("https://www.instagram.com/p/Dl4Test/", metadata, tmp_path)
        assert Path(result.items[0].local_path or "").exists()
        assert Path(result.items[0].local_path or "").name == "instagram_001.mp4"


@pytest.mark.asyncio
async def test_download_cleans_up_partial_on_failure(tmp_path: Path) -> None:
    provider = GalleryDlInstagramPostProvider()
    metadata = MediaContentMetadata(
        content_type="photo",
        title="Test",
        author="Tester",
        duration_seconds=0,
        items=[
            MediaItemMetadata(
                position=1,
                source_url="https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/ok1.jpg",
                media_type="photo",
            ),
            MediaItemMetadata(
                position=2,
                source_url="https://instagram.fsrg6-1.fna.fbcdn.net/v/t51.82787-15/fail1.jpg",
                media_type="photo",
            ),
        ],
    )
    valid_jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 100

    class OkResponse:
        content = valid_jpeg
        headers = {"content-type": "image/jpeg"}

        def raise_for_status(self) -> None:
            pass

    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.side_effect = [OkResponse(), Exception("Network failure")]
        with pytest.raises(DownloadError):
            await provider.download_content("https://www.instagram.com/p/Dl5Test/", metadata, tmp_path)

    remaining = list(tmp_path.glob("instagram_*"))
    assert len(remaining) == 0
