import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import Admin
from app.database.repositories import JobRepository
from app.dependencies import get_current_admin, get_db
from app.downloader.dtos import JobDownloadSnapshot
from app.downloader.exceptions import (
    ContentNotSupportedError,
    DownloadError,
    TikTokChallengeError,
)
from app.downloader.gallery_dl_tiktok_photo_provider import GalleryDlTikTokPhotoProvider
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.service import DownloaderService
from app.downloader.yt_dlp_provider import YtDlpProvider
from app.main import app
from app.queue.worker import QueueWorker
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class MockYtDlpProcess:
    def __init__(self, returncode: int, stdout: bytes = b"", stderr: bytes = b"") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    async def communicate(self) -> tuple[bytes, bytes]:
        return self.stdout, self.stderr


def video_snapshot(*, canonical_url: str | None = None) -> JobDownloadSnapshot:
    url = canonical_url or "https://www.tiktok.com/@farhan_sukabola/video/7669081226115943700"
    return JobDownloadSnapshot(
        id="video-routing-job",
        original_url=url,
        canonical_url=canonical_url,
        platform="tiktok",
        items=(),
    )


def photo_metadata() -> TikTokContentMetadata:
    return TikTokContentMetadata(
        content_type="photo",
        title="Photo post",
        author="Creator",
        duration_seconds=0,
        items=[
            TikTokMediaItemMetadata(
                position=1,
                source_url="https://p16-common-sign.tiktokcdn.com/slide1.jpg",
                media_type="photo",
            )
        ],
    )


@pytest.mark.asyncio
async def test_canonical_video_ytdlp_failure_never_calls_photo_providers(tmp_path: Path) -> None:
    downloader = DownloaderService()
    original_error = DownloadError("ERROR: HTTP Error 403: Forbidden")

    with (
        patch.object(
            downloader.yt_dlp,
            "extract_metadata",
            new_callable=AsyncMock,
            side_effect=original_error,
        ) as mock_ytdlp,
        patch.object(
            downloader.gallery_dl, "extract_metadata", new_callable=AsyncMock
        ) as mock_gallery,
        patch.object(
            downloader.photo_provider, "extract_metadata", new_callable=AsyncMock
        ) as mock_html,
    ):
        with pytest.raises(DownloadError) as exc_info:
            await downloader.extract_metadata(
                video_snapshot(
                    canonical_url=(
                        "https://www.tiktok.com/@farhan_sukabola/video/7669081226115943700"
                        "?_r=1&_t=ZS-98WVwoVsynv"
                    )
                ),
                tmp_path,
            )

    assert exc_info.value is original_error
    mock_ytdlp.assert_awaited_once()
    mock_gallery.assert_not_awaited()
    mock_html.assert_not_awaited()


@pytest.mark.asyncio
async def test_unclassified_unsupported_url_can_fall_back_to_photo(tmp_path: Path) -> None:
    downloader = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="unclassified-photo",
        original_url="https://www.tiktok.com/t/ZExample/",
        canonical_url="https://www.tiktok.com/t/ZExample/",
        platform="tiktok",
        items=(),
    )
    metadata = photo_metadata()

    with (
        patch.object(
            downloader.yt_dlp, "extract_metadata", new_callable=AsyncMock, return_value=None
        ) as mock_ytdlp,
        patch.object(
            downloader.gallery_dl,
            "extract_metadata",
            new_callable=AsyncMock,
            return_value=metadata,
        ) as mock_gallery,
        patch.object(
            downloader.photo_provider, "extract_metadata", new_callable=AsyncMock
        ) as mock_html,
    ):
        result = await downloader.extract_metadata(snapshot, tmp_path)

    assert result.provider is downloader.gallery_dl
    mock_ytdlp.assert_awaited_once()
    mock_gallery.assert_awaited_once()
    mock_html.assert_not_awaited()


@pytest.mark.asyncio
async def test_ytdlp_challenge_on_video_raises_video_challenge(tmp_path: Path) -> None:
    provider = YtDlpProvider()
    process = MockYtDlpProcess(
        1,
        stderr=(
            b"ERROR: [TikTok] Sign in to confirm you are not a bot. "
            b"Fresh cookies are needed."
        ),
    )

    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=process):
        with pytest.raises(TikTokChallengeError) as exc_info:
            await provider.extract_metadata(
                "https://www.tiktok.com/@creator/video/7669081226115943700", tmp_path
            )

    assert "Sign in to confirm you are not a bot" in exc_info.value.message
    assert "video" in exc_info.value.message.lower()


@pytest.mark.asyncio
async def test_ytdlp_network_error_on_video_is_transient_download_error(tmp_path: Path) -> None:
    provider = YtDlpProvider()
    process = MockYtDlpProcess(
        1, stderr=b"ERROR: Unable to download webpage: HTTP Error 403: Forbidden"
    )

    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=process):
        with pytest.raises(DownloadError) as exc_info:
            await provider.extract_metadata(
                "https://www.tiktok.com/@creator/video/7669081226115943700", tmp_path
            )

    assert not isinstance(exc_info.value, ContentNotSupportedError)
    assert "HTTP Error 403" in exc_info.value.message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stderr", "friendly_fragment"),
    [
        (b"ERROR: Private video. Sign in if you've been granted access", "privat"),
        (b"ERROR: TikTok said this post is unavailable", "tidak tersedia"),
    ],
)
async def test_ytdlp_deleted_or_private_video_is_permanent(
    tmp_path: Path, stderr: bytes, friendly_fragment: str
) -> None:
    provider = YtDlpProvider()
    process = MockYtDlpProcess(1, stderr=stderr)

    with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=process):
        with pytest.raises(ContentNotSupportedError) as exc_info:
            await provider.extract_metadata(
                "https://www.tiktok.com/@creator/video/7669081226115943700", tmp_path
            )

    assert friendly_fragment in exc_info.value.user_friendly_message


def test_ytdlp_sanitizer_preserves_important_diagnostics() -> None:
    provider = YtDlpProvider()
    diagnostics = [
        "Sign in to confirm you are not a bot",
        "Fresh cookies are needed",
        "Video unavailable",
        "Private video",
        "HTTP Error 403",
        "Unsupported URL",
        "Unable to extract",
        "TikTok said this post is unavailable",
    ]

    sanitized = provider._sanitize_error("\n".join(f"ERROR: {item}" for item in diagnostics))

    for diagnostic in diagnostics:
        assert diagnostic in sanitized
    assert sanitized != "yt-dlp execution error"


def test_ytdlp_sanitizer_redacts_cookie_path_tokens_and_signed_urls() -> None:
    provider = YtDlpProvider()
    raw_error = (
        "Loading cookies from /Users/service/bot-tiktok/secrets/tiktok-cookies.txt\n"
        "Authorization: Bearer bearer-secret\n"
        "sessionid=top-secret token=token-secret\n"
        "ERROR: HTTP Error 403 at "
        "https://v16.tiktokcdn.com/video/path?signature=sig-secret&token=url-secret"
    )

    sanitized = provider._sanitize_error(raw_error)

    for secret in (
        "/Users/service/bot-tiktok/secrets/tiktok-cookies.txt",
        "bearer-secret",
        "top-secret",
        "token-secret",
        "sig-secret",
        "url-secret",
    ):
        assert secret not in sanitized
    assert "[REDACTED_PATH]" in sanitized
    assert "[REDACTED_SIGNED_URL]" in sanitized
    assert "HTTP Error 403" in sanitized


@pytest.mark.asyncio
async def test_gallery_dl_exit_zero_empty_video_returns_none(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    provider = GalleryDlTikTokPhotoProvider()
    process = MockYtDlpProcess(0, stdout=b"[]")

    with (
        patch("shutil.which", return_value="/usr/local/bin/gallery-dl"),
        patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=process),
    ):
        result = await provider.extract_metadata(
            "https://www.tiktok.com/@creator/video/7669081226115943700", tmp_path
        )

    assert result is None
    assert "result=empty" in caplog.text
    assert "extraction completed" not in caplog.text


@pytest.mark.asyncio
async def test_gallery_dl_exit_zero_empty_photo_is_not_success(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    provider = GalleryDlTikTokPhotoProvider()
    process = MockYtDlpProcess(0, stdout=json.dumps([]).encode())

    with (
        patch("shutil.which", return_value="/usr/local/bin/gallery-dl"),
        patch("asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=process),
    ):
        with pytest.raises(ContentNotSupportedError) as exc_info:
            await provider.extract_metadata(
                "https://www.tiktok.com/@creator/photo/7669081226115943700", tmp_path
            )

    assert "Slide foto TikTok tidak ditemukan" in exc_info.value.user_friendly_message
    assert "metadata_entries=0" in caplog.text
    assert "slide_count=0" in caplog.text
    assert "result=empty" in caplog.text


@pytest.mark.asyncio
async def test_video_network_error_enters_worker_retry(test_db: AsyncSession) -> None:
    session_maker = async_sessionmaker(
        bind=test_db.bind, class_=AsyncSession, expire_on_commit=False
    )
    canonical_url = "https://www.tiktok.com/@creator/video/7669081226115943700"
    async with session_maker() as session:
        job = await JobRepository(session).create_job(
            inbound_message_id="msg-video-network-retry",
            webhook_event_id="evt-video-network-retry",
            sender_number="628111222333",
            original_url=canonical_url,
            canonical_url=canonical_url,
        )
        await session.commit()
        job_id = job.id

    worker = QueueWorker(session_maker)
    with (
        patch(
            "app.downloader.service.YtDlpProvider.extract_metadata",
            new_callable=AsyncMock,
            side_effect=DownloadError("Network timeout while contacting TikTok"),
        ),
        patch("app.queue.worker.asyncio.sleep", new_callable=AsyncMock),
    ):
        await worker._process_job_safely(job_id)

    async with session_maker() as session:
        failed_attempt = await JobRepository(session).get_by_id(job_id)
        assert failed_attempt is not None
        assert failed_attempt.status == "queued"
        assert failed_attempt.error_code == "RETRY_SCHEDULED"


@pytest.mark.asyncio
async def test_canonical_is_saved_and_rendered_when_extraction_fails(
    test_db: AsyncSession,
) -> None:
    session_maker = async_sessionmaker(
        bind=test_db.bind, class_=AsyncSession, expire_on_commit=False
    )
    short_url = "https://vt.tiktok.com/ZS4B1DxAc/"
    canonical_url = "https://www.tiktok.com/@farhan_sukabola/video/7669081226115943700"
    async with session_maker() as session:
        job = await JobRepository(session).create_job(
            inbound_message_id="msg-save-canonical-failure",
            webhook_event_id="evt-save-canonical-failure",
            sender_number="628111222333",
            original_url=short_url,
            canonical_url=None,
        )
        await session.commit()
        job_id = job.id

    async def fail_after_canonical_is_committed(*_args: object) -> None:
        async with session_maker() as verification_session:
            persisted = await JobRepository(verification_session).get_by_id(job_id)
            assert persisted is not None
            assert persisted.canonical_url == canonical_url
        raise ContentNotSupportedError(
            "Video unavailable", user_friendly_message="Video TikTok tidak tersedia."
        )

    worker = QueueWorker(session_maker)
    with (
        patch(
            "app.downloader.service.DownloaderService.resolve_canonical_url",
            new_callable=AsyncMock,
            return_value=canonical_url,
        ),
        patch(
            "app.downloader.service.YtDlpProvider.extract_metadata",
            new_callable=AsyncMock,
            side_effect=fail_after_canonical_is_committed,
        ),
        patch.object(worker, "_send_failure_notification", new_callable=AsyncMock),
    ):
        await worker._process_job_safely(job_id)

    async with session_maker() as session:
        failed_job = await JobRepository(session).get_by_id(job_id)
        assert failed_job is not None
        assert failed_job.status == "failed"
        assert failed_job.canonical_url == canonical_url

    admin = Admin(id=91, username="canonical_admin", password_hash="hash", is_active=True)
    test_db.add(admin)
    await test_db.commit()

    async def mock_get_admin() -> Admin:
        return admin

    async def mock_get_db():
        yield test_db

    app.dependency_overrides[get_current_admin] = mock_get_admin
    app.dependency_overrides[get_db] = mock_get_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/admin/history/{job_id}")
        assert response.status_code == 200
        assert canonical_url in response.text
    finally:
        app.dependency_overrides.clear()
