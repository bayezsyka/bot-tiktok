import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from app.config import get_settings
from app.downloader.gallery_dl_instagram_post_provider import GalleryDlInstagramPostProvider
from app.downloader.gallery_dl_tiktok_photo_provider import (
    GalleryDlTikTokPhotoProvider,
    sanitize_stderr,
)
from app.downloader.instagram_provider import InstagramReelProvider
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.tiktok_photo_provider import TikTokPhotoProvider, _sanitize_error_message
from app.downloader.yt_dlp_provider import YtDlpProvider
from app.gateway.client import FarrosWAGatewayClient
from app.main import app
from app.security.urls import (
    resolve_canonical_media_url,
    resolve_canonical_tiktok_url,
)
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_empty_proxy_preserves_default_behavior() -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        settings.TIKTOK_PROXY_URL = ""

        # yt-dlp args do not have --proxy
        yt_provider = YtDlpProvider()
        assert "--proxy" not in yt_provider._get_base_args()

        # gallery-dl args do not have --proxy
        gdl_provider = GalleryDlTikTokPhotoProvider()
        fake_json = json.dumps([
            [2, {"id": "7668360024648846599", "user": "test_user"}],
            [3, "https://p16-common-sign.tiktokcdn.com/slide1.jpg", {"id": "7668360024648846599"}],
        ])
        with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
             patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate.return_value = (fake_json.encode("utf-8"), b"")
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            await gdl_provider.extract_metadata(
                "https://www.tiktok.com/@user/photo/7668360024648846599", Path("/tmp")
            )
            called_args = mock_exec.call_args[0]
            assert "--proxy" not in called_args
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


@pytest.mark.asyncio
async def test_tiktok_gallery_dl_receives_proxy(tmp_path: Path) -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        settings.TIKTOK_PROXY_URL = "http://user:secret123@proxy.local:8080"
        provider = GalleryDlTikTokPhotoProvider()

        fake_json = json.dumps([
            [2, {"id": "7668360024648846599", "user": "test_user"}],
            [3, "https://p16-common-sign.tiktokcdn.com/slide1.jpg", {"id": "7668360024648846599"}],
        ])

        with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
             patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate.return_value = (fake_json.encode("utf-8"), b"")
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            metadata = await provider.extract_metadata(
                "https://www.tiktok.com/@test_user/photo/7668360024648846599", tmp_path
            )

            assert metadata is not None
            assert len(metadata.items) == 1

            # Verify subprocess call args
            called_args = mock_exec.call_args[0]
            assert "--proxy" in called_args
            proxy_idx = called_args.index("--proxy")
            assert called_args[proxy_idx + 1] == "http://user:secret123@proxy.local:8080"
            # Verify shell=True was NOT used
            assert mock_exec.call_args[1].get("shell") is not True
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


@pytest.mark.asyncio
async def test_tiktok_yt_dlp_receives_proxy() -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        settings.TIKTOK_PROXY_URL = "http://proxy.local:8080"
        provider = YtDlpProvider()

        base_args = provider._get_base_args()
        assert "--proxy" in base_args
        idx = base_args.index("--proxy")
        assert base_args[idx + 1] == "http://proxy.local:8080"

        with patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate.return_value = (
                json.dumps({
                    "id": "7123456789012345678",
                    "title": "Test Video",
                    "duration": 30,
                    "url": "https://v16-webapp-prime.tiktokcdn.com/video.mp4",
                    "formats": [{"url": "https://v16-webapp-prime.tiktokcdn.com/video.mp4"}],
                }).encode("utf-8"),
                b"",
            )
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            metadata = await provider.extract_metadata(
                "https://www.tiktok.com/@user/video/7123456789012345678", Path("/tmp")
            )
            assert metadata is not None
            called_args = mock_exec.call_args[0]
            assert "--proxy" in called_args
            assert called_args[called_args.index("--proxy") + 1] == "http://proxy.local:8080"
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


@pytest.mark.asyncio
async def test_tiktok_canonical_resolver_uses_proxy() -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        settings.TIKTOK_PROXY_URL = "http://proxy.local:8080"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client_instance = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client_instance.head.return_value = mock_response

            res = await resolve_canonical_tiktok_url("https://www.tiktok.com/@user/video/1234567890123456789")
            assert res == "https://www.tiktok.com/@user/video/1234567890123456789"

            # Check that AsyncClient was initialized with proxy
            mock_client_cls.assert_called_once()
            _, kwargs = mock_client_cls.call_args
            assert kwargs.get("proxy") == "http://proxy.local:8080"
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


@pytest.mark.asyncio
async def test_tiktok_photo_provider_and_fallback_use_proxy() -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        settings.TIKTOK_PROXY_URL = "http://proxy.local:8080"
        provider = TikTokPhotoProvider()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client_instance = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = "<html><head><title>Test</title></head><body></body></html>"
            mock_client_instance.get.return_value = mock_response

            await provider._fetch_html("https://www.tiktok.com/@user/photo/7668360024648846599")

            mock_client_cls.assert_called_once()
            _, kwargs = mock_client_cls.call_args
            assert kwargs.get("proxy") == "http://proxy.local:8080"
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


@pytest.mark.asyncio
async def test_tiktok_media_download_uses_proxy(tmp_path: Path) -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        settings.TIKTOK_PROXY_URL = "http://proxy.local:8080"
        provider = GalleryDlTikTokPhotoProvider()

        metadata = TikTokContentMetadata(
            content_type="photo",
            title="Test",
            author="Author",
            duration_seconds=0,
            items=[
                TikTokMediaItemMetadata(
                    position=1,
                    source_url="https://p16-common-sign.tiktokcdn.com/slide1.jpg",
                    media_type="photo",
                )
            ],
        )

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client_instance = AsyncMock()
            mock_client_cls.return_value.__aenter__.return_value = mock_client_instance

            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.content = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 100
            mock_client_instance.get.return_value = mock_resp

            result = await provider.download_content(
                "https://www.tiktok.com/@user/photo/7668360024648846599", metadata, tmp_path
            )

            assert len(result.items) == 1
            mock_client_cls.assert_called_once()
            _, kwargs = mock_client_cls.call_args
            assert kwargs.get("proxy") == "http://proxy.local:8080"
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


@pytest.mark.asyncio
async def test_instagram_and_gateway_never_receive_proxy(tmp_path: Path) -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        settings.TIKTOK_PROXY_URL = "http://proxy.local:8080"

        # 1. Instagram Reels provider base args
        ig_reel = InstagramReelProvider()
        assert "--proxy" not in ig_reel._get_base_args()

        # 2. Instagram Post (gallery-dl) provider
        ig_post = GalleryDlInstagramPostProvider()
        with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
             patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate.return_value = (b"[]", b"")
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            try:
                await ig_post.extract_metadata("https://www.instagram.com/p/C123456/", tmp_path)
            except Exception:
                pass
            called_args = mock_exec.call_args[0]
            assert "--proxy" not in called_args

        # 3. Instagram canonical resolver
        canonical = await resolve_canonical_media_url("https://www.instagram.com/p/C123456/")
        assert canonical == "https://www.instagram.com/p/C123456/"

        # 4. Farros WA Gateway Client
        gw_client = FarrosWAGatewayClient()
        assert not hasattr(gw_client, "proxy")
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


def test_proxy_credentials_redaction() -> None:
    yt = YtDlpProvider()
    raw_yt_err = (
        "ERROR: Unable to download webpage: HTTP Error 403: Forbidden "
        "--proxy http://secret_user:super_secret_password@proxy.example.com:8080 "
        "https://secret_user:super_secret_password@proxy.example.com/api/test"
    )
    sanitized_yt = yt._sanitize_error(raw_yt_err)
    assert "super_secret_password" not in sanitized_yt
    assert "secret_user" not in sanitized_yt

    raw_gdl_err = (
        "[tiktok][error] Failed with --proxy http://myuser:mypassword@proxy.host:3128\n"
        "Connection refused to http://myuser:mypassword@proxy.host:3128"
    )
    sanitized_gdl = sanitize_stderr(raw_gdl_err)
    assert "mypassword" not in sanitized_gdl
    assert "myuser" not in sanitized_gdl

    raw_tt_err = "Request failed via http://user123:pass456@proxy.corp:8888/endpoint"
    sanitized_tt = _sanitize_error_message(raw_tt_err)
    assert "pass456" not in sanitized_tt
    assert "user123" not in sanitized_tt


@pytest.mark.asyncio
async def test_logs_only_contain_proxy_configured_boolean(caplog: pytest.LogCaptureFixture) -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL
    try:
        secret_proxy = "http://supersecretuser:supersecretpass@127.0.0.1:8888"
        settings.TIKTOK_PROXY_URL = secret_proxy
        provider = GalleryDlTikTokPhotoProvider()

        fake_json = json.dumps([
            [2, {"id": "7668360024648846599", "user": "test_user"}],
            [3, "https://p16-common-sign.tiktokcdn.com/slide1.jpg", {"id": "7668360024648846599"}],
        ])

        with patch("shutil.which", return_value="/usr/local/bin/gallery-dl"), \
             patch("asyncio.create_subprocess_exec", new_callable=AsyncMock) as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate.return_value = (fake_json.encode("utf-8"), b"")
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            with caplog.at_level(logging.INFO):
                await provider.extract_metadata(
                    "https://www.tiktok.com/@test_user/photo/7668360024648846599", Path("/tmp")
                )

            assert "proxy_configured=True" in caplog.text
            assert "supersecretuser" not in caplog.text
            assert "supersecretpass" not in caplog.text
            assert "8888" not in caplog.text
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy


@pytest.mark.asyncio
async def test_health_endpoint_exposes_proxy_status() -> None:
    settings = get_settings()
    original_proxy = settings.TIKTOK_PROXY_URL

    transport = ASGITransport(app=app)
    try:
        # 1. Direct / empty proxy
        settings.TIKTOK_PROXY_URL = ""
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("tiktok_proxy") == "direct"
            assert data.get("status") == "ok"

        # 2. Configured proxy
        settings.TIKTOK_PROXY_URL = "http://myuser:mypass@127.0.0.1:8080"
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("tiktok_proxy") == "configured"
            # Ensure secrets are never present anywhere in response
            assert "myuser" not in resp.text
            assert "mypass" not in resp.text
            assert "8080" not in resp.text
    finally:
        settings.TIKTOK_PROXY_URL = original_proxy
