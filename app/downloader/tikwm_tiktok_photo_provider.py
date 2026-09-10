import logging
import re
import time
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings
from app.downloader.exceptions import (
    DownloadError,
    DownloadSizeLimitExceededError,
    DownloadTimeoutError,
)
from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata
from app.downloader.providers import DownloaderProvider
from app.downloader.tiktok_photo_provider import extract_item_id_from_url

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}


def _sanitize_error_message(err_text: str) -> str:
    if not err_text:
        return ""
    sanitized = re.sub(r"(?i)https?://[^:@\s/]+:[^@\s/]+@[^\s<>\"']+", "[REDACTED_PROXY]", err_text)
    sanitized = re.sub(r"(?i)(--proxy\s+)[^\s]+", r"\1[REDACTED]", sanitized)
    return sanitized


class TikwmTikTokPhotoProvider(DownloaderProvider):
    def __init__(self) -> None:
        self.settings = get_settings()

    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        metadata = await self.extract_metadata(canonical_url, job_dir)
        return bool(metadata and metadata.content_type == "photo" and len(metadata.items) > 0)

    async def extract_metadata(
        self, canonical_url: str, job_dir: Path
    ) -> MediaContentMetadata | None:
        item_id = extract_item_id_from_url(canonical_url) or "unknown"
        api_url = getattr(self.settings, "TIKWM_API_URL", "https://www.tikwm.com/api/")
        if not api_url:
            return None

        proxy = self.settings.TIKTOK_PROXY_URL or None
        timeout = min(float(self.settings.JOB_TIMEOUT_SECONDS), 30.0)
        start_time = time.monotonic()

        params = {
            "url": canonical_url,
            "hd": "1",
        }

        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=True,
                headers=DEFAULT_HEADERS,
                verify=True,
                proxy=proxy,
            ) as client:
                resp = await client.get(api_url, params=params)
                resp.raise_for_status()
                data = resp.json()
        except httpx.TimeoutException as e:
            elapsed = time.monotonic() - start_time
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=photo result=timeout "
                f"item_id={item_id} image_count=0 elapsed_seconds={elapsed:.2f}"
            )
            raise DownloadTimeoutError(
                f"Waktu permintaan TikWM habis ({int(timeout)}s).",
                user_friendly_message="Waktu pengambilan data media TikTok habis.",
            ) from e
        except Exception as e:
            elapsed = time.monotonic() - start_time
            clean_err = _sanitize_error_message(str(e))
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=photo result=failure "
                f"item_id={item_id} image_count=0 error={clean_err} elapsed_seconds={elapsed:.2f}"
            )
            return None

        elapsed = time.monotonic() - start_time

        if not isinstance(data, dict):
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=photo result=invalid_response "
                f"item_id={item_id} image_count=0 elapsed_seconds={elapsed:.2f}"
            )
            return None

        code = data.get("code")
        if code != 0:
            msg = str(data.get("msg") or f"code_{code}")
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=photo result=failure "
                f"item_id={item_id} image_count=0 code={code} msg={msg} elapsed_seconds={elapsed:.2f}"
            )
            return None

        payload: dict[str, Any] = data.get("data") or {}
        if not isinstance(payload, dict):
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=photo result=missing_data "
                f"item_id={item_id} image_count=0 elapsed_seconds={elapsed:.2f}"
            )
            return None

        raw_images = payload.get("images")
        if not isinstance(raw_images, list) or len(raw_images) == 0:
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=photo result=empty "
                f"item_id={item_id} image_count=0 elapsed_seconds={elapsed:.2f}"
            )
            return None

        # Collect valid photo URLs preserving order and deduplicating
        valid_urls: list[str] = []
        seen: set[str] = set()
        for u in raw_images:
            if isinstance(u, str) and u.startswith("http") and u not in seen:
                seen.add(u)
                valid_urls.append(u)

        if not valid_urls:
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=photo result=no_valid_urls "
                f"item_id={item_id} image_count=0 elapsed_seconds={elapsed:.2f}"
            )
            return None

        title = str(payload.get("title") or "TikTok Photo Post")[:200]
        author_info = payload.get("author") or {}
        author = "TikTok Creator"
        if isinstance(author_info, dict):
            author = str(
                author_info.get("nickname")
                or author_info.get("unique_id")
                or "TikTok Creator"
            )

        items = [
            MediaItemMetadata(
                position=idx + 1,
                source_url=url,
                media_type="photo",
            )
            for idx, url in enumerate(valid_urls)
        ]

        # Extract background music / sound if present
        music_url = payload.get("music") or payload.get("play") or None
        music_duration = 0
        music_info = payload.get("music_info")
        if isinstance(music_info, dict):
            music_duration = int(music_info.get("duration") or 0)
        elif isinstance(payload.get("duration"), (int, float)):
            music_duration = int(payload.get("duration") or 0)

        logger.info(
            f"provider=tikwm platform=tiktok content_type=photo result=success "
            f"item_id={item_id} image_count={len(items)} has_music={bool(music_url)} elapsed_seconds={elapsed:.2f}"
        )

        return MediaContentMetadata(
            content_type="photo",
            title=title,
            author=author,
            duration_seconds=music_duration,
            music_url=music_url,
            items=items,
        )

    async def download_content(
        self, canonical_url: str, metadata: MediaContentMetadata, job_dir: Path
    ) -> MediaContentMetadata:
        if not metadata.items:
            raise DownloadError("Foto tidak ditemukan pada postingan ini.")

        headers = {
            **DEFAULT_HEADERS,
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        }

        total_slideshow_bytes = 0
        max_bytes = self.settings.MAX_SOURCE_DOWNLOAD_MB * 1024 * 1024
        proxy = self.settings.TIKTOK_PROXY_URL or None

        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers=headers,
            verify=True,
            proxy=proxy,
        ) as client:
            for item in metadata.items:
                try:
                    resp = await client.get(item.source_url)
                    resp.raise_for_status()
                    content = resp.content
                except Exception as e:
                    self._cleanup_downloaded_photos(job_dir)
                    sanitized_err = _sanitize_error_message(str(e)) or "Pengunduhan slide foto terganggu."
                    raise DownloadError(
                        f"Gagal mengunduh foto slide #{item.position}: {sanitized_err}",
                        user_friendly_message="Gagal mengunduh file media. Pengunduhan slide foto terganggu.",
                    ) from e

                total_slideshow_bytes += len(content)
                if len(content) > max_bytes or total_slideshow_bytes > max_bytes:
                    self._cleanup_downloaded_photos(job_dir)
                    raise DownloadSizeLimitExceededError(
                        f"Ukuran foto slide #{item.position} melebihi batas unduhan.",
                        user_friendly_message="Ukuran foto melebihi batas maksimal unduhan.",
                    )

                ext = self._detect_image_extension(content)
                if not ext:
                    self._cleanup_downloaded_photos(job_dir)
                    raise DownloadError(
                        f"File foto slide #{item.position} rusak atau bukan gambar valid.",
                        user_friendly_message="File media rusak atau format gambar tidak didukung.",
                    )

                local_filename = job_dir / f"photo_{item.position:03d}.{ext}"
                try:
                    with open(local_filename, "wb") as f:
                        f.write(content)
                except Exception as e:
                    self._cleanup_downloaded_photos(job_dir)
                    raise DownloadError(f"Gagal menyimpan file foto slide #{item.position}: {e}") from e

                item.local_path = str(local_filename.resolve())

        return metadata

    def _detect_image_extension(self, content: bytes) -> str | None:
        if content.startswith(b"\xff\xd8\xff"):
            return "jpg"
        if content.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"
        if content[:4] == b"RIFF" and content[8:12] == b"WEBP":
            return "webp"
        if content.startswith(b"GIF8"):
            return "gif"
        if content.startswith(b"\x00\x00\x00 ftypavif") or content.startswith(b"\x00\x00\x00\x1cftypmif1"):
            return "avif"
        return None

    def _cleanup_downloaded_photos(self, job_dir: Path) -> None:
        for f in job_dir.glob("photo_*"):
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass
