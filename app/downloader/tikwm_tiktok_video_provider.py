import logging
import re
import time
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings
from app.downloader.exceptions import (
    ContentNotSupportedError,
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


class TikwmTikTokVideoProvider(DownloaderProvider):
    def __init__(self) -> None:
        self.settings = get_settings()

    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        metadata = await self.extract_metadata(canonical_url, job_dir)
        return bool(metadata and metadata.content_type == "video" and len(metadata.items) > 0)

    async def _fetch_fresh_video_url(self, canonical_url: str) -> str | None:
        """Re-fetch a fresh signed CDN URL from TikWM API. Used when the cached URL expires."""
        item_id = extract_item_id_from_url(canonical_url) or "unknown"
        api_url = getattr(self.settings, "TIKWM_API_URL", "https://www.tikwm.com/api/")
        if not api_url:
            return None

        proxy = self.settings.TIKTOK_PROXY_URL or None
        timeout = 30.0

        params = {"url": canonical_url, "hd": "1"}

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
        except Exception as e:
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=video result=refresh_failure "
                f"item_id={item_id} error={_sanitize_error_message(str(e))}"
            )
            return None

        if not isinstance(data, dict) or data.get("code") != 0:
            return None

        payload: dict[str, Any] = data.get("data") or {}
        if not isinstance(payload, dict):
            return None

        video_url = payload.get("hdplay") or payload.get("play") or payload.get("wmplay")
        if not isinstance(video_url, str) or not video_url.startswith("http"):
            return None

        logger.info(
            f"provider=tikwm platform=tiktok content_type=video result=url_refreshed item_id={item_id}"
        )
        return video_url

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
                f"provider=tikwm platform=tiktok content_type=video result=timeout "
                f"item_id={item_id} elapsed_seconds={elapsed:.2f}"
            )
            raise DownloadTimeoutError(
                f"Waktu permintaan TikWM video habis ({int(timeout)}s).",
                user_friendly_message="Waktu pengambilan video TikTok habis.",
            ) from e
        except Exception as e:
            elapsed = time.monotonic() - start_time
            clean_err = _sanitize_error_message(str(e))
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=video result=failure "
                f"item_id={item_id} error={clean_err} elapsed_seconds={elapsed:.2f}"
            )
            return None

        elapsed = time.monotonic() - start_time

        if not isinstance(data, dict):
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=video result=invalid_response "
                f"item_id={item_id} elapsed_seconds={elapsed:.2f}"
            )
            return None

        code = data.get("code")
        if code != 0:
            msg = str(data.get("msg") or f"code_{code}")
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=video result=failure "
                f"item_id={item_id} code={code} msg={msg} elapsed_seconds={elapsed:.2f}"
            )
            return None

        payload: dict[str, Any] = data.get("data") or {}
        if not isinstance(payload, dict):
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=video result=missing_data "
                f"item_id={item_id} elapsed_seconds={elapsed:.2f}"
            )
            return None

        # Prefer hdplay, fallback to play or wmplay
        video_url = payload.get("hdplay") or payload.get("play") or payload.get("wmplay")
        if not isinstance(video_url, str) or not video_url.startswith("http"):
            logger.warning(
                f"provider=tikwm platform=tiktok content_type=video result=empty "
                f"item_id={item_id} elapsed_seconds={elapsed:.2f}"
            )
            return None

        duration = int(payload.get("duration") or 0)
        if duration > self.settings.MAX_VIDEO_DURATION_SECONDS:
            raise ContentNotSupportedError(
                f"Durasi video melebihi batas {self.settings.MAX_VIDEO_DURATION_SECONDS} detik.",
                user_friendly_message="Durasi video terlalu panjang melebihi batas maksimal.",
            )

        title = str(payload.get("title") or "TikTok Video")[:200]
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
                position=1,
                source_url=video_url,
                media_type="video",
            )
        ]

        logger.info(
            f"provider=tikwm platform=tiktok content_type=video result=success "
            f"item_id={item_id} elapsed_seconds={elapsed:.2f}"
        )

        return MediaContentMetadata(
            content_type="video",
            title=title,
            author=author,
            duration_seconds=duration,
            items=items,
        )

    async def _stream_video_url(
        self,
        video_url: str,
        output_file: Path,
        max_bytes: int,
        proxy: str | None,
        headers: dict,
    ) -> int:
        """Stream video from URL to output_file. Returns total bytes streamed. Raises on error."""
        async with httpx.AsyncClient(
            timeout=float(self.settings.JOB_TIMEOUT_SECONDS),
            follow_redirects=True,
            headers=headers,
            verify=True,
            proxy=proxy,
        ) as client:
            async with client.stream("GET", video_url) as resp:
                resp.raise_for_status()

                content_type = resp.headers.get("content-type", "").lower()
                if "text/html" in content_type or "application/json" in content_type:
                    raise DownloadError("Server TikWM mengembalikan response non-video.")

                content_length_str = resp.headers.get("content-length")
                if content_length_str and content_length_str.isdigit():
                    if int(content_length_str) > max_bytes:
                        raise DownloadSizeLimitExceededError(
                            "Ukuran video TikWM melebihi batas maksimal.",
                            user_friendly_message="Ukuran video asli melebihi batas maksimal unduhan.",
                        )

                total_streamed = 0
                first_chunk = True
                with open(output_file, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        if not chunk:
                            continue
                        if first_chunk:
                            first_chunk = False
                            header = chunk[:64].lstrip()
                            if header.startswith(
                                (b"<!DOCTYPE", b"<!doctype", b"<html", b"<HTML", b'{"', b"{'")
                            ):
                                raise DownloadError(
                                    "File video hasil unduhan bukan stream video yang valid."
                                )
                        total_streamed += len(chunk)
                        if total_streamed > max_bytes:
                            raise DownloadSizeLimitExceededError(
                                "Ukuran video TikWM melebihi batas maksimal.",
                                user_friendly_message="Ukuran video asli melebihi batas maksimal unduhan.",
                            )
                        f.write(chunk)

                if total_streamed == 0:
                    raise DownloadError("File video hasil unduhan kosong atau tidak ditemukan.")

                return total_streamed

    async def download_content(
        self, canonical_url: str, metadata: MediaContentMetadata, job_dir: Path
    ) -> MediaContentMetadata:
        if not metadata.items:
            raise DownloadError("Metadata video tidak valid.")

        output_file = job_dir / "video_source.mp4"
        max_bytes = self.settings.MAX_SOURCE_DOWNLOAD_MB * 1024 * 1024
        proxy = self.settings.TIKTOK_PROXY_URL or None

        headers = {
            **DEFAULT_HEADERS,
            "Accept": "video/mp4,video/*,*/*;q=0.8",
        }

        video_url = metadata.items[0].source_url

        try:
            await self._stream_video_url(video_url, output_file, max_bytes, proxy, headers)
        except (DownloadSizeLimitExceededError, ContentNotSupportedError):
            self._cleanup_downloaded_videos(job_dir)
            raise
        except httpx.HTTPStatusError as e:
            # CDN URL expired (404) or geo-blocked (403) — re-fetch a fresh URL from TikWM
            status_code = e.response.status_code
            if status_code in (403, 404):
                logger.warning(
                    f"provider=tikwm platform=tiktok content_type=video result=cdn_expired "
                    f"status={status_code} url={canonical_url} — refreshing CDN URL"
                )
                self._cleanup_downloaded_videos(job_dir)
                fresh_url = await self._fetch_fresh_video_url(canonical_url)
                if fresh_url and fresh_url != video_url:
                    try:
                        await self._stream_video_url(fresh_url, output_file, max_bytes, proxy, headers)
                        logger.info(
                            f"provider=tikwm platform=tiktok content_type=video result=cdn_refresh_success "
                            f"url={canonical_url}"
                        )
                    except (DownloadSizeLimitExceededError, ContentNotSupportedError):
                        self._cleanup_downloaded_videos(job_dir)
                        raise
                    except Exception as retry_err:
                        self._cleanup_downloaded_videos(job_dir)
                        sanitized = _sanitize_error_message(str(retry_err))
                        raise DownloadError(
                            f"Gagal mengunduh video TikWM setelah refresh URL: {sanitized}",
                            user_friendly_message="Terjadi gangguan saat mengunduh file video.",
                        ) from retry_err
                else:
                    raise DownloadError(
                        f"URL CDN video TikWM expired (HTTP {status_code}) dan gagal mendapat URL baru.",
                        user_friendly_message="Terjadi gangguan saat mengunduh file video.",
                    ) from e
            else:
                self._cleanup_downloaded_videos(job_dir)
                sanitized = _sanitize_error_message(str(e))
                raise DownloadError(
                    f"Gagal mengunduh video TikWM: {sanitized}",
                    user_friendly_message="Terjadi gangguan saat mengunduh file video.",
                ) from e
        except DownloadError:
            self._cleanup_downloaded_videos(job_dir)
            raise
        except Exception as e:
            self._cleanup_downloaded_videos(job_dir)
            sanitized = _sanitize_error_message(str(e))
            raise DownloadError(
                f"Gagal mengunduh video TikWM: {sanitized}",
                user_friendly_message="Terjadi gangguan saat mengunduh file video.",
            ) from e

        if not output_file.exists() or output_file.stat().st_size == 0:
            self._cleanup_downloaded_videos(job_dir)
            raise DownloadError("File video hasil unduhan kosong atau tidak ditemukan.")

        metadata.items[0].local_path = str(output_file.resolve())
        return metadata

    def _cleanup_downloaded_videos(self, job_dir: Path) -> None:
        for f in job_dir.glob("video_source*"):
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass
