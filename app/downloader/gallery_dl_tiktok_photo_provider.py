import asyncio
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from app.config import get_settings
from app.downloader.exceptions import (
    ContentNotSupportedError,
    DownloadError,
    DownloadSizeLimitExceededError,
    DownloadTimeoutError,
    TikTokChallengeError,
)
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.providers import DownloaderProvider
from app.downloader.tiktok_photo_provider import (
    DEFAULT_TIKTOK_HEADERS,
    _is_allowed_image_url,
    _load_netscape_cookies,
    extract_item_id_from_url,
)

logger = logging.getLogger(__name__)


def sanitize_stderr(stderr_text: str) -> str:
    """Sanitize subprocess stderr to remove sensitive cookies, file paths, proxy credentials, or signature tokens."""
    if not stderr_text:
        return ""
    lines = stderr_text.strip().split("\n")
    clean_lines = []
    for line in lines:
        if "cookie" in line.lower() or "/" in line or "\\" in line or "@" in line:
            # Skip lines exposing internal paths, cookie info, or credentials
            continue
        clean_lines.append(line.strip())
    clean_message = " - ".join(clean_lines[:2]) or "gallery-dl execution error"
    clean_message = re.sub(r"(?i)https?://[^:@\s/]+:[^@\s/]+@[^\s<>\"']+", "[REDACTED_PROXY]", clean_message)
    clean_message = re.sub(r"(?i)(--proxy\s+)[^\s]+", r"\1[REDACTED]", clean_message)
    return clean_message


def _is_valid_slide_url(url: str) -> bool:
    if not _is_allowed_image_url(url):
        return False
    url_lower = url.lower()
    if any(ext in url_lower for ext in (".mp3", ".mp4", ".m4a", ".wav", ".aac")):
        return False
    if "/cover" in url_lower or "cover.jpg" in url_lower or "cover.png" in url_lower:
        return False
    return True


def parse_gallery_dl_tiktok_json(
    raw_json_str: str, canonical_url: str, expected_item_id: str | None = None
) -> TikTokContentMetadata | None:
    """
    Parse JSON output from gallery-dl 1.32.8 --dump-json.
    Format: list of entries where element 0 is type code (2 for metadata, 3 for image items).
    """
    if not raw_json_str or not raw_json_str.strip():
        return None

    try:
        data = json.loads(raw_json_str)
    except Exception as e:
        logger.error(f"Failed to parse gallery-dl JSON output: {e}")
        raise DownloadError("Output metadata gallery-dl rusak atau tidak valid.") from e

    if not isinstance(data, list):
        return None

    raw_urls: list[str] = []
    author = "TikTok Creator"
    title = "TikTok Photo Post"
    extracted_item_id: str | None = None

    for entry in data:
        if not isinstance(entry, list) or len(entry) < 2:
            continue

        type_code = entry[0]
        meta: dict[str, Any] = {}

        if type_code in (1, 3) and len(entry) >= 2:
            url = entry[1]
            if isinstance(url, str) and url.startswith("http") and _is_valid_slide_url(url):
                raw_urls.append(url)
            if len(entry) >= 3 and isinstance(entry[2], dict):
                meta = entry[2]
        elif type_code == 2 and isinstance(entry[1], dict):
            meta = entry[1]

        if meta:
            if not extracted_item_id:
                m_id = str(meta.get("id") or meta.get("aweme_id") or meta.get("video", {}).get("id") or "")
                if m_id and m_id.isdigit():
                    extracted_item_id = m_id
            if author == "TikTok Creator":
                a_name = str(
                    meta.get("user")
                    or meta.get("author", {}).get("nickname")
                    or meta.get("author", {}).get("uniqueId")
                    or ""
                )
                if a_name:
                    author = a_name
            if title == "TikTok Photo Post":
                t_val = str(meta.get("title") or meta.get("desc") or "")
                if t_val:
                    title = t_val[:200]

    # Verify item ID match if expected_item_id was supplied
    target_id = expected_item_id or extract_item_id_from_url(canonical_url)
    if target_id and extracted_item_id and extracted_item_id != target_id:
        logger.warning(
            f"gallery-dl item ID mismatch: extracted {extracted_item_id} != expected {target_id}"
        )
        return None

    # Deduplicate URLs while preserving slide order
    seen: set[str] = set()
    ordered_urls: list[str] = []
    for u in raw_urls:
        if u not in seen:
            seen.add(u)
            ordered_urls.append(u)

    if not ordered_urls:
        return None

    items = [
        TikTokMediaItemMetadata(
            position=idx + 1,
            source_url=url,
            media_type="photo",
        )
        for idx, url in enumerate(ordered_urls)
    ]

    return TikTokContentMetadata(
        content_type="photo",
        title=title,
        author=author,
        duration_seconds=0,
        items=items,
    )


class GalleryDlTikTokPhotoProvider(DownloaderProvider):
    def __init__(self) -> None:
        self.settings = get_settings()

    def _is_binary_available(self) -> bool:
        binary_path = self.settings.GALLERY_DL_BINARY
        if os.path.isabs(binary_path):
            return os.path.exists(binary_path) and os.path.isfile(binary_path)
        return shutil.which(binary_path) is not None

    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        metadata = await self.extract_metadata(canonical_url, job_dir)
        return bool(metadata and metadata.content_type == "photo" and len(metadata.items) > 0)

    async def extract_metadata(self, canonical_url: str, job_dir: Path) -> TikTokContentMetadata | None:
        item_id = extract_item_id_from_url(canonical_url) or "unknown"

        if not self._is_binary_available():
            logger.error("GALLERY_DL_UNAVAILABLE: gallery-dl binary not found in PATH")
            return None

        cookies_file = self.settings.TIKTOK_COOKIES_FILE
        cookie_configured = bool(
            cookies_file and os.path.exists(cookies_file) and os.path.isfile(cookies_file)
        )

        args = [
            self.settings.GALLERY_DL_BINARY,
            "--dump-json",
            "-o",
            "extractor.tiktok.photos=true",
            "-o",
            "extractor.tiktok.videos=false",
            "-o",
            "extractor.tiktok.audio=false",
            "-o",
            "extractor.tiktok.covers=false",
        ]
        if self.settings.TIKTOK_PROXY_URL:
            args.extend(["--proxy", self.settings.TIKTOK_PROXY_URL])
        if cookie_configured:
            args.extend(["--cookies", cookies_file])
        args.append(canonical_url)

        timeout = min(float(self.settings.JOB_TIMEOUT_SECONDS), 240.0)
        start_time = time.monotonic()
        challenge_detected = False
        exit_code = -1

        try:
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
                exit_code = process.returncode or 0
            except TimeoutError as e:
                # Safely terminate process hierarchy
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass
                logger.error(f"gallery-dl subprocess timed out after {timeout} seconds")
                raise DownloadTimeoutError(
                    f"Waktu pengunduhan metadata gallery-dl habis ({int(timeout)}s)."
                ) from e

        except DownloadTimeoutError:
            raise
        except Exception as e:
            logger.error(f"Failed to execute gallery-dl subprocess: {e}")
            return None

        elapsed_sec = time.monotonic() - start_time
        stderr_text = stderr.decode("utf-8", errors="replace")

        if exit_code != 0:
            clean_err = sanitize_stderr(stderr_text)
            if "challenge" in stderr_text.lower() or "captcha" in stderr_text.lower() or "403" in stderr_text:
                challenge_detected = True
                logger.warning(
                    f"TikTok challenge page encountered via gallery-dl for item {item_id}: {clean_err}"
                )
                raise TikTokChallengeError(
                    message=f"TikTok challenge encountered via gallery-dl for item {item_id}",
                    user_friendly_message="TikTok sementara menolak akses downloader. Silakan coba kembali beberapa saat lagi.",
                )
            if "Unsupported URL" in stderr_text or "No suitable extractor" in stderr_text:
                return None

            logger.warning(f"gallery-dl nonzero exit code ({exit_code}): {clean_err}")
            return None

        try:
            stdout_text = stdout.decode("utf-8", errors="replace")
            metadata = parse_gallery_dl_tiktok_json(stdout_text, canonical_url, expected_item_id=item_id)
        except DownloadError:
            raise
        except Exception as e:
            logger.error(f"Error processing gallery-dl metadata JSON: {e}")
            metadata = None

        slide_count = len(metadata.items) if metadata else 0
        try:
            parsed_output = json.loads(stdout_text) if stdout_text.strip() else []
            metadata_entries = len(parsed_output) if isinstance(parsed_output, list) else 0
        except (TypeError, ValueError):
            metadata_entries = 0

        proxy_configured = bool(self.settings.TIKTOK_PROXY_URL)

        if not metadata or not metadata.items:
            logger.warning(
                f"TikTok photo extraction empty: platform=tiktok provider=gallery-dl result=empty "
                f"item_id={item_id} cookie_configured={cookie_configured} proxy_configured={proxy_configured} exit_code={exit_code} "
                f"metadata_entries={metadata_entries} slide_count=0 "
                f"challenge_detected={challenge_detected} elapsed_seconds={elapsed_sec:.2f}"
            )
            if "/photo/" in urlsplit(canonical_url).path.lower():
                raise ContentNotSupportedError(
                    "gallery-dl selesai tanpa menghasilkan slide untuk canonical URL /photo/.",
                    user_friendly_message="Slide foto TikTok tidak ditemukan atau postingan tidak tersedia.",
                )
            return None

        logger.info(
            f"TikTok photo extraction completed: platform=tiktok content_type=photo provider=gallery-dl result=success "
            f"item_id={item_id} cookie_configured={cookie_configured} proxy_configured={proxy_configured} exit_code={exit_code} "
            f"metadata_entries={metadata_entries} slide_count={slide_count} "
            f"challenge_detected={challenge_detected} elapsed_seconds={elapsed_sec:.2f}"
        )

        return metadata

    async def download_content(
        self, canonical_url: str, metadata: TikTokContentMetadata, job_dir: Path
    ) -> TikTokContentMetadata:
        if not metadata.items:
            raise DownloadError("Foto tidak ditemukan pada postingan ini.")

        cookies = _load_netscape_cookies(self.settings.TIKTOK_COOKIES_FILE)
        headers = {
            **DEFAULT_TIKTOK_HEADERS,
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        }

        total_slideshow_bytes = 0
        max_bytes = self.settings.MAX_SOURCE_DOWNLOAD_MB * 1024 * 1024
        proxy = self.settings.TIKTOK_PROXY_URL or None

        async with httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers=headers,
            cookies=cookies,
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
                    sanitized_err = sanitize_stderr(str(e)) or "Pengunduhan slide foto terganggu."
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
        return None

    def _cleanup_downloaded_photos(self, job_dir: Path) -> None:
        for f in job_dir.glob("photo_*"):
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass
