import asyncio
import json
import logging
import os
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
)
from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata
from app.downloader.providers import DownloaderProvider

logger = logging.getLogger(__name__)

# gallery-dl message type codes
_CODE_METADATA = 2
_CODE_MEDIA = 3

# Instagram CDN hosts allowed for photo/video downloads
ALLOWED_INSTAGRAM_CDN_HOSTS = (
    ".fbcdn.net",
    ".cdninstagram.net",
    ".instagram.com",
)

# Image magic-byte signatures
_IMAGE_EXTENSIONS_BY_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"\xff\xd8\xff", "jpg"),
    (b"\x89PNG\r\n\x1a\n", "png"),
    (b"RIFF", "webp"),
    (b"GIF8", "gif"),
    (b"\x00\x00\x00 ftypavif", "avif"),
    (b"\x00\x00\x00\x1cftypmif1", "avif"),
)

# Video magic-byte signatures for MP4 family
_VIDEO_MAGIC_PREFIXES: tuple[bytes, ...] = (
    b"\x00\x00\x00\x1cftyp",
    b"\x00\x00\x00\x18ftyp",
    b"\x00\x00\x00\x20ftyp",
)

PHOTO_EXTENSIONS = ("jpg", "jpeg", "png", "webp", "gif", "avif")
VIDEO_EXTENSIONS = ("mp4", "mov", "m4v", "webm")

# Error classification keywords
_LOGIN_KEYWORDS = ("login required", "login_required", "requires authentication", "requires login")
_PRIVATE_KEYWORDS = ("private", "is_private")
_CHECKPOINT_KEYWORDS = ("checkpoint", "challenge", "captcha", "verify")
_DELETED_KEYWORDS = ("deleted", "not found", "no longer available", "unavailable")
_FORBIDDEN_KEYWORDS = ("403", "forbidden")
_RATE_LIMIT_KEYWORDS = ("429", "rate limit", "too many requests")


def sanitize_instagram_stderr(stderr_text: str) -> str:
    """Sanitize subprocess stderr to remove sensitive cookies, file paths, or signature tokens."""
    if not stderr_text:
        return ""
    lines = stderr_text.strip().split("\n")
    clean_lines = []
    for line in lines:
        lower = line.lower()
        if "cookie" in lower or "sessionid" in lower or "csrf" in lower:
            continue
        if "/" in line or "\\" in line:
            continue
        clean_lines.append(line.strip())
    return " - ".join(clean_lines[:2]) or "gallery-dl execution error"


def _is_allowed_instagram_cdn_url(url: str) -> bool:
    if not isinstance(url, str) or not url.startswith("http"):
        return False
    try:
        parsed = httpx.URL(url)
        host = parsed.host.lower()
        return any(host == d.lstrip(".") or host.endswith(d) for d in ALLOWED_INSTAGRAM_CDN_HOSTS)
    except Exception:
        return False


def _extract_shortcode_from_url(url: str) -> str | None:
    """Extract Instagram shortcode from /p/, /reel/, or /reels/ URL path."""
    if not url:
        return None
    try:
        path = urlsplit(url).path
    except Exception:
        return None
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2 and parts[0] in ("p", "reel", "reels"):
        sc = parts[1].strip()
        return sc or None
    return None


def _classify_extension(ext: str | None, video_url: Any) -> str:
    """Return 'photo', 'video', or 'unknown' for a media entry."""
    if video_url:
        return "video"
    if not ext:
        return "photo"
    e = ext.lower().lstrip(".")
    if e in VIDEO_EXTENSIONS:
        return "video"
    return "photo"


def parse_gallery_dl_instagram_post_json(
    raw_json_str: str, canonical_url: str
) -> MediaContentMetadata | None:
    """
    Parse gallery-dl --dump-json output for Instagram /p/ posts.

    gallery-dl emits a list of entries:
      - [2, {metadata_dict}]            # post-level metadata (type code 2)
      - [3, "https://cdn.../img.jpg", {item_meta}]  # media item (type code 3)

    Media items carry a `num` field (1-based position) for carousel ordering,
    an `extension` field, and a `video_url` field (non-null for video items).
    """
    if not raw_json_str or not raw_json_str.strip():
        return None

    try:
        data = json.loads(raw_json_str)
    except Exception as e:
        logger.error(f"Failed to parse gallery-dl Instagram JSON output: {e}")
        raise DownloadError("Output metadata gallery-dl Instagram rusak atau tidak valid.") from e

    if not isinstance(data, list):
        return None

    expected_shortcode = _extract_shortcode_from_url(canonical_url)

    post_meta: dict[str, Any] = {}
    media_entries: list[dict[str, Any]] = []

    for entry in data:
        if not isinstance(entry, list) or len(entry) < 2:
            continue

        type_code = entry[0]

        if type_code == _CODE_METADATA and isinstance(entry[1], dict):
            post_meta = entry[1]
        elif type_code == _CODE_MEDIA and len(entry) >= 2:
            url = entry[1]
            item_meta: dict[str, Any] = entry[2] if len(entry) >= 3 and isinstance(entry[2], dict) else {}
            if isinstance(url, str) and url.startswith("http"):
                media_entries.append({"url": url, "meta": item_meta})

    if not media_entries:
        return None

    # Verify shortcode from metadata if available
    meta_shortcode = (
        post_meta.get("post_shortcode")
        or post_meta.get("shortcode")
        or None
    )
    if expected_shortcode and meta_shortcode and str(meta_shortcode) != expected_shortcode:
        logger.warning(
            f"gallery-dl Instagram shortcode mismatch: extracted {meta_shortcode} != expected {expected_shortcode}"
        )
        return None

    # Sort by num to preserve original carousel order
    def _num_key(entry: dict[str, Any]) -> int:
        n = entry.get("meta", {}).get("num")
        try:
            return int(n) if n is not None else 0
        except (TypeError, ValueError):
            return 0

    media_entries.sort(key=_num_key)

    # Build ordered, deduplicated media items
    seen_urls: set[str] = set()
    ordered_items: list[MediaItemMetadata] = []

    for entry in media_entries:
        url = entry["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        item_meta = entry.get("meta", {})
        ext = item_meta.get("extension")
        video_url = item_meta.get("video_url")
        media_type = _classify_extension(
            ext if isinstance(ext, str) else None,
            video_url,
        )

        # Skip non-media files (covers, previews, avatars, audio-only, sidecar thumbnails)
        if not _is_allowed_instagram_cdn_url(url):
            continue
        url_lower = url.lower()
        if any(skip in url_lower for skip in ("/cover", "cover.jpg", "cover.png", "/avatar", "/preview")):
            continue
        if url_lower.endswith((".mp3", ".m4a", ".wav", ".aac", ".ogg")):
            continue

        ordered_items.append(
            MediaItemMetadata(
                position=len(ordered_items) + 1,
                source_url=url,
                media_type=media_type,
            )
        )

    if not ordered_items:
        return None

    # Determine content_type
    media_types = {item.media_type for item in ordered_items}
    if len(ordered_items) > 1 and media_types == {"photo", "video"}:
        content_type = "carousel"
    elif media_types == {"video"}:
        content_type = "video"
    else:
        content_type = "photo"

    author = str(
        post_meta.get("fullname")
        or post_meta.get("username")
        or "Instagram"
    )
    title = str(post_meta.get("description") or post_meta.get("post_url") or "Instagram Post")[:200]

    return MediaContentMetadata(
        content_type=content_type,
        title=title,
        author=author,
        duration_seconds=0,
        items=ordered_items,
    )


class GalleryDlInstagramPostProvider(DownloaderProvider):
    """Downloads Instagram /p/ posts (single photo, photo carousel, mixed photo/video) via gallery-dl."""

    def __init__(self) -> None:
        self.settings = get_settings()

    def _is_binary_available(self) -> bool:
        binary_path = self.settings.GALLERY_DL_BINARY
        if os.path.isabs(binary_path):
            return os.path.exists(binary_path) and os.path.isfile(binary_path)
        return shutil.which(binary_path) is not None

    def _is_cookie_configured(self) -> bool:
        cookies_file = self.settings.INSTAGRAM_COOKIES_FILE
        return bool(
            cookies_file
            and os.path.exists(cookies_file)
            and os.path.isfile(cookies_file)
            and os.access(cookies_file, os.R_OK)
        )

    def _build_metadata_args(self, canonical_url: str) -> list[str]:
        args: list[str] = [
            self.settings.GALLERY_DL_BINARY,
            "--dump-json",
            "-o",
            "extractor.instagram.order-files=asc",
            "-o",
            "extractor.instagram.videos=merged",
        ]
        if self._is_cookie_configured():
            args.extend(["--cookies", self.settings.INSTAGRAM_COOKIES_FILE])
        args.append(canonical_url)
        return args

    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        metadata = await self.extract_metadata(canonical_url, job_dir)
        return bool(metadata and metadata.items)

    async def extract_metadata(self, canonical_url: str, job_dir: Path) -> MediaContentMetadata | None:
        shortcode = _extract_shortcode_from_url(canonical_url) or "unknown"

        if not self._is_binary_available():
            logger.error(
                "Instagram post extraction: provider=gallery-dl shortcode=%s "
                "cookie_configured=%s exit_code=-1 error=binary_unavailable media_count=0",
                shortcode,
                self._is_cookie_configured(),
            )
            raise DownloadError(
                "Binary gallery-dl tidak tersedia.",
                user_friendly_message="Sistem unduhan Instagram sedang tidak tersedia.",
            )

        cookie_configured = self._is_cookie_configured()
        args = self._build_metadata_args(canonical_url)
        timeout = min(float(self.settings.JOB_TIMEOUT_SECONDS), 240.0)
        start_time = time.monotonic()
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
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass
                elapsed = time.monotonic() - start_time
                logger.error(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=-1 media_count=0 "
                    "error=timeout elapsed_seconds=%.2f",
                    shortcode, cookie_configured, elapsed,
                )
                raise DownloadTimeoutError(
                    f"Waktu pengunduhan metadata Instagram habis ({int(timeout)}s).",
                    user_friendly_message="Pengunduhan konten Instagram memakan waktu terlalu lama.",
                ) from e
        except DownloadTimeoutError:
            raise
        except Exception as e:
            elapsed = time.monotonic() - start_time
            logger.error(
                "Instagram post extraction: provider=gallery-dl shortcode=%s "
                "cookie_configured=%s exit_code=-1 media_count=0 "
                "error=subprocess_failed elapsed_seconds=%.2f: %s",
                shortcode, cookie_configured, elapsed, e,
            )
            raise DownloadError(
                "Gagal menjalankan gallery-dl untuk Instagram.",
                user_friendly_message="Gagal mengambil konten Instagram. Silakan coba kembali.",
            ) from e

        elapsed_sec = time.monotonic() - start_time
        stderr_text = stderr.decode("utf-8", errors="replace")
        stdout_text = stdout.decode("utf-8", errors="replace")

        # Classify nonzero exit errors
        if exit_code != 0:
            clean_err = sanitize_instagram_stderr(stderr_text)
            lower_err = stderr_text.lower()

            if any(k in lower_err for k in _CHECKPOINT_KEYWORDS):
                logger.warning(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=%s media_count=0 "
                    "error=checkpoint elapsed_seconds=%.2f",
                    shortcode, cookie_configured, exit_code, elapsed_sec,
                )
                raise ContentNotSupportedError(
                    "Instagram checkpoint/challenge encountered.",
                    user_friendly_message="Instagram memerlukan verifikasi. Silakan coba kembali nanti.",
                )
            if any(k in lower_err for k in _RATE_LIMIT_KEYWORDS):
                logger.warning(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=%s media_count=0 "
                    "error=rate_limited elapsed_seconds=%.2f",
                    shortcode, cookie_configured, exit_code, elapsed_sec,
                )
                raise DownloadError(
                    "Instagram membatasi jumlah permintaan (429).",
                    user_friendly_message="Instagram sedang membatasi permintaan. Silakan coba kembali nanti.",
                )
            if any(k in lower_err for k in _FORBIDDEN_KEYWORDS):
                logger.warning(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=%s media_count=0 "
                    "error=forbidden_403 elapsed_seconds=%.2f",
                    shortcode, cookie_configured, exit_code, elapsed_sec,
                )
                raise DownloadError(
                    "Instagram menolak akses (403).",
                    user_friendly_message="Instagram menolak akses. Silakan coba kembali nanti.",
                )
            if any(k in lower_err for k in _LOGIN_KEYWORDS) or not cookie_configured:
                logger.warning(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=%s media_count=0 "
                    "error=login_required elapsed_seconds=%.2f",
                    shortcode, cookie_configured, exit_code, elapsed_sec,
                )
                raise ContentNotSupportedError(
                    "Instagram requires authentication.",
                    user_friendly_message="Konten Instagram memerlukan login. Silakan coba konten lain.",
                )
            if any(k in lower_err for k in _PRIVATE_KEYWORDS):
                logger.warning(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=%s media_count=0 "
                    "error=private_post elapsed_seconds=%.2f",
                    shortcode, cookie_configured, exit_code, elapsed_sec,
                )
                raise ContentNotSupportedError(
                    "Instagram post is private.",
                    user_friendly_message="Konten Instagram bersifat privat.",
                )
            if any(k in lower_err for k in _DELETED_KEYWORDS):
                logger.warning(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=%s media_count=0 "
                    "error=deleted_or_missing elapsed_seconds=%.2f",
                    shortcode, cookie_configured, exit_code, elapsed_sec,
                )
                raise ContentNotSupportedError(
                    "Instagram post deleted or not found.",
                    user_friendly_message="Konten Instagram telah dihapus atau tidak tersedia.",
                )
            if "Unsupported URL" in stderr_text or "No suitable extractor" in stderr_text:
                logger.warning(
                    "Instagram post extraction: provider=gallery-dl shortcode=%s "
                    "cookie_configured=%s exit_code=%s media_count=0 "
                    "error=unsupported_url elapsed_seconds=%.2f",
                    shortcode, cookie_configured, exit_code, elapsed_sec,
                )
                return None

            logger.warning(
                "Instagram post extraction: provider=gallery-dl shortcode=%s "
                "cookie_configured=%s exit_code=%s media_count=0 "
                "error=unknown elapsed_seconds=%.2f: %s",
                shortcode, cookie_configured, exit_code, elapsed_sec, clean_err,
            )
            raise DownloadError(
                f"gallery-dl keluar dengan kode {exit_code}.",
                user_friendly_message="Gagal mengambil konten Instagram. Silakan coba kembali.",
            )

        # Parse JSON output
        try:
            metadata = parse_gallery_dl_instagram_post_json(stdout_text, canonical_url)
        except DownloadError:
            elapsed = time.monotonic() - start_time
            logger.error(
                "Instagram post extraction: provider=gallery-dl shortcode=%s "
                "cookie_configured=%s exit_code=%s media_count=0 "
                "error=malformed_json elapsed_seconds=%.2f",
                shortcode, cookie_configured, exit_code, elapsed,
            )
            raise

        if not metadata or not metadata.items:
            image_count = 0
            video_count = 0
            content_type = "none"
        else:
            image_count = sum(1 for i in metadata.items if i.media_type == "photo")
            video_count = sum(1 for i in metadata.items if i.media_type == "video")
            content_type = metadata.content_type

        media_count = len(metadata.items) if metadata else 0

        if not metadata or not metadata.items:
            logger.warning(
                "Instagram post extraction: provider=gallery-dl shortcode=%s "
                "cookie_configured=%s exit_code=%s content_type=none media_count=0 "
                "image_count=0 video_count=0 error=empty_output elapsed_seconds=%.2f",
                shortcode, cookie_configured, exit_code, elapsed_sec,
            )
            raise ContentNotSupportedError(
                "gallery-dl selesai tanpa menghasilkan media untuk URL Instagram /p/.",
                user_friendly_message="Konten Instagram tidak ditemukan atau tidak tersedia.",
            )

        logger.info(
            "Instagram post extraction: provider=gallery-dl shortcode=%s "
            "cookie_configured=%s exit_code=%s content_type=%s media_count=%s "
            "image_count=%s video_count=%s elapsed_seconds=%.2f",
            shortcode, cookie_configured, exit_code, content_type,
            media_count, image_count, video_count, elapsed_sec,
        )

        return metadata

    async def download_content(
        self, canonical_url: str, metadata: MediaContentMetadata, job_dir: Path
    ) -> MediaContentMetadata:
        if not metadata.items:
            raise DownloadError("Media Instagram tidak ditemukan pada postingan ini.")

        max_bytes = self.settings.MAX_SOURCE_DOWNLOAD_MB * 1024 * 1024
        total_bytes = 0

        for item in metadata.items:
            if item.media_type == "video":
                await self._download_video_item(item, job_dir, max_bytes)
            else:
                await self._download_photo_item(item, job_dir, max_bytes)

            local_path = item.local_path
            if not local_path or not os.path.exists(local_path):
                self._cleanup_partial(job_dir)
                raise DownloadError(
                    f"File media Instagram posisi {item.position} gagal disimpan.",
                    user_friendly_message="Gagal mengunduh file media Instagram.",
                )
            total_bytes += os.path.getsize(local_path)

        if total_bytes > max_bytes:
            self._cleanup_partial(job_dir)
            raise DownloadSizeLimitExceededError(
                "Ukuran total media Instagram melebihi batas unduhan.",
                user_friendly_message="Ukuran media Instagram melebihi batas maksimal unduhan.",
            )

        return metadata

    async def _download_photo_item(
        self, item: MediaItemMetadata, job_dir: Path, max_bytes: int
    ) -> None:
        if not _is_allowed_instagram_cdn_url(item.source_url):
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"URL media foto Instagram posisi {item.position} bukan host CDN yang valid.",
                user_friendly_message="Gagal mengunduh file media Instagram.",
            )

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }

        try:
            async with httpx.AsyncClient(
                timeout=30.0, follow_redirects=True, headers=headers, verify=True
            ) as client:
                resp = await client.get(item.source_url)
                resp.raise_for_status()
                content = resp.content
        except Exception as e:
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"Gagal mengunduh foto Instagram posisi {item.position}: {e}",
                user_friendly_message="Gagal mengunduh file media Instagram.",
            ) from e

        if len(content) > max_bytes:
            self._cleanup_partial(job_dir)
            raise DownloadSizeLimitExceededError(
                f"Ukuran foto Instagram posisi {item.position} melebihi batas unduhan.",
                user_friendly_message="Ukuran foto Instagram melebihi batas maksimal unduhan.",
            )

        content_type_header = resp.headers.get("content-type", "").lower()
        if content_type_header and not content_type_header.startswith("image/"):
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"Content-Type foto Instagram posisi {item.position} tidak valid: {content_type_header}",
                user_friendly_message="File media Instagram rusak atau format tidak didukung.",
            )

        ext = self._detect_image_extension(content)
        if not ext:
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"File foto Instagram posisi {item.position} rusak atau bukan gambar valid.",
                user_friendly_message="File media Instagram rusak atau format gambar tidak didukung.",
            )

        local_filename = job_dir / f"instagram_{item.position:03d}.{ext}"
        try:
            with open(local_filename, "wb") as f:
                f.write(content)
        except Exception as e:
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"Gagal menyimpan file foto Instagram posisi {item.position}: {e}"
            ) from e

        item.local_path = str(local_filename.resolve())

    async def _download_video_item(
        self, item: MediaItemMetadata, job_dir: Path, max_bytes: int
    ) -> None:
        if not _is_allowed_instagram_cdn_url(item.source_url):
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"URL media video Instagram posisi {item.position} bukan host CDN yang valid.",
                user_friendly_message="Gagal mengunduh file media Instagram.",
            )

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept": "video/mp4,video/*,*/*;q=0.8",
        }

        try:
            async with httpx.AsyncClient(
                timeout=60.0, follow_redirects=True, headers=headers, verify=True
            ) as client:
                resp = await client.get(item.source_url)
                resp.raise_for_status()
                content = resp.content
        except Exception as e:
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"Gagal mengunduh video Instagram posisi {item.position}: {e}",
                user_friendly_message="Gagal mengunduh file media Instagram.",
            ) from e

        if len(content) > max_bytes:
            self._cleanup_partial(job_dir)
            raise DownloadSizeLimitExceededError(
                f"Ukuran video Instagram posisi {item.position} melebihi batas unduhan.",
                user_friendly_message="Ukuran video Instagram melebihi batas maksimal unduhan.",
            )

        if not self._is_valid_mp4(content):
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"File video Instagram posisi {item.position} rusak atau bukan MP4 valid.",
                user_friendly_message="File video Instagram rusak atau format tidak didukung.",
            )

        local_filename = job_dir / f"instagram_{item.position:03d}.mp4"
        try:
            with open(local_filename, "wb") as f:
                f.write(content)
        except Exception as e:
            self._cleanup_partial(job_dir)
            raise DownloadError(
                f"Gagal menyimpan file video Instagram posisi {item.position}: {e}"
            ) from e

        item.local_path = str(local_filename.resolve())

    def _detect_image_extension(self, content: bytes) -> str | None:
        for magic, ext in _IMAGE_EXTENSIONS_BY_MAGIC:
            if content.startswith(magic):
                if ext == "webp" and content[8:12] != b"WEBP":
                    continue
                return ext
        return None

    def _is_valid_mp4(self, content: bytes) -> bool:
        for prefix in _VIDEO_MAGIC_PREFIXES:
            if content.startswith(prefix):
                return True
        return False

    def _cleanup_partial(self, job_dir: Path) -> None:
        for f in job_dir.glob("instagram_*"):
            try:
                f.unlink(missing_ok=True)
            except Exception:
                pass
