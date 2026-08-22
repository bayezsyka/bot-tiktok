import asyncio
import json
import logging
import os
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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

logger = logging.getLogger(__name__)

_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "auth_token",
    "cookie",
    "msToken",
    "sessionid",
    "sig",
    "signature",
    "token",
    "x-bogus",
}
_SIGNED_MEDIA_HOST_MARKERS = ("tiktokcdn.com", "byteoversea.com", "ibytedtos.com")


class YtDlpProvider(DownloaderProvider):
    def __init__(self) -> None:
        self.settings = get_settings()

    def _get_base_args(self) -> list[str]:
        args = [self.settings.YT_DLP_BINARY, "--no-playlist", "--no-warnings"]
        if self.settings.TIKTOK_PROXY_URL:
            args.extend(["--proxy", self.settings.TIKTOK_PROXY_URL])
        cookies_file = self.settings.TIKTOK_COOKIES_FILE
        if cookies_file and os.path.exists(cookies_file) and os.path.isfile(cookies_file):
            args.extend(["--cookies", cookies_file])
        return args

    def _sanitize_error(self, err_text: str) -> str:
        """Keep diagnostic text while redacting paths, cookies, tokens, and signed URLs."""
        if not err_text:
            return "Unknown yt-dlp error"

        def sanitize_url(match: re.Match[str]) -> str:
            raw_url = match.group(0).rstrip(".,);]")
            suffix = match.group(0)[len(raw_url) :]
            try:
                parsed = urlsplit(raw_url)
                query_items = parse_qsl(parsed.query, keep_blank_values=True)
            except ValueError:
                return "[REDACTED_URL]" + suffix

            # Redact user/pass if present in URL
            if parsed.username or parsed.password:
                return "[REDACTED_PROXY_URL]" + suffix

            has_sensitive_query = any(key.lower() in {k.lower() for k in _SENSITIVE_QUERY_KEYS} for key, _ in query_items)
            if parsed.query and any(marker in parsed.netloc.lower() for marker in _SIGNED_MEDIA_HOST_MARKERS):
                return "[REDACTED_SIGNED_URL]" + suffix
            if has_sensitive_query:
                safe_query = urlencode(
                    [
                        (key, "[REDACTED]" if key.lower() in {k.lower() for k in _SENSITIVE_QUERY_KEYS} else value)
                        for key, value in query_items
                    ]
                )
                return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, safe_query, parsed.fragment)) + suffix
            return raw_url + suffix

        sanitized = re.sub(r"\x1b\[[0-9;]*m", "", err_text)
        sanitized = re.sub(r"https?://[^\s<>\"']+", sanitize_url, sanitized)
        sanitized = re.sub(
            r"(?i)\b(authorization\s*:\s*bearer|bearer)\s+[^\s,;]+",
            r"\1 [REDACTED]",
            sanitized,
        )
        sanitized = re.sub(
            r"(?i)\b(access[_-]?token|auth[_-]?token|msToken|sessionid|signature|x-bogus|token)\s*[:=]\s*[^\s,;]+",
            r"\1=[REDACTED]",
            sanitized,
        )
        sanitized = re.sub(
            r"(?i)(cookie(?:s)?(?:\s+file)?(?:\s+(?:loaded\s+)?from|\s+at|\s+path)?\s*[:=]?\s*)"
            r"(?:[A-Za-z]:\\[^\s]+|/[^\s,;]+)",
            r"\1[REDACTED_PATH]",
            sanitized,
        )
        sanitized = re.sub(
            r"(?i)(cookie\s*:\s*)(?!needed\b)[^\n]+",
            r"\1[REDACTED]",
            sanitized,
        )
        sanitized = re.sub(
            r"(?i)(--proxy\s+)[^\s]+",
            r"\1[REDACTED]",
            sanitized,
        )
        sanitized = re.sub(
            r"(?<![:/\w])/(?:Users|home|app|opt|srv|var|tmp|private|workspace|code|root)(?:/[^\s:,;]+)+",
            "[REDACTED_PATH]",
            sanitized,
        )
        sanitized = re.sub(r"(?i)\b[A-Z]:\\(?:[^\s:,;]+\\)*[^\s:,;]+", "[REDACTED_PATH]", sanitized)

        clean_lines = [line.strip() for line in sanitized.splitlines() if line.strip()]
        clean_message = " - ".join(clean_lines)
        if not clean_message:
            return "Unknown yt-dlp error"
        return clean_message[:1200]

    @staticmethod
    def _canonical_type(canonical_url: str) -> str | None:
        path = urlsplit(canonical_url).path.lower()
        if "/video/" in path:
            return "video"
        if "/photo/" in path:
            return "photo"
        return None

    def _handle_nonzero_error(
        self, canonical_url: str, err_text: str, *, operation: str
    ) -> None:
        """Classify yt-dlp failures without turning classified videos into photo fallbacks."""
        clean_err = self._sanitize_error(err_text)
        error_lower = err_text.lower()
        canonical_type = self._canonical_type(canonical_url)

        private_markers = ("private video", "this video is private", "private account")
        unavailable_markers = (
            "video unavailable",
            "video is unavailable",
            "video isn't available",
            "post is unavailable",
            "post isn't available",
            "no longer available",
            "has been deleted",
            "video has been removed",
            "content is not available",
            "status code 404",
            "http error 404",
        )
        challenge_markers = (
            "sign in to confirm you are not a bot",
            "sign in",
            "fresh cookies are needed",
            "login",
            "login required",
            "log in to continue",
            "bot verification",
            "captcha",
            "challenge",
            "cookies are needed",
            "cookie is required",
            "use --cookies",
            "use browser cookies",
            "cookies-from-browser",
        )
        network_markers = (
            "http error 403",
            "status code 403",
            "http error 429",
            "status code 429",
            "timed out",
            "timeout",
            "temporary failure",
            "network is unreachable",
            "connection reset",
            "connection refused",
            "remote end closed connection",
            "http error 500",
            "http error 502",
            "http error 503",
            "http error 504",
        )
        non_video_markers = ("unsupported url", "slideshow", "image post", "photo post")

        if any(marker in error_lower for marker in private_markers):
            raise ContentNotSupportedError(
                f"Video TikTok private: {clean_err}",
                user_friendly_message="Video TikTok bersifat privat dan tidak dapat diunduh.",
            )
        if any(marker in error_lower for marker in unavailable_markers):
            raise ContentNotSupportedError(
                f"Video TikTok tidak tersedia: {clean_err}",
                user_friendly_message="Video TikTok sudah dihapus atau tidak tersedia.",
            )
        if any(marker in error_lower for marker in challenge_markers):
            raise TikTokChallengeError(
                message=f"Verifikasi TikTok saat {operation} video: {clean_err}",
                user_friendly_message=(
                    "TikTok sementara meminta verifikasi untuk mengakses video. "
                    "Video akan dicoba kembali."
                ),
            )
        if any(marker in error_lower for marker in network_markers):
            raise DownloadError(
                f"Gangguan jaringan saat {operation} video TikTok: {clean_err}",
                user_friendly_message=(
                    "Koneksi ke TikTok terganggu saat memproses video. Silakan coba kembali."
                ),
            )
        if any(marker in error_lower for marker in non_video_markers):
            if canonical_type is None:
                logger.info(
                    "yt-dlp declined unclassified TikTok URL: result=non_video error=%s",
                    clean_err,
                )
                return
            if canonical_type == "photo":
                return
            raise ContentNotSupportedError(
                f"URL video TikTok tidak didukung yt-dlp: {clean_err}",
                user_friendly_message="Link ini tidak didukung sebagai video TikTok.",
            )

        raise DownloadError(
            f"yt-dlp gagal saat {operation} video TikTok: {clean_err}",
            user_friendly_message="Video TikTok sementara tidak dapat diproses. Silakan coba kembali.",
        )

    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        # yt-dlp handles videos; if extract_metadata succeeds as video, we handle it
        metadata = await self.extract_metadata(canonical_url, job_dir)
        return bool(metadata and metadata.content_type == "video")

    async def extract_metadata(self, canonical_url: str, job_dir: Path) -> TikTokContentMetadata | None:
        args = self._get_base_args() + ["--dump-json", canonical_url]

        try:
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=min(self.settings.JOB_TIMEOUT_SECONDS, 60.0)
            )
        except TimeoutError as e:
            raise DownloadTimeoutError("Timeout while extracting video metadata from TikTok") from e
        except Exception as e:
            logger.error("Failed to run yt-dlp subprocess: %s", self._sanitize_error(str(e)))
            raise DownloadError("Gagal menjalankan downloader video") from e

        if process.returncode != 0:
            err_msg = stderr.decode("utf-8", errors="replace")
            self._handle_nonzero_error(canonical_url, err_msg, operation="ekstraksi metadata")
            return None

        try:
            data = json.loads(stdout.decode("utf-8", errors="replace"))
        except Exception as e:
            raise DownloadError(
                "Output metadata yt-dlp untuk video rusak atau tidak valid.",
                user_friendly_message="Video TikTok sementara tidak dapat diproses. Silakan coba kembali.",
            ) from e

        # Check for live stream or playlist
        if data.get("is_live") or data.get("live_status") == "is_live":
            raise ContentNotSupportedError(
                "Live stream tidak didukung.",
                user_friendly_message="Konten live stream TikTok tidak dapat diunduh.",
            )
        if data.get("_type") in ("playlist", "multi_video"):
            raise ContentNotSupportedError(
                "Playlist tidak didukung.",
                user_friendly_message="Link playlist atau profil tidak didukung. Harap kirim link postingan tunggal.",
            )

        duration = int(data.get("duration") or 0)
        if duration > self.settings.MAX_VIDEO_DURATION_SECONDS:
            raise ContentNotSupportedError(
                f"Durasi video melebihi batas {self.settings.MAX_VIDEO_DURATION_SECONDS} detik.",
                user_friendly_message="Durasi video terlalu panjang melebihi batas maksimal.",
            )

        title = str(data.get("title") or data.get("description") or "TikTok Video")[:200]
        author = str(data.get("uploader") or data.get("channel") or "Unknown")

        # Check if it's actually an image slideshow identified by yt-dlp without video streams
        formats = data.get("formats", [])
        if not formats and not data.get("url"):
            if self._canonical_type(canonical_url) == "video":
                raise DownloadError(
                    "yt-dlp tidak menemukan stream video pada canonical URL /video/.",
                    user_friendly_message="Stream video TikTok tidak dapat ditemukan. Silakan coba kembali.",
                )
            return None

        return TikTokContentMetadata(
            content_type="video",
            title=title,
            author=author,
            duration_seconds=duration,
            items=[
                TikTokMediaItemMetadata(
                    position=1,
                    source_url=canonical_url,
                    media_type="video",
                )
            ],
        )

    async def download_content(
        self, canonical_url: str, metadata: TikTokContentMetadata, job_dir: Path
    ) -> TikTokContentMetadata:
        if not metadata.items:
            raise DownloadError("Metadata item tidak valid.")

        output_template = str(job_dir / "video_source.%(ext)s")
        max_size_m = self.settings.MAX_SOURCE_DOWNLOAD_MB

        args = self._get_base_args() + [
            "-f",
            "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "--merge-output-format",
            "mp4",
            "--max-filesize",
            f"{max_size_m}M",
            "-o",
            output_template,
            canonical_url,
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=float(self.settings.JOB_TIMEOUT_SECONDS)
            )
        except TimeoutError as e:
            raise DownloadTimeoutError("Waktu pengunduhan video habis.") from e
        except Exception as e:
            raise DownloadError("Terjadi kesalahan sistem saat mengunduh video.") from e

        if process.returncode != 0:
            err_msg = stderr.decode("utf-8", errors="replace")
            if "File is larger than max-filesize" in err_msg or "max-filesize" in err_msg:
                raise DownloadSizeLimitExceededError(
                    "Ukuran sumber video melebihi batas maksimal.",
                    user_friendly_message="Ukuran video asli melebihi batas maksimal unduhan.",
                )
            self._handle_nonzero_error(canonical_url, err_msg, operation="pengunduhan")
            raise DownloadError("yt-dlp gagal mengunduh video tanpa detail error.")

        # Find downloaded file
        downloaded_files = [
            f for f in job_dir.iterdir() if f.is_file() and f.stem == "video_source"
        ]
        if not downloaded_files:
            # check if any file was downloaded
            downloaded_files = [f for f in job_dir.iterdir() if f.is_file() and not f.name.startswith(".")]

        if not downloaded_files:
            raise DownloadError("File video hasil unduhan tidak ditemukan.")

        metadata.items[0].local_path = str(downloaded_files[0].resolve())
        return metadata
