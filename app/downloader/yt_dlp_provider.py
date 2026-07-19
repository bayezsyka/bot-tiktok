import asyncio
import json
import logging
import os
from pathlib import Path

from app.config import get_settings
from app.downloader.exceptions import (
    ContentNotSupportedError,
    DownloadError,
    DownloadSizeLimitExceededError,
    DownloadTimeoutError,
)
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.providers import DownloaderProvider

logger = logging.getLogger(__name__)


class YtDlpProvider(DownloaderProvider):
    def __init__(self) -> None:
        self.settings = get_settings()

    def _get_base_args(self) -> list[str]:
        args = [self.settings.YT_DLP_BINARY, "--no-playlist", "--no-warnings"]
        cookies_file = self.settings.TIKTOK_COOKIES_FILE
        if cookies_file and os.path.exists(cookies_file) and os.path.isfile(cookies_file):
            args.extend(["--cookies", cookies_file])
        return args

    def _sanitize_error(self, err_text: str) -> str:
        """Sanitize error message to prevent exposing internal paths or cookies info."""
        if not err_text:
            return "Unknown yt-dlp error"
        lines = err_text.strip().split("\n")
        # Keep only key message lines
        clean_lines = []
        for line in lines:
            if "cookie" in line.lower() or "/" in line or "\\" in line:
                continue
            clean_lines.append(line.strip())
        return " - ".join(clean_lines[:2]) or "yt-dlp execution error"

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
            logger.error(f"Failed to run yt-dlp subprocess: {e}")
            raise DownloadError("Gagal menjalankan downloader video") from e

        if process.returncode != 0:
            err_msg = stderr.decode("utf-8", errors="replace")
            # If yt-dlp says unsupported or photo post / slideshow without video stream
            if "Unsupported URL" in err_msg or "slideshow" in err_msg.lower() or "image post" in err_msg.lower():
                return None
            clean_err = self._sanitize_error(err_msg)
            logger.warning(f"yt-dlp dump-json error: {clean_err}")
            return None

        try:
            data = json.loads(stdout.decode("utf-8", errors="replace"))
        except Exception:
            return None

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
            clean_err = self._sanitize_error(err_msg)
            raise DownloadError(f"Gagal mengunduh video dari TikTok: {clean_err}")

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
