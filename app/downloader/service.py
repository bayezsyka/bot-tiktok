import asyncio
import logging
import os
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from app.config import get_settings
from app.downloader.dtos import (
    DownloadedContentResult,
    DownloadedItemResult,
    ExtractedMetadataResult,
    JobDownloadSnapshot,
)
from app.downloader.exceptions import DownloadError
from app.downloader.gallery_dl_instagram_post_provider import GalleryDlInstagramPostProvider
from app.downloader.gallery_dl_tiktok_photo_provider import GalleryDlTikTokPhotoProvider
from app.downloader.instagram_provider import InstagramReelProvider
from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata
from app.downloader.providers import DownloaderProvider
from app.downloader.tiktok_photo_provider import TikTokPhotoProvider
from app.downloader.tikwm_tiktok_photo_provider import TikwmTikTokPhotoProvider
from app.downloader.tikwm_tiktok_video_provider import TikwmTikTokVideoProvider
from app.downloader.yt_dlp_provider import YtDlpProvider
from app.media.slideshow import render_slideshow_video
from app.security.urls import INSTAGRAM_POST_PATHS, check_url_security, resolve_canonical_tiktok_url

logger = logging.getLogger(__name__)


class DownloaderService:
    def __init__(self) -> None:
        self.yt_dlp = YtDlpProvider()
        self.tikwm_video_provider = TikwmTikTokVideoProvider()
        self.gallery_dl = GalleryDlTikTokPhotoProvider()
        self.photo_provider = TikTokPhotoProvider()
        self.tikwm_provider = TikwmTikTokPhotoProvider()
        self.ig_provider = InstagramReelProvider()
        self.ig_post_provider = GalleryDlInstagramPostProvider()
        self.settings = get_settings()

    @staticmethod
    def _instagram_path_kind(canonical_url: str) -> str:
        """Return 'post' for /p/, 'reel' for /reel/ or /reels/, '' otherwise."""
        path = urlsplit(canonical_url).path.lower()
        parts = [p for p in path.split("/") if p]
        if parts and parts[0] in INSTAGRAM_POST_PATHS:
            return "post"
        if parts and parts[0] in ("reel", "reels"):
            return "reel"
        return ""

    @staticmethod
    def _canonical_type(canonical_url: str) -> str | None:
        path = urlsplit(canonical_url).path.lower()
        if "/video/" in path:
            return "video"
        if "/photo/" in path:
            return "photo"
        return None

    async def resolve_canonical_url(self, snapshot: JobDownloadSnapshot) -> str:
        """Resolve short TikTok links without opening or using a database session."""
        if snapshot.canonical_url:
            return snapshot.canonical_url

        is_safe, detected_platform = check_url_security(snapshot.original_url)
        if not is_safe or not detected_platform:
            raise DownloadError(
                "Link media tidak valid atau tidak aman.",
                user_friendly_message="Link media tidak valid, berisiko, atau tidak dapat diakses.",
            )

        parsed = urlsplit(snapshot.original_url)
        hostname = (parsed.hostname or "").lower()
        is_tiktok_short_url = detected_platform == "tiktok" and (
            hostname in {"vt.tiktok.com", "vm.tiktok.com"}
            or parsed.path.lower().startswith("/t/")
        )
        if not is_tiktok_short_url:
            return snapshot.original_url

        canonical_url = await resolve_canonical_tiktok_url(snapshot.original_url)
        if not canonical_url:
            raise DownloadError(
                "Link media tidak valid atau tidak aman.",
                user_friendly_message="Link media tidak valid, berisiko, atau tidak dapat diakses.",
            )
        return canonical_url

    async def extract_metadata(
        self, snapshot: JobDownloadSnapshot, job_dir: Path
    ) -> ExtractedMetadataResult:
        """
        Resolve the canonical URL and extract metadata without opening or using a DB session.
        """
        platform = snapshot.platform or "tiktok"
        canonical_url = await self.resolve_canonical_url(snapshot)

        provider: DownloaderProvider
        metadata: MediaContentMetadata | None = None

        if platform == "instagram":
            ig_kind = self._instagram_path_kind(canonical_url)
            if ig_kind == "post":
                provider = self.ig_post_provider
                metadata = await self.ig_post_provider.extract_metadata(canonical_url, job_dir)
                if not metadata or not metadata.items:
                    raise DownloadError(
                        "Link Instagram Post tidak dapat diproses.",
                        user_friendly_message="konten Instagram tidak dapat diakses. pastikan kontennya bersifat publik.",
                    )
            elif ig_kind == "reel":
                provider = self.ig_provider
                metadata = await self.ig_provider.extract_metadata(canonical_url, job_dir)
                if not metadata or not metadata.items:
                    raise DownloadError(
                        "Link Instagram Reels tidak dapat diproses.",
                        user_friendly_message="reels instagram tidak dapat diakses. pastikan akun dan kontennya bersifat publik.",
                    )
            else:
                raise DownloadError(
                    "Link Instagram tidak dikenali sebagai Reel maupun Post.",
                    user_friendly_message="Link Instagram tidak dapat diproses.",
                )
        else:
            # TikTok platform
            canonical_type = self._canonical_type(canonical_url)

            if canonical_type == "photo":
                # Primary for /photo/: gallery-dl
                provider = self.gallery_dl
                gallery_empty_error: Exception | None = None
                try:
                    metadata = await self.gallery_dl.extract_metadata(canonical_url, job_dir)
                except Exception as exc:
                    gallery_empty_error = exc

                # Fallback to HTML parser if gallery-dl returned no slides or hit a challenge
                if not metadata:
                    try:
                        metadata = await self.photo_provider.extract_metadata(canonical_url, job_dir)
                        if metadata:
                            provider = self.photo_provider
                    except Exception as exc:
                        if not gallery_empty_error:
                            gallery_empty_error = exc

                # Fallback to TikWM provider if native providers failed or returned no slides
                if not metadata:
                    try:
                        metadata = await self.tikwm_provider.extract_metadata(canonical_url, job_dir)
                        if metadata:
                            provider = self.tikwm_provider
                    except Exception as exc:
                        logger.warning(f"TikWM photo fallback failed for {canonical_url}: {exc}")

                if (not metadata or not metadata.items) and gallery_empty_error:
                    raise gallery_empty_error



            elif canonical_type == "video":
                # Primary for /video/: yt-dlp
                provider = self.yt_dlp
                yt_error: Exception | None = None
                try:
                    metadata = await self.yt_dlp.extract_metadata(canonical_url, job_dir)
                except Exception as exc:
                    yt_error = exc

                # Fallback to TikWM video provider if yt-dlp failed or returned no usable items
                if not metadata or not metadata.items:
                    try:
                        metadata = await self.tikwm_video_provider.extract_metadata(canonical_url, job_dir)
                        if metadata and metadata.items:
                            provider = self.tikwm_video_provider
                    except Exception as exc:
                        logger.warning(f"TikWM video fallback failed for {canonical_url}: {exc}")

                if not metadata or not metadata.items:
                    if yt_error:
                        raise yt_error
                    raise DownloadError(
                        "yt-dlp tidak menghasilkan metadata untuk canonical URL /video/.",
                        user_friendly_message="Video TikTok sementara tidak dapat diproses. Silakan coba kembali.",
                    )
            else:
                # Unclassified path
                metadata = await self.yt_dlp.extract_metadata(canonical_url, job_dir)
                provider = self.yt_dlp
                if not metadata:
                    try:
                        metadata = await self.gallery_dl.extract_metadata(canonical_url, job_dir)
                        if metadata:
                            provider = self.gallery_dl
                    except Exception:
                        metadata = None
                if not metadata:
                    try:
                        metadata = await self.photo_provider.extract_metadata(canonical_url, job_dir)
                        if metadata:
                            provider = self.photo_provider
                    except Exception:
                        metadata = None
                if not metadata:
                    try:
                        metadata = await self.tikwm_provider.extract_metadata(canonical_url, job_dir)
                        if metadata:
                            provider = self.tikwm_provider
                    except Exception:
                        metadata = None
                if not metadata:
                    try:
                        metadata = await self.tikwm_video_provider.extract_metadata(canonical_url, job_dir)
                        if metadata:
                            provider = self.tikwm_video_provider
                    except Exception:
                        metadata = None

            if not metadata or not metadata.items:
                raise DownloadError(
                    "Link TikTok tidak dapat dipahami sebagai video maupun postingan foto.",
                    user_friendly_message="konten tidak dapat diproses. pastikan link masih aktif, bersifat publik, dan dapat dibuka.",
                )

        return ExtractedMetadataResult(
            canonical_url=canonical_url,
            provider=provider,
            metadata=metadata,
        )

    async def _download_audio_file(self, music_url: str, job_dir: Path) -> Path | None:
        """Download background music file to job_dir."""
        try:
            audio_path = job_dir / "audio_source.mp3"
            proxy = self.settings.TIKTOK_PROXY_URL or None
            headers = {"User-Agent": "Mozilla/5.0"}
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers, proxy=proxy) as client:
                resp = await client.get(music_url)
                resp.raise_for_status()
                with open(audio_path, "wb") as f:
                    f.write(resp.content)
            if audio_path.exists() and audio_path.stat().st_size > 0:
                return audio_path
        except Exception as e:
            logger.warning(f"Failed to download audio from {music_url}: {e}")
        return None

    async def download_content(
        self,
        snapshot: JobDownloadSnapshot,
        provider: DownloaderProvider,
        metadata: MediaContentMetadata,
        job_dir: Path,
    ) -> DownloadedContentResult:
        """Download physical files and optionally render slideshow video if selected."""
        items = list(snapshot.items)
        if items and all(item.status == "sent" or item.gateway_message_id for item in items):
            total_existing = sum(item.source_size_bytes or 0 for item in items)
            return DownloadedContentResult(items=(), source_size_bytes=total_existing)

        canonical_url = snapshot.canonical_url or snapshot.original_url
        updated_metadata = await provider.download_content(canonical_url, metadata, job_dir)

        # Check if user selected Video mode for Photo content
        selected_mode = getattr(snapshot, "selected_mode", None)
        music_url = getattr(snapshot, "music_url", None) or getattr(metadata, "music_url", None)

        if selected_mode == "video" and metadata.content_type == "photo":
            logger.info(f"Rendering photo slideshow to video for job {snapshot.id}")
            audio_path = None
            if music_url:
                audio_path = await self._download_audio_file(music_url, job_dir)

            photo_paths = [item.local_path for item in updated_metadata.items if item.local_path]
            video_out_path = job_dir / "slideshow_rendered.mp4"
            duration = metadata.duration_seconds or 0

            success = await render_slideshow_video(
                image_paths=photo_paths,
                audio_path=audio_path,
                output_path=video_out_path,
                duration_seconds=duration,
            )

            if success and video_out_path.exists() and video_out_path.stat().st_size > 0:
                v_size = video_out_path.stat().st_size
                downloaded_items = [
                    DownloadedItemResult(
                        position=1,
                        media_type="video",
                        source_url=None,
                        local_filename=str(video_out_path.resolve()),
                        source_size_bytes=v_size,
                    )
                ]
                return DownloadedContentResult(
                    items=tuple(downloaded_items),
                    source_size_bytes=v_size,
                )
            else:
                logger.warning("Slideshow video rendering failed, falling back to individual photos.")

        # Default photo or normal video flow
        total_source_size = 0
        items_dict = {item.position: item for item in items}
        downloaded_items: list[DownloadedItemResult] = []

        for item_meta in updated_metadata.items:
            snapshot_item = items_dict.get(item_meta.position)

            if snapshot_item and (snapshot_item.status == "sent" or snapshot_item.gateway_message_id):
                total_source_size += snapshot_item.source_size_bytes or 0
                continue

            if item_meta.local_path and os.path.exists(item_meta.local_path):
                size = os.path.getsize(item_meta.local_path)
                total_source_size += size
                downloaded_items.append(
                    DownloadedItemResult(
                        position=item_meta.position,
                        media_type=item_meta.media_type,
                        source_url=item_meta.source_url,
                        local_filename=str(item_meta.local_path),
                        source_size_bytes=size,
                    )
                )
            else:
                raise DownloadError(
                    f"File fisik hasil download untuk posisi {item_meta.position} tidak ditemukan di disk.",
                    user_friendly_message="Gagal mengunduh file media. File tidak ditemukan.",
                )

        return DownloadedContentResult(
            items=tuple(downloaded_items),
            source_size_bytes=total_source_size,
        )
