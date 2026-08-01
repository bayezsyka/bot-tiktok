import logging
import os
from pathlib import Path

from app.downloader.dtos import (
    DownloadedContentResult,
    DownloadedItemResult,
    ExtractedMetadataResult,
    JobDownloadSnapshot,
)
from app.downloader.exceptions import DownloadError
from app.downloader.gallery_dl_tiktok_photo_provider import GalleryDlTikTokPhotoProvider
from app.downloader.instagram_provider import InstagramReelProvider
from app.downloader.metadata import MediaContentMetadata
from app.downloader.providers import DownloaderProvider
from app.downloader.tiktok_photo_provider import TikTokPhotoProvider
from app.downloader.yt_dlp_provider import YtDlpProvider
from app.security.urls import resolve_canonical_tiktok_url

logger = logging.getLogger(__name__)


class DownloaderService:
    def __init__(self) -> None:
        self.yt_dlp = YtDlpProvider()
        self.gallery_dl = GalleryDlTikTokPhotoProvider()
        self.photo_provider = TikTokPhotoProvider()
        self.ig_provider = InstagramReelProvider()

    async def extract_metadata(
        self, snapshot: JobDownloadSnapshot, job_dir: Path
    ) -> ExtractedMetadataResult:
        """
        Resolve the canonical URL and extract metadata without opening or using a DB session.
        """
        platform = snapshot.platform or "tiktok"

        if not snapshot.canonical_url:
            canonical_url = await resolve_canonical_tiktok_url(snapshot.original_url)
            if not canonical_url:
                raise DownloadError(
                    "Link media tidak valid atau tidak aman.",
                    user_friendly_message="Link media tidak valid, berisiko, atau tidak dapat diakses.",
                )
        else:
            canonical_url = snapshot.canonical_url

        provider: DownloaderProvider
        metadata: MediaContentMetadata | None = None

        if platform == "instagram":
            provider = self.ig_provider
            metadata = await self.ig_provider.extract_metadata(canonical_url, job_dir)
            if not metadata or not metadata.items:
                raise DownloadError(
                    "Link Instagram Reels tidak dapat diproses.",
                    user_friendly_message="reels instagram tidak dapat diakses. pastikan akun dan kontennya bersifat publik.",
                )
        else:
            # TikTok platform
            is_photo_url = "/photo/" in canonical_url

            if is_photo_url:
                # Primary for /photo/: gallery-dl
                metadata = await self.gallery_dl.extract_metadata(canonical_url, job_dir)
                provider = self.gallery_dl

                # Fallback to HTML parser if gallery-dl returned None
                if not metadata:
                    metadata = await self.photo_provider.extract_metadata(canonical_url, job_dir)
                    provider = self.photo_provider
            else:
                # Primary for video / unclassified: yt-dlp first
                metadata = await self.yt_dlp.extract_metadata(canonical_url, job_dir)
                provider = self.yt_dlp

                if not metadata:
                    metadata = await self.gallery_dl.extract_metadata(canonical_url, job_dir)
                    provider = self.gallery_dl

                if not metadata:
                    metadata = await self.photo_provider.extract_metadata(canonical_url, job_dir)
                    provider = self.photo_provider

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

    async def download_content(
        self,
        snapshot: JobDownloadSnapshot,
        provider: DownloaderProvider,
        metadata: MediaContentMetadata,
        job_dir: Path,
    ) -> DownloadedContentResult:
        """Download physical files without opening or using a DB session."""
        items = list(snapshot.items)
        if items and all(item.status == "sent" or item.gateway_message_id for item in items):
            total_existing = sum(item.source_size_bytes or 0 for item in items)
            return DownloadedContentResult(items=(), source_size_bytes=total_existing)

        canonical_url = snapshot.canonical_url or snapshot.original_url

        updated_metadata = await provider.download_content(canonical_url, metadata, job_dir)

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

        # Verify no non-sent item is left without a local_filename
        downloaded_positions = {item.position for item in downloaded_items}
        for item in items:
            if item.status != "sent" and not item.gateway_message_id:
                if item.position in downloaded_positions:
                    continue
                if item.local_filename and os.path.exists(item.local_filename):
                    total_source_size += item.source_size_bytes or os.path.getsize(item.local_filename)
                    continue
                raise DownloadError(
                    f"Item posisi {item.position} tidak memiliki file hasil unduhan lokal.",
                    user_friendly_message="Gagal mengunduh seluruh file media.",
                )

        return DownloadedContentResult(
            items=tuple(downloaded_items),
            source_size_bytes=total_source_size,
        )

    async def extract_and_prepare_job(self, *args: object, **kwargs: object) -> object:
        raise RuntimeError(
            "extract_and_prepare_job was removed from the production path. "
            "Use extract_metadata(snapshot, job_dir) and persist metadata in a short DB session."
        )

    async def download_job_content(self, *args: object, **kwargs: object) -> object:
        raise RuntimeError(
            "download_job_content was removed from the production path. "
            "Use download_content(snapshot, provider, metadata, job_dir) without a DB session."
        )
