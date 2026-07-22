import logging
import os
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import DownloadItem, DownloadJob
from app.downloader.exceptions import DownloadError
from app.downloader.providers import DownloaderProvider
from app.downloader.tiktok_photo_provider import TikTokPhotoProvider
from app.downloader.yt_dlp_provider import YtDlpProvider
from app.security.urls import resolve_canonical_tiktok_url

logger = logging.getLogger(__name__)


class DownloaderService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.yt_dlp = YtDlpProvider()
        self.photo_provider = TikTokPhotoProvider()

    async def extract_and_prepare_job(
        self, job: DownloadJob, job_dir: Path
    ) -> tuple[DownloaderProvider, Any]:
        """
        Detect content type using providers, extract metadata, and populate DownloadItem records in DB.
        Returns (selected_provider, metadata).
        """
        if not job.canonical_url:
            canonical_url = await resolve_canonical_tiktok_url(job.original_url)
            if not canonical_url:
                raise DownloadError(
                    "Link TikTok tidak valid atau tidak aman.",
                    user_friendly_message="Link TikTok tidak valid, berisiko, atau tidak dapat diakses.",
                )
            job.canonical_url = canonical_url
        else:
            canonical_url = job.canonical_url

        # Try yt-dlp first for video
        metadata = await self.yt_dlp.extract_metadata(canonical_url, job_dir)
        provider: DownloaderProvider = self.yt_dlp

        # If yt-dlp didn't return metadata, try photo provider
        if not metadata:
            metadata = await self.photo_provider.extract_metadata(canonical_url, job_dir)
            provider = self.photo_provider

        if not metadata or not metadata.items:
            raise DownloadError(
                "Link TikTok tidak dapat dipahami sebagai video maupun postingan foto.",
                user_friendly_message="Konten TikTok tidak dapat diproses. pastikan link masih aktif, bersifat publik, dan dapat dibuka.",
            )

        # Update Job fields
        job.content_type = metadata.content_type
        job.media_count = len(metadata.items)
        job.duration_seconds = metadata.duration_seconds

        # Create or update DownloadItem records (avoid duplicate when retrying/recovering)
        existing_items = {item.position: item for item in job.items}
        for item_meta in metadata.items:
            db_item = existing_items.get(item_meta.position)
            if db_item:
                if db_item.status != "sent" and not db_item.gateway_message_id:
                    db_item.media_type = item_meta.media_type
                    db_item.source_url = item_meta.source_url
            else:
                item = DownloadItem(
                    job_id=job.id,
                    position=item_meta.position,
                    media_type=item_meta.media_type,
                    status="pending",
                    source_url=item_meta.source_url,
                )
                job.items.append(item)
                self.session.add(item)

        await self.session.flush()
        return provider, metadata

    async def download_job_content(
        self,
        job: DownloadJob,
        provider: DownloaderProvider,
        metadata: Any,
        job_dir: Path,
    ) -> None:
        """Download physical files for the job using the selected provider and update local items."""
        items = list(job.items) if job.items else []
        if items and all(item.status == "sent" or item.gateway_message_id for item in items):
            return

        canonical_url = job.canonical_url or job.original_url

        updated_metadata = await provider.download_content(canonical_url, metadata, job_dir)

        total_source_size = 0
        items_dict = {item.position: item for item in (job.items or [])}

        for item_meta in updated_metadata.items:
            db_item = items_dict.get(item_meta.position)
            if not db_item:
                db_item = DownloadItem(
                    job_id=job.id,
                    position=item_meta.position,
                    media_type=item_meta.media_type,
                    status="pending",
                    source_url=item_meta.source_url,
                )
                job.items.append(db_item)
                self.session.add(db_item)
                items_dict[item_meta.position] = db_item


            if db_item.status == "sent" or db_item.gateway_message_id:
                total_source_size += (db_item.source_size_bytes or 0)
                continue

            if item_meta.local_path and os.path.exists(item_meta.local_path):
                size = os.path.getsize(item_meta.local_path)
                db_item.local_filename = str(item_meta.local_path)
                db_item.source_size_bytes = size
                total_source_size += size
            else:
                raise DownloadError(
                    f"File fisik hasil download untuk posisi {item_meta.position} tidak ditemukan di disk.",
                    user_friendly_message="Gagal mengunduh file media dari TikTok. File tidak ditemukan."
                )

        # Verify no non-sent item is left without a local_filename
        for item in (job.items or []):
            if item.status != "sent" and not item.gateway_message_id:
                if not item.local_filename or not os.path.exists(item.local_filename):
                    raise DownloadError(
                        f"Item posisi {item.position} tidak memiliki file hasil unduhan lokal.",
                        user_friendly_message="Gagal mengunduh seluruh file media dari TikTok."
                    )

        job.source_size_bytes = total_source_size
        await self.session.flush()
