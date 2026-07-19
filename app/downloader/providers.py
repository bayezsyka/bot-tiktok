from abc import ABC, abstractmethod
from pathlib import Path

from app.downloader.metadata import TikTokContentMetadata


class DownloaderProvider(ABC):
    @abstractmethod
    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        """Check if this provider can handle or recognize the content type of the URL."""
        pass

    @abstractmethod
    async def extract_metadata(self, canonical_url: str, job_dir: Path) -> TikTokContentMetadata | None:
        """Extract metadata (title, items, duration, content_type) from URL."""
        pass

    @abstractmethod
    async def download_content(
        self, canonical_url: str, metadata: TikTokContentMetadata, job_dir: Path
    ) -> TikTokContentMetadata:
        """Download physical media files to job_dir and populate local_path in metadata items."""
        pass
