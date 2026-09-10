from dataclasses import dataclass
from pathlib import Path

from app.downloader.metadata import MediaContentMetadata
from app.downloader.providers import DownloaderProvider


@dataclass(frozen=True)
class ItemProcessingSnapshot:
    id: int
    position: int
    media_type: str
    status: str
    gateway_message_id: str | None
    local_filename: str | None
    source_size_bytes: int | None = None
    final_size_bytes: int | None = None
    source_url: str | None = None


@dataclass(frozen=True)
class JobDownloadSnapshot:
    id: str
    original_url: str
    canonical_url: str | None
    platform: str
    items: tuple[ItemProcessingSnapshot, ...]
    selected_mode: str | None = None
    music_url: str | None = None


@dataclass(frozen=True)
class ExtractedMetadataResult:
    canonical_url: str
    provider: DownloaderProvider
    metadata: MediaContentMetadata


@dataclass(frozen=True)
class DownloadedItemResult:
    position: int
    media_type: str
    source_url: str | None
    local_filename: str
    source_size_bytes: int


@dataclass(frozen=True)
class DownloadedContentResult:
    items: tuple[DownloadedItemResult, ...]
    source_size_bytes: int


@dataclass(frozen=True)
class ProcessedItemResult:
    item_id: int
    status: str
    local_filename: str | None = None
    final_size_bytes: int | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ProcessedJobResult:
    items: tuple[ProcessedItemResult, ...]
    final_size_bytes: int


@dataclass(frozen=True)
class MissingLocalFile:
    item_id: int
    position: int


@dataclass(frozen=True)
class ProcessingSnapshot:
    job_id: str
    media_count: int
    items: tuple[ItemProcessingSnapshot, ...]
    missing_local_files: tuple[MissingLocalFile, ...]


def local_path_size(path: str | Path) -> int:
    return Path(path).stat().st_size
