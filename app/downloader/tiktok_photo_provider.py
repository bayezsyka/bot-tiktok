import html
import json
import logging
import re
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings
from app.downloader.exceptions import DownloadError
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.providers import DownloaderProvider

logger = logging.getLogger(__name__)

REHYDRATION_REGEX = re.compile(
    r'<script\s+id="[^"]*__UNIVERSAL_DATA_FOR_REHYDRATION__[^"]*"\s+type="application/json">(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)
SIGI_STATE_REGEX = re.compile(
    r'<script\s+id="[^"]*SIGI_STATE[^"]*"\s+type="application/json">(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


def _find_images_in_dict(data: Any) -> list[str]:
    """Recursively search for image/slideshow URL lists inside TikTok JSON structures."""
    results: list[str] = []
    if isinstance(data, dict):
        # Check explicit imagePost structure
        if "imagePost" in data and isinstance(data["imagePost"], dict):
            images = data["imagePost"].get("images") or data["imagePost"].get("image_list")
            if isinstance(images, list):
                for img in images:
                    if isinstance(img, dict):
                        url = (
                            img.get("imageURL")
                            or img.get("displayImageURL")
                            or img.get("imageURLList", [None])[0]
                        )
                        if isinstance(url, dict) and "urlList" in url:
                            url_list = url["urlList"]
                            if isinstance(url_list, list) and url_list:
                                results.append(str(url_list[0]))
                        elif isinstance(url, str) and url.startswith("http"):
                            results.append(url)
                    elif isinstance(img, str) and img.startswith("http"):
                        results.append(img)
                if results:
                    return results

        # Check images direct array
        if "images" in data and isinstance(data["images"], list) and len(data["images"]) > 0:
            for item in data["images"]:
                if isinstance(item, dict):
                    urls = item.get("urlList") or item.get("imageURLList")
                    if isinstance(urls, list) and urls:
                        results.append(str(urls[-1]))  # highest quality
                    elif isinstance(item.get("displayImageURL"), dict):
                        urls = item["displayImageURL"].get("urlList")
                        if isinstance(urls, list) and urls:
                            results.append(str(urls[-1]))
            if results:
                return results

        # Search nested dict keys
        for v in data.values():
            found = _find_images_in_dict(v)
            if found:
                return found

    elif isinstance(data, list):
        for item in data:
            found = _find_images_in_dict(item)
            if found:
                return found

    return results


def parse_tiktok_photo_post_html(html_content: str, canonical_url: str) -> TikTokContentMetadata | None:
    """
    Parse TikTok HTML to extract photo post/slideshow images and metadata.
    Tested independently without network access.
    """
    if not html_content:
        return None

    # Try extracting JSON script blobs
    json_blobs = []
    for match in REHYDRATION_REGEX.findall(html_content):
        json_blobs.append(match)
    for match in SIGI_STATE_REGEX.findall(html_content):
        json_blobs.append(match)

    for raw_json in json_blobs:
        try:
            clean_json = html.unescape(raw_json)
            data = json.loads(clean_json)
            images = _find_images_in_dict(data)
            if images:
                # Deduplicate while preserving order
                seen = set()
                ordered_images = []
                for img_url in images:
                    if img_url not in seen and img_url.startswith("http"):
                        seen.add(img_url)
                        ordered_images.append(img_url)

                if ordered_images:
                    items = [
                        TikTokMediaItemMetadata(
                            position=idx + 1,
                            source_url=url,
                            media_type="photo",
                        )
                        for idx, url in enumerate(ordered_images)
                    ]
                    return TikTokContentMetadata(
                        content_type="photo",
                        title="TikTok Photo Post",
                        author="TikTok Creator",
                        duration_seconds=0,
                        items=items,
                    )
        except Exception:
            continue

    return None


class TikTokPhotoProvider(DownloaderProvider):
    def __init__(self) -> None:
        self.settings = get_settings()

    async def _fetch_html(self, url: str) -> str:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
        }
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                resp = await client.get(url, headers=headers)
                resp.raise_for_status()
                return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch canonical TikTok HTML: {e}")
            return ""

    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        metadata = await self.extract_metadata(canonical_url, job_dir)
        return bool(metadata and metadata.content_type == "photo" and len(metadata.items) > 0)

    async def extract_metadata(self, canonical_url: str, job_dir: Path) -> TikTokContentMetadata | None:
        html_content = await self._fetch_html(canonical_url)
        if not html_content:
            return None
        return parse_tiktok_photo_post_html(html_content, canonical_url)

    async def download_content(
        self, canonical_url: str, metadata: TikTokContentMetadata, job_dir: Path
    ) -> TikTokContentMetadata:
        if not metadata.items:
            raise DownloadError("Foto tidak ditemukan pada postingan ini.")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Referer": "https://www.tiktok.com/",
        }

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=headers) as client:
            for item in metadata.items:
                try:
                    resp = await client.get(item.source_url)
                    resp.raise_for_status()
                    content = resp.content
                except Exception as e:
                    raise DownloadError(f"Gagal mengunduh foto slide #{item.position}: {e}") from e

                # Verify file size
                if len(content) > self.settings.MAX_SOURCE_DOWNLOAD_MB * 1024 * 1024:
                    raise DownloadError(f"Ukuran foto slide #{item.position} melebihi batas unduhan.")

                # Validate magic bytes / signature
                ext = "jpg"
                if content.startswith(b"\xff\xd8\xff"):
                    ext = "jpg"
                elif content.startswith(b"\x89PNG\r\n\x1a\n"):
                    ext = "png"
                elif content[:4] == b"RIFF" and content[8:12] == b"WEBP":
                    ext = "webp"
                elif content.startswith(b"GIF8"):
                    ext = "gif"
                else:
                    # If unrecognized, still try saving or raise error if totally invalid
                    if len(content) < 100:
                        raise DownloadError(f"File foto slide #{item.position} rusak atau bukan gambar valid.")

                local_filename = job_dir / f"photo_{item.position:03d}.{ext}"
                with open(local_filename, "wb") as f:
                    f.write(content)

                item.local_path = str(local_filename.resolve())

        return metadata
