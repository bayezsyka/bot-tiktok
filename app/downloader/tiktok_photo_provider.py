import html
import json
import logging
import os
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings
from app.downloader.exceptions import (
    DownloadError,
    DownloadSizeLimitExceededError,
    TikTokChallengeError,
)
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.providers import DownloaderProvider

logger = logging.getLogger(__name__)

ALLOWED_IMAGE_DOMAINS = (
    ".tiktokcdn.com",
    ".byteoversea.com",
    ".ibyteimg.com",
    ".akamaized.net",
    ".tiktok.com",
    ".muscdn.com",
    "tiktokcdn.com",
    "byteoversea.com",
    "ibyteimg.com",
)

DEFAULT_TIKTOK_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    "Referer": "https://www.tiktok.com/",
}


class TikTokScriptTagParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.script_blobs: list[tuple[str | None, str]] = []
        self._in_script = False
        self._current_id: str | None = None
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "script":
            attr_dict = {k.lower(): (v or "") for k, v in attrs if k is not None}
            self._in_script = True
            self._current_id = attr_dict.get("id")
            self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._in_script:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "script" and self._in_script:
            text = "".join(self._current_text).strip()
            if text:
                self.script_blobs.append((self._current_id, text))
            self._in_script = False
            self._current_id = None
            self._current_text = []


def extract_json_script_blobs(html_content: str) -> list[tuple[str | None, str]]:
    """Extract script blobs (id, content) using a tolerant HTML parser."""
    if not html_content:
        return []
    parser = TikTokScriptTagParser()
    try:
        parser.feed(html_content)
    except Exception as e:
        logger.debug(f"HTMLParser warning (returning parsed blobs): {e}")

    blobs = list(parser.script_blobs)

    # Fallback regex if specific key scripts were not parsed
    found_ids = {b[0] for b in blobs if b[0]}
    for script_id in ("__UNIVERSAL_DATA_FOR_REHYDRATION__", "SIGI_STATE", "sigi-persisted-data"):
        if script_id not in found_ids:
            pattern = re.compile(
                rf'<script[^>]*\bid=["\']?{script_id}["\']?[^>]*>(.*?)</script>',
                re.DOTALL | re.IGNORECASE,
            )
            for m in pattern.findall(html_content):
                blobs.append((script_id, m))

    return blobs


def extract_item_id_from_url(canonical_url: str) -> str | None:
    """Extract numeric TikTok item ID from canonical URL."""
    if not canonical_url:
        return None
    match = re.search(r"/(?:photo|video|v)/(\d+)", canonical_url)
    if match:
        return match.group(1)
    match = re.search(r"[?&](?:itemId|item_id)=(\d+)", canonical_url)
    if match:
        return match.group(1)
    match = re.search(r"(\d{15,22})", canonical_url)
    if match:
        return match.group(1)
    return None


def find_photo_post_payload(data: Any, expected_item_id: str | None = None) -> dict[str, Any] | None:
    """
    Search JSON data for a photo post payload matching expected_item_id.
    Avoids picking recommendation thumbnails or author avatars.
    """
    if isinstance(data, dict):
        if expected_item_id and expected_item_id in data and isinstance(data[expected_item_id], dict):
            target = data[expected_item_id]
            if isinstance(target, dict) and ("imagePost" in target or "image_post_info" in target or "images" in target):
                return target

        d_id = str(data.get("id") or data.get("itemId") or data.get("aweme_id") or "")
        has_photo_keys = (
            "imagePost" in data
            or "image_post_info" in data
            or ("images" in data and isinstance(data["images"], list) and len(data["images"]) > 0)
        )

        if has_photo_keys:
            if expected_item_id:
                if d_id == expected_item_id:
                    return data
            else:
                if "avatar" not in d_id.lower() and "author" not in data:
                    return data

        for k, v in data.items():
            if k in ("author", "authorStats", "music", "stats", "suggestedWords", "recommendations"):
                continue
            if isinstance(v, (dict, list)):
                res = find_photo_post_payload(v, expected_item_id)
                if res:
                    return res

    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                res = find_photo_post_payload(item, expected_item_id)
                if res:
                    return res

    return None


def _is_allowed_image_url(url: str) -> bool:
    if not isinstance(url, str) or not url.startswith("http"):
        return False
    try:
        parsed = httpx.URL(url)
        host = parsed.host.lower()
        return any(host == domain or host.endswith(domain) for domain in ALLOWED_IMAGE_DOMAINS)
    except Exception:
        return False


def _get_best_url_from_list(url_list: list[Any]) -> str | None:
    valid_urls = [str(u) for u in url_list if isinstance(u, str) and _is_allowed_image_url(str(u))]
    if not valid_urls:
        return None
    return valid_urls[-1]


def extract_photo_urls(payload: dict[str, Any]) -> list[str]:
    """
    Extract photo slide URLs from photo post payload.
    Preserves slide order, picks highest quality URL, deduplicates without altering order.
    """
    if not isinstance(payload, dict):
        return []

    images_raw: list[Any] = []
    if "imagePost" in payload and isinstance(payload["imagePost"], dict):
        images_raw = payload["imagePost"].get("images") or payload["imagePost"].get("image_list") or []
    elif "image_post_info" in payload and isinstance(payload["image_post_info"], dict):
        images_raw = payload["image_post_info"].get("images") or []
    elif "images" in payload and isinstance(payload["images"], list):
        images_raw = payload["images"]

    if not isinstance(images_raw, list):
        return []

    slide_urls: list[str] = []

    for img in images_raw:
        if isinstance(img, dict):
            url_list: list[Any] | None = None
            if "imageURL" in img and isinstance(img["imageURL"], dict):
                url_list = img["imageURL"].get("urlList")
            elif "displayImageURL" in img and isinstance(img["displayImageURL"], dict):
                url_list = img["displayImageURL"].get("urlList")
            elif "display_image" in img and isinstance(img["display_image"], dict):
                url_list = img["display_image"].get("url_list")
            elif "urlList" in img and isinstance(img["urlList"], list):
                url_list = img["urlList"]
            elif "url_list" in img and isinstance(img["url_list"], list):
                url_list = img["url_list"]
            elif "imageURLList" in img and isinstance(img["imageURLList"], list):
                url_list = img["imageURLList"]

            if url_list:
                best_url = _get_best_url_from_list(url_list)
                if best_url:
                    slide_urls.append(best_url)
            else:
                direct_url = img.get("imageURL") or img.get("displayImageURL") or img.get("url")
                if isinstance(direct_url, str) and _is_allowed_image_url(direct_url):
                    slide_urls.append(direct_url)
        elif isinstance(img, str) and _is_allowed_image_url(img):
            slide_urls.append(img)

    seen: set[str] = set()
    ordered_unique_urls: list[str] = []
    for url in slide_urls:
        if url not in seen:
            seen.add(url)
            ordered_unique_urls.append(url)

    return ordered_unique_urls


def is_challenge_page(html_content: str, has_valid_payload: bool) -> bool:
    if has_valid_payload:
        return False
    if not html_content:
        return False

    title_match = re.search(r"<title>(.*?)</title>", html_content, re.IGNORECASE)
    title = title_match.group(1).strip() if title_match else ""
    is_generic_title = "TikTok - Make Your Day" in title or title == "TikTok"

    challenge_keywords = ["captcha", "challenge", "verify-bar", "sec_sdk", "whocovered", "robot", "human verification"]
    content_lower = html_content.lower()
    challenge_marker_found = any(kw in content_lower for kw in challenge_keywords)

    return is_generic_title or challenge_marker_found


def _load_netscape_cookies(cookies_file_path: str) -> httpx.Cookies | None:
    if not cookies_file_path or not os.path.exists(cookies_file_path) or not os.path.isfile(cookies_file_path):
        return None

    httpx_cookies = httpx.Cookies()
    loaded_count = 0

    try:
        with open(cookies_file_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception as e:
        logger.warning(f"Sanitized: Unable to read cookies file: {e}")
        return None

    for line_idx, line in enumerate(lines, start=1):
        line_str = line.strip()
        if not line_str:
            continue
        if line_str.startswith("#HttpOnly_"):
            line_str = line_str[len("#HttpOnly_"):]
        elif line_str.startswith("#") or line_str.startswith("//"):
            continue

        parts = line_str.split("\t")
        if len(parts) >= 7:
            domain, flag, path, secure, expiration, name, value = parts[:7]
            try:
                cookie_name = name.strip()
                cookie_val = value.strip()
                cookie_domain = domain.strip()
                cookie_path = path.strip() or "/"
                if cookie_name:
                    httpx_cookies.set(
                        cookie_name,
                        cookie_val,
                        domain=cookie_domain or "www.tiktok.com",
                        path=cookie_path,
                    )
                    loaded_count += 1
            except Exception as e:
                logger.warning(f"Sanitized: Skipping invalid cookie entry at line {line_idx}: {e}")
        else:
            logger.warning(f"Sanitized: Skipping malformed cookie line at line {line_idx}")

    if loaded_count > 0:
        return httpx_cookies
    return None


def parse_tiktok_photo_post_html(html_content: str, canonical_url: str) -> TikTokContentMetadata | None:
    if not html_content:
        return None

    item_id = extract_item_id_from_url(canonical_url)
    script_blobs = extract_json_script_blobs(html_content)

    for _, raw_json in script_blobs:
        try:
            clean_json = html.unescape(raw_json)
            data = json.loads(clean_json)
            payload = find_photo_post_payload(data, expected_item_id=item_id)
            if payload:
                urls = extract_photo_urls(payload)
                if urls:
                    items = [
                        TikTokMediaItemMetadata(
                            position=idx + 1,
                            source_url=url,
                            media_type="photo",
                        )
                        for idx, url in enumerate(urls)
                    ]
                    author = "TikTok Creator"
                    if isinstance(payload, dict) and isinstance(payload.get("author"), dict):
                        author = str(
                            payload["author"].get("nickname")
                            or payload["author"].get("uniqueId")
                            or "TikTok Creator"
                        )
                    title = "TikTok Photo Post"
                    if isinstance(payload, dict):
                        title = str(payload.get("desc") or payload.get("title") or "TikTok Photo Post")[:200]

                    return TikTokContentMetadata(
                        content_type="photo",
                        title=title,
                        author=author,
                        duration_seconds=0,
                        items=items,
                    )
        except Exception:
            continue

    return None


class TikTokPhotoProvider(DownloaderProvider):
    def __init__(self) -> None:
        self.settings = get_settings()

    async def _fetch_html(self, url: str, cookies: httpx.Cookies | None = None) -> str:
        try:
            async with httpx.AsyncClient(
                timeout=15.0,
                follow_redirects=True,
                headers=DEFAULT_TIKTOK_HEADERS,
                cookies=cookies,
                verify=True,
            ) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch canonical TikTok HTML: {e}")
            return ""

    async def _fetch_fallback_item_detail(
        self, item_id: str, canonical_url: str, cookies: httpx.Cookies | None
    ) -> TikTokContentMetadata | None:
        """Fallback to official web endpoints if main HTML returns a challenge or bootstrap shell."""
        if not item_id:
            return None

        endpoints = [
            f"https://www.tiktok.com/api/item/detail/?itemId={item_id}",
            f"https://www.tiktok.com/node/share/post/{item_id}",
        ]

        headers = {
            **DEFAULT_TIKTOK_HEADERS,
            "Accept": "application/json, text/plain, */*",
        }

        async with httpx.AsyncClient(
            timeout=15.0, follow_redirects=True, cookies=cookies, verify=True
        ) as client:
            for ep in endpoints:
                try:
                    resp = await client.get(ep, headers=headers)
                    if resp.status_code != 200:
                        continue
                    content_type = resp.headers.get("content-type", "")
                    if len(resp.content) > 5 * 1024 * 1024:
                        continue

                    if "json" in content_type:
                        data = resp.json()
                    else:
                        text = resp.text
                        script_blobs = extract_json_script_blobs(text)
                        data = None
                        for _, blob in script_blobs:
                            try:
                                blob_data = json.loads(html.unescape(blob))
                                if find_photo_post_payload(blob_data, expected_item_id=item_id):
                                    data = blob_data
                                    break
                            except Exception:
                                continue
                        if not data:
                            continue

                    payload = find_photo_post_payload(data, expected_item_id=item_id)
                    if not payload:
                        continue

                    urls = extract_photo_urls(payload)
                    if not urls:
                        continue

                    items = [
                        TikTokMediaItemMetadata(
                            position=idx + 1,
                            source_url=url,
                            media_type="photo",
                        )
                        for idx, url in enumerate(urls)
                    ]

                    author = "TikTok Creator"
                    if isinstance(payload, dict) and isinstance(payload.get("author"), dict):
                        author = str(
                            payload["author"].get("nickname")
                            or payload["author"].get("uniqueId")
                            or "TikTok Creator"
                        )

                    title = "TikTok Photo Post"
                    if isinstance(payload, dict):
                        title = str(payload.get("desc") or payload.get("title") or "TikTok Photo Post")[:200]

                    return TikTokContentMetadata(
                        content_type="photo",
                        title=title,
                        author=author,
                        duration_seconds=0,
                        items=items,
                    )
                except Exception as e:
                    logger.debug(f"Fallback endpoint {ep} failed: {e}")
                    continue

        return None

    async def can_handle(self, canonical_url: str, job_dir: Path) -> bool:
        metadata = await self.extract_metadata(canonical_url, job_dir)
        return bool(metadata and metadata.content_type == "photo" and len(metadata.items) > 0)

    async def extract_metadata(self, canonical_url: str, job_dir: Path) -> TikTokContentMetadata | None:
        item_id = extract_item_id_from_url(canonical_url) or "unknown"
        cookies_file = self.settings.TIKTOK_COOKIES_FILE
        cookies = _load_netscape_cookies(cookies_file)
        cookie_configured = bool(cookies is not None)

        html_content = await self._fetch_html(canonical_url, cookies=cookies)
        metadata = parse_tiktok_photo_post_html(html_content, canonical_url) if html_content else None

        strategy = "html_rehydration" if metadata else "none"
        challenge_detected = False

        if not metadata:
            challenge_detected = is_challenge_page(html_content, has_valid_payload=False)
            fallback_metadata = await self._fetch_fallback_item_detail(item_id, canonical_url, cookies)
            if fallback_metadata:
                metadata = fallback_metadata
                strategy = "fallback_api"
            elif challenge_detected:
                logger.warning(
                    f"TikTok challenge detected: platform=tiktok content_type=photo item_id={item_id} "
                    f"extraction_strategy=failed cookie_configured={cookie_configured} challenge_detected=true slide_count=0"
                )
                raise TikTokChallengeError(
                    message=f"TikTok challenge page encountered for item {item_id}",
                    user_friendly_message="TikTok sementara menolak akses downloader. Silakan coba kembali beberapa saat lagi.",
                )

        slide_count = len(metadata.items) if metadata else 0
        logger.info(
            f"TikTok photo extraction completed: platform=tiktok content_type=photo item_id={item_id} "
            f"extraction_strategy={strategy} cookie_configured={cookie_configured} challenge_detected={challenge_detected} slide_count={slide_count}"
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

        async with httpx.AsyncClient(
            timeout=30.0, follow_redirects=True, headers=headers, cookies=cookies, verify=True
        ) as client:
            for item in metadata.items:
                try:
                    resp = await client.get(item.source_url)
                    resp.raise_for_status()
                    content = resp.content
                except Exception as e:
                    self._cleanup_downloaded_photos(job_dir)
                    raise DownloadError(
                        f"Gagal mengunduh foto slide #{item.position}: {e}",
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
