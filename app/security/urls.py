import ipaddress
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse, urlunparse

import httpx

ALLOWED_TIKTOK_DOMAINS: set[str] = {
    "tiktok.com",
    "www.tiktok.com",
    "m.tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
}

ALLOWED_INSTAGRAM_DOMAINS: set[str] = {
    "instagram.com",
    "www.instagram.com",
    "m.instagram.com",
}

TIKTOK_URL_REGEX = re.compile(
    r"https?://(?:www\.|m\.|vm\.|vt\.)?tiktok\.com/[^\s]+"
)

INSTAGRAM_URL_REGEX = re.compile(
    r"https?://(?:www\.|m\.)?instagram\.com/(?:reels?|p)/[A-Za-z0-9_-]+[^\s]*"
)

# Instagram shortcode path prefixes and the content hint they map to.
INSTAGRAM_REEL_PATHS = ("reel", "reels")
INSTAGRAM_POST_PATHS = ("p",)
INSTAGRAM_VALID_PATHS = INSTAGRAM_REEL_PATHS + INSTAGRAM_POST_PATHS
INSTAGRAM_SHORTCODE_REGEX = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass
class ExtractedMediaUrl:
    original_url: str
    platform: str  # 'tiktok' or 'instagram'
    content_hint: str  # 'video' or 'photo' or 'reel' or 'post'
    canonical_url: str | None = None


def normalize_phone_number(raw_number: str) -> str | None:
    """
    Normalize WhatsApp phone number to 628xxxxxxxxxx format.
    Returns None if invalid.
    """
    if not raw_number:
        return None

    if "@lid" in str(raw_number).lower() or str(raw_number).lower().endswith("lid"):
        return None

    # Strip all non-digit characters
    digits = re.sub(r"\D", "", str(raw_number))

    if not digits:
        return None

    if digits.startswith("08"):
        digits = "62" + digits[1:]
    elif digits.startswith("8"):
        digits = "62" + digits
    elif digits.startswith("6208"):
        digits = "62" + digits[3:]

    if digits.startswith("62") and 10 <= len(digits) <= 15:
        return digits

    return None


def parse_lid_mapping(mapping_str: str | None = None) -> dict[str, str]:
    """
    Parse FARROS_WA_LID_MAP environment string into a dictionary of {LID: 628...}.
    Format: FARROS_WA_LID_MAP=84306181542117:628xxxxxxxxxx,12345678901234:628yyyyyyyyyy
    """
    if mapping_str is None:
        from app.config import get_settings
        mapping_str = get_settings().FARROS_WA_LID_MAP

    result: dict[str, str] = {}
    if not mapping_str or not isinstance(mapping_str, str):
        return result

    pairs = mapping_str.split(",")
    for pair in pairs:
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        parts = pair.split(":")
        if len(parts) != 2:
            continue
        lid_part = parts[0].strip()
        num_part = parts[1].strip()

        if not lid_part or not lid_part.isdigit():
            continue

        if not num_part or not num_part.isdigit() or not num_part.startswith("62") or not (10 <= len(num_part) <= 15):
            continue

        result[lid_part] = num_part

    return result


def resolve_lid_to_phone(lid: str, mapping_str: str | None = None) -> str | None:
    """Resolve an LID string to its mapped 628... phone number using FARROS_WA_LID_MAP."""
    if not lid:
        return None
    digits_lid = re.sub(r"\D", "", str(lid))
    if not digits_lid:
        return None
    mapping = parse_lid_mapping(mapping_str)
    return mapping.get(digits_lid)


def is_safe_hostname(hostname: str) -> tuple[bool, str | None]:
    """
    Check hostname against SSRF targets and allowlists.
    Returns (is_safe, platform_name).
    """
    if not hostname:
        return False, None

    hostname = hostname.lower().strip()
    if hostname in ("localhost", "localhost.localdomain", "0.0.0.0", "127.0.0.1", "::1"):
        return False, None

    try:
        ip_obj = ipaddress.ip_address(hostname)
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_multicast or ip_obj.is_reserved:
            return False, None
        if str(ip_obj) == "169.254.169.254":
            return False, None
    except ValueError:
        pass

    # Check TikTok allowlist
    if hostname in ALLOWED_TIKTOK_DOMAINS or any(hostname.endswith("." + d) for d in ALLOWED_TIKTOK_DOMAINS):
        return True, "tiktok"

    # Check Instagram allowlist
    if hostname in ALLOWED_INSTAGRAM_DOMAINS or any(hostname.endswith("." + d) for d in ALLOWED_INSTAGRAM_DOMAINS):
        return True, "instagram"

    return False, None


def check_url_security(url_str: str) -> tuple[bool, str | None]:
    """
    Validate scheme, credentials, port, domain, and path security.
    Returns (is_safe, platform_name).
    """
    try:
        parsed = urlparse(url_str)
        if parsed.scheme.lower() != "https":
            return False, None
        if parsed.username or parsed.password:
            return False, None
        if parsed.port and parsed.port not in (80, 443):
            return False, None

        hostname = parsed.hostname
        if not hostname:
            return False, None

        is_safe, platform = is_safe_hostname(hostname)
        if not is_safe or not platform:
            return False, None

        # Platform specific path checks
        path = parsed.path.lower()
        if platform == "instagram":
            # Instagram path MUST be /reel/{shortcode}, /reels/{shortcode}, or /p/{shortcode}
            parts = [p for p in path.split("/") if p]
            if len(parts) < 2 or parts[0] not in INSTAGRAM_VALID_PATHS:
                return False, None
            # Reject invalid endpoints
            if any(p in ("accounts", "login", "explore", "direct", "stories", "live") for p in parts):
                return False, None
            # Reject empty shortcode
            shortcode = parts[1].strip()
            if not shortcode:
                return False, None
            # Reject shortcode with characters other than letters, digits, underscore, dash
            if not INSTAGRAM_SHORTCODE_REGEX.match(shortcode):
                return False, None

        return True, platform
    except Exception:
        return False, None


def is_safe_tiktok_url(url_str: str) -> bool:
    is_safe, platform = check_url_security(url_str)
    return bool(is_safe and platform == "tiktok")


def sanitize_media_url(url_str: str) -> str:
    """Clean fragment and strip unneeded trailing punctuation from URL."""
    try:
        clean_url = url_str.strip(".,!?;:\"'()[]{}<>")
        parsed = urlparse(clean_url)
        # Reconstruct URL without fragment
        sanitized = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ""))
        return sanitized
    except Exception:
        return url_str.strip(".,!?;:\"'()[]{}<>")


def _instagram_content_hint(clean_url: str) -> str:
    """Determine Instagram content hint ('reel' or 'post') from URL path."""
    try:
        path = urlparse(clean_url).path.lower()
    except Exception:
        return "reel"
    path_parts = [p for p in path.split("/") if p]
    if path_parts and path_parts[0] in INSTAGRAM_POST_PATHS:
        return "post"
    return "reel"


def extract_supported_media_url(text: str) -> ExtractedMediaUrl | None:
    """Extract the first valid TikTok or Instagram HTTPS URL from text message."""
    if not text:
        return None

    words = text.split()
    for word in words:
        clean_word = sanitize_media_url(word)
        is_safe, platform = check_url_security(clean_word)
        if is_safe and platform:
            if platform == "instagram":
                hint = _instagram_content_hint(clean_word)
            else:
                hint = "video"
            return ExtractedMediaUrl(
                original_url=clean_word,
                platform=platform,
                content_hint=hint,
            )

    # Fallback to regex matches
    for match in TIKTOK_URL_REGEX.findall(text):
        clean_match = sanitize_media_url(match)
        is_safe, platform = check_url_security(clean_match)
        if is_safe and platform == "tiktok":
            return ExtractedMediaUrl(original_url=clean_match, platform="tiktok", content_hint="video")

    for match in INSTAGRAM_URL_REGEX.findall(text):
        clean_match = sanitize_media_url(match)
        is_safe, platform = check_url_security(clean_match)
        if is_safe and platform == "instagram":
            return ExtractedMediaUrl(
                original_url=clean_match,
                platform="instagram",
                content_hint=_instagram_content_hint(clean_match),
            )

    return None


def extract_tiktok_url(text: str) -> str | None:
    """Backward compatibility wrapper to extract TikTok URL string."""
    res = extract_supported_media_url(text)
    if res and res.platform == "tiktok":
        return res.original_url
    return None


async def resolve_canonical_media_url(url_str: str, max_redirects: int = 5) -> str | None:
    """
    Follow redirects safely to obtain canonical media URL.
    Verifies target redirect URL against SSRF and allowlist.
    """
    is_safe, platform = check_url_security(url_str)
    if not is_safe or not platform:
        return None

    current_url = url_str

    # Instagram does not strictly require redirect resolution via HEAD
    if platform == "instagram":
        return current_url

    async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
        for _ in range(max_redirects):
            try:
                response = await client.head(
                    current_url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                    },
                )
            except Exception:
                break

            if response.status_code in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    break

                next_url = urljoin(current_url, location)
                next_safe, _ = check_url_security(next_url)
                if not next_safe:
                    return None

                current_url = next_url
            else:
                break

    safe_end, _ = check_url_security(current_url)
    return current_url if safe_end else None


async def resolve_canonical_tiktok_url(url_str: str, max_redirects: int = 5) -> str | None:
    """Backward compatibility alias for resolve_canonical_media_url."""
    return await resolve_canonical_media_url(url_str, max_redirects=max_redirects)
