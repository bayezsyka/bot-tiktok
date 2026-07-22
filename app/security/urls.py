import ipaddress
import re
from urllib.parse import urlparse

import httpx

ALLOWED_TIKTOK_DOMAINS: set[str] = {
    "tiktok.com",
    "www.tiktok.com",
    "m.tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
}

URL_REGEX = re.compile(
    r"https?://(?:www\.|m\.|vm\.|vt\.)?tiktok\.com/[^\s]+"
)


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
    elif not digits.startswith("62"):
        # If it doesn't start with 62 and wasn't converted above, it might be invalid Indonesian number or foreign
        # But if it already starts with 62 we keep it
        pass

    if digits.startswith("62") and 10 <= len(digits) <= 15:
        return digits

    return None


def parse_lid_mapping(mapping_str: str | None = None) -> dict[str, str]:
    """
    Parse FARROS_WA_LID_MAP environment string into a dictionary of {LID: 628...}.
    Format: FARROS_WA_LID_MAP=84306181542117:628xxxxxxxxxx,12345678901234:628yyyyyyyyyy
    - Separates pairs using comma
    - Separates LID and number using colon
    - Only accepts digits for LID
    - Only accepts destination numbers formatted starting with 62 and length 10-15 digits
    - Ignores invalid entries safely
    - Does NOT log mapping
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

        # Only accept digits for LID
        if not lid_part or not lid_part.isdigit():
            continue

        # Only accept destination number format 62 and length 10-15 digits
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



def is_safe_hostname(hostname: str) -> bool:

    """Check hostname against SSRF targets (private IPs, localhost, cloud metadata endpoints)."""
    if not hostname:
        return False

    hostname = hostname.lower().strip()
    if hostname in ("localhost", "localhost.localdomain", "0.0.0.0", "127.0.0.1", "::1"):
        return False

    try:
        # Check if hostname is directly an IP
        ip_obj = ipaddress.ip_address(hostname)
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_multicast or ip_obj.is_reserved:
            return False
        # Specifically check AWS/cloud metadata address
        if str(ip_obj) == "169.254.169.254":
            return False
    except ValueError:
        pass

    # Ensure hostname ends with one of allowed domains
    if hostname not in ALLOWED_TIKTOK_DOMAINS and not any(
        hostname.endswith("." + domain) for domain in ALLOWED_TIKTOK_DOMAINS
    ):
        return False

    return True


def check_url_security(url_str: str) -> bool:
    """Validate scheme and domain for a single URL string."""
    try:
        parsed = urlparse(url_str)
        if parsed.scheme.lower() != "https":
            return False
        if not parsed.hostname or not is_safe_hostname(parsed.hostname):
            return False
        return True
    except Exception:
        return False


is_safe_tiktok_url = check_url_security


def extract_tiktok_url(text: str) -> str | None:
    """Extract the first valid TikTok HTTPS URL from text message."""
    if not text:
        return None

    # Find all potential URLs in text
    words = text.split()
    for word in words:
        # Strip punctuation at end if attached
        clean_word = word.strip(".,!?;:\"'()[]{}<>")
        if check_url_security(clean_word):
            return clean_word

    # Fallback to regex search
    matches = URL_REGEX.findall(text)
    for match in matches:
        clean_match = match.strip(".,!?;:\"'()[]{}<>")
        if check_url_security(clean_match):
            return str(clean_match)

    return None


async def resolve_canonical_tiktok_url(url_str: str, max_redirects: int = 5) -> str | None:
    """
    Follow up to `max_redirects` redirects safely to obtain the canonical TikTok URL.
    Verifies every target along the chain against SSRF and domain allowlist.
    """
    if not check_url_security(url_str):
        return None

    current_url = url_str

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
                # If HEAD fails or forbidden, try GET with stream=True or stop and return current_url if already canonical
                break

            if response.status_code in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    break

                # Resolve relative redirects if any (though TikTok issues absolute)
                from urllib.parse import urljoin
                next_url = urljoin(current_url, location)

                # Validate the target redirect URL
                if not check_url_security(next_url):
                    return None

                current_url = next_url
            else:
                break

    return current_url if check_url_security(current_url) else None
