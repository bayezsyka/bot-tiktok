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
