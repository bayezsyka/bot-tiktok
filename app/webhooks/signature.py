import hashlib
import hmac
import time
from collections.abc import Mapping

from fastapi import HTTPException, status

from app.config import get_settings


def verify_webhook_signature(
    headers: Mapping[str, str],
    raw_body: bytes,
    secret: str,
    tolerance_seconds: int = 300,
) -> bool:
    """
    Verify X-FWAG headers and HMAC-SHA256 signature using constant-time comparison.
    Signature formula: HMAC-SHA256(secret, timestamp + "." + raw_body)
    """
    event_hdr = headers.get("x-fwag-event") or headers.get("X-FWAG-Event")
    event_id = headers.get("x-fwag-event-id") or headers.get("X-FWAG-Event-Id")
    timestamp_str = headers.get("x-fwag-timestamp") or headers.get("X-FWAG-Timestamp")
    signature = headers.get("x-fwag-signature") or headers.get("X-FWAG-Signature")

    if not event_hdr or not event_id or not timestamp_str or not signature:
        return False

    try:
        ts = int(timestamp_str)
    except ValueError:
        return False

    now = int(time.time())
    if abs(now - ts) > tolerance_seconds:
        return False

    body_str = raw_body.decode("utf-8", errors="replace")
    message = f"{timestamp_str}.{body_str}".encode()
    expected_sig = hmac.new(
        secret.encode("utf-8"),
        message,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(expected_sig.lower(), signature.lower())


def validate_webhook_headers_and_signature(headers: Mapping[str, str], raw_body: bytes) -> tuple[str, str, int]:
    """
    Validate headers/signature and return (event_type, event_id, timestamp).
    Raises HTTP 401 if invalid.
    """
    settings = get_settings()
    secret = settings.FARROS_WA_WEBHOOK_SECRET
    if not secret:
        # If secret is not set in local dev, check or raise 401 depending on setup. But we enforce check.
        pass

    if not verify_webhook_signature(headers, raw_body, secret, settings.WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid signature or timestamp expired",
        )

    event_type = headers.get("x-fwag-event") or headers.get("X-FWAG-Event") or ""
    event_id = headers.get("x-fwag-event-id") or headers.get("X-FWAG-Event-Id") or ""
    timestamp_str = headers.get("x-fwag-timestamp") or headers.get("X-FWAG-Timestamp") or "0"

    return event_type, event_id, int(timestamp_str)
