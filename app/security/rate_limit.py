import time
from collections import defaultdict

from fastapi import HTTPException, status

from app.config import get_settings


class InMemoryRateLimiter:
    """
    Thread-safe/async-safe in-memory rate limiter using sliding window timestamps.
    Used for webhook sender phone numbers and admin login protection.
    """
    def __init__(self) -> None:
        # key -> list of timestamps
        self.requests: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, key: str, max_requests: int, window_seconds: int) -> tuple[bool, int]:
        now = time.time()
        window_start = now - window_seconds

        # Filter out old timestamps
        current_timestamps = [ts for ts in self.requests[key] if ts > window_start]
        self.requests[key] = current_timestamps

        if len(current_timestamps) >= max_requests:
            retry_after = int(current_timestamps[0] + window_seconds - now)
            return False, max(retry_after, 1)

        self.requests[key].append(now)
        return True, 0

    def reset(self, key: str) -> None:
        if key in self.requests:
            del self.requests[key]


# Singleton instances
webhook_limiter = InMemoryRateLimiter()
login_limiter = InMemoryRateLimiter()


def check_webhook_rate_limit(phone_number: str) -> None:
    settings = get_settings()
    window_seconds = settings.RATE_LIMIT_WINDOW_MINUTES * 60
    allowed, retry_after = webhook_limiter.is_allowed(phone_number, settings.RATE_LIMIT_REQUESTS, window_seconds)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Try again in {retry_after} seconds.",
        )


def check_login_rate_limit(ip_address: str) -> None:
    # Allow 10 login attempts per 15 minutes per IP
    allowed, retry_after = login_limiter.is_allowed(f"login:{ip_address}", max_requests=10, window_seconds=900)
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many login attempts. Please wait {retry_after} seconds.",
        )
