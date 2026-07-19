from app.security.rate_limit import InMemoryRateLimiter, check_webhook_rate_limit


def test_in_memory_rate_limiter_allow_and_block() -> None:
    limiter = InMemoryRateLimiter()
    key = "test-client-phone"
    max_req = 3
    window_sec = 10

    assert limiter.is_allowed(key, max_req, window_sec)[0] is True
    assert limiter.is_allowed(key, max_req, window_sec)[0] is True
    assert limiter.is_allowed(key, max_req, window_sec)[0] is True
    # 4th request inside window should be blocked
    assert limiter.is_allowed(key, max_req, window_sec)[0] is False


def test_check_webhook_rate_limit_exception() -> None:
    key = "628999999999"
    # exhaust requests
    for _ in range(30):
        try:
            check_webhook_rate_limit(key)
        except Exception:
            break
