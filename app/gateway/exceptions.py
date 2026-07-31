class GatewayError(Exception):
    """Base class for all Farros WA Gateway client errors."""
    pass


class GatewayTimeoutError(GatewayError):
    """Raised when request to gateway times out."""
    pass


class GatewayNetworkError(GatewayError):
    """Raised when connection/network failure occurs while contacting gateway."""
    pass


class GatewayResponseError(GatewayError):
    """Raised when gateway returns an error HTTP status code."""
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"Gateway returned HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class GatewayRateLimitError(GatewayError):
    """Raised when gateway returns HTTP 429 (rate limit exceeded).

    Callers should stop the current batch and wait before retrying.
    """
    def __init__(self, retry_after: float | None = None, message: str = "") -> None:
        detail = "Gateway rate limit exceeded (429)"
        if retry_after is not None:
            detail += f", retry after {retry_after}s"
        if message:
            detail += f": {message}"
        super().__init__(detail)
        self.retry_after = retry_after
