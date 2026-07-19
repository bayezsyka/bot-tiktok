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
