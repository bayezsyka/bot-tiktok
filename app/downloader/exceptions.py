class DownloadError(Exception):
    """Base exception for all download and metadata extraction errors."""
    def __init__(self, message: str, user_friendly_message: str = "") -> None:
        super().__init__(message)
        self.message = message
        self.user_friendly_message = user_friendly_message or message


class ContentNotSupportedError(DownloadError):
    """Raised when URL points to live stream, profile, playlist, or private/deleted content."""
    pass


class DownloadSizeLimitExceededError(DownloadError):
    """Raised when source media exceeds MAX_SOURCE_DOWNLOAD_MB limit."""
    pass


class DownloadTimeoutError(DownloadError):
    """Raised when download process exceeds JOB_TIMEOUT_SECONDS or subprocess timeout."""
    pass
