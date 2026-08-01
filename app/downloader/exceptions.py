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


class TikTokChallengeError(DownloadError):
    """Raised when TikTok anti-bot / challenge page is detected."""

    def __init__(
        self,
        message: str = "TikTok challenge detected",
        user_friendly_message: str = "TikTok sementara menolak akses downloader. Silakan coba kembali beberapa saat lagi.",
    ) -> None:
        super().__init__(message, user_friendly_message=user_friendly_message)
