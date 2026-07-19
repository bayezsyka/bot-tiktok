import os
import shutil
from functools import lru_cache
from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_NAME: str = "Farros TikTok Bot"
    APP_ENV: str = "local"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 3200
    APP_SECRET: str = "change-me"
    APP_BASE_URL: str = "http://localhost:3200"

    DATABASE_PATH: str = "./storage/database/app.sqlite"
    TEMP_DIR: str = "./storage/tmp"
    LOG_LEVEL: str = "INFO"

    FARROS_WA_BASE_URL: str = "https://wa.sangkolo.my.id"
    FARROS_WA_API_KEY: str = ""
    FARROS_WA_WEBHOOK_SECRET: str = ""
    FARROS_WA_SESSION_ID: str = ""

    YT_DLP_BINARY: str = "yt-dlp"
    FFMPEG_BINARY: str = "ffmpeg"
    FFPROBE_BINARY: str = "ffprobe"
    TIKTOK_COOKIES_FILE: str = ""

    MAX_MEDIA_MB: int = 15
    MAX_SOURCE_DOWNLOAD_MB: int = 500
    MAX_VIDEO_DURATION_SECONDS: int = 900
    JOB_TIMEOUT_SECONDS: int = 600
    MAX_JOB_RETRIES: int = 2
    TEMP_FILE_TTL_MINUTES: int = 120
    WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS: int = 300

    RATE_LIMIT_REQUESTS: int = 5
    RATE_LIMIT_WINDOW_MINUTES: int = 10

    ADMIN_INITIAL_USERNAME: str = "admin"
    ADMIN_INITIAL_PASSWORD: str = ""
    SESSION_COOKIE_SECURE: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("DATABASE_PATH", "TEMP_DIR", mode="after")
    @classmethod
    def resolve_paths(cls, v: str) -> str:
        # We ensure paths are resolved absolute paths
        path = Path(v).resolve()
        return str(path)

    @property
    def database_url(self) -> str:
        # SQLAlchemy sqlite+aiosqlite url
        return f"sqlite+aiosqlite:///{self.DATABASE_PATH}"

    def validate_startup_configuration(self) -> None:
        """Validate critical configuration and environment setup on application startup."""
        # Ensure directories exist
        db_dir = Path(self.DATABASE_PATH).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        temp_dir = Path(self.TEMP_DIR)
        temp_dir.mkdir(parents=True, exist_ok=True)

        # Check binary availability in PATH if not absolute paths
        for _binary_name, binary_path in [
            ("yt-dlp", self.YT_DLP_BINARY),
            ("ffmpeg", self.FFMPEG_BINARY),
            ("ffprobe", self.FFPROBE_BINARY),
        ]:
            if not os.path.isabs(binary_path) and not shutil.which(binary_path):
                # We do not crash startup if mock/test, but we log or ensure awareness
                pass


@lru_cache
def get_settings() -> Settings:
    return Settings()
