import logging
import zoneinfo
from datetime import UTC, datetime, tzinfo

from app.config import get_settings

logger = logging.getLogger(__name__)


def to_local_timezone(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    settings = get_settings()

    tz: tzinfo

    try:
        tz = zoneinfo.ZoneInfo(settings.APP_TIMEZONE)
    except zoneinfo.ZoneInfoNotFoundError:
        logger.warning(f"Timezone {settings.APP_TIMEZONE} not found, falling back to Asia/Jakarta")
        try:
            tz = zoneinfo.ZoneInfo("Asia/Jakarta")
        except zoneinfo.ZoneInfoNotFoundError:
            tz = UTC

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)

    return dt.astimezone(tz)

def format_local_datetime(dt: datetime | None, format_str: str = "%d %b %Y, %H:%M") -> str:
    if dt is None:
        return "-"
    local_dt = to_local_timezone(dt)
    if not local_dt:
        return "-"
    return f"{local_dt.strftime(format_str)} WIB"

def format_local_datetime_seconds(dt: datetime | None) -> str:
    return format_local_datetime(dt, "%d %b %Y, %H:%M:%S")
