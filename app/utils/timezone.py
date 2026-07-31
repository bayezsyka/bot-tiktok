import zoneinfo
from datetime import datetime

from app.config import get_settings


def to_local_timezone(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    settings = get_settings()
    tz = zoneinfo.ZoneInfo(settings.APP_TIMEZONE)
    return dt.astimezone(tz)

def format_local_datetime(dt: datetime | None, format_str: str = "%d %b %Y %H:%M") -> str:
    if dt is None:
        return "-"
    local_dt = to_local_timezone(dt)
    return local_dt.strftime(format_str) # type: ignore
