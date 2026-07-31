from datetime import UTC, datetime

from app.utils.timezone import (
    format_local_datetime,
    format_local_datetime_seconds,
    to_local_timezone,
)


def test_timezone_wib_conversion():
    dt_utc = datetime(2023, 10, 27, 10, 0, 0, tzinfo=UTC)
    local_dt = to_local_timezone(dt_utc)
    assert local_dt is not None
    assert local_dt.hour == 17  # 10:00 UTC -> 17:00 WIB (+7)

def test_naive_datetime_assumes_utc():
    # A naive datetime should be assumed to be UTC before conversion
    dt_naive = datetime(2023, 10, 27, 10, 0, 0)
    local_dt = to_local_timezone(dt_naive)
    assert local_dt is not None
    assert local_dt.hour == 17

def test_format_local_datetime_appends_wib():
    dt_utc = datetime(2023, 10, 27, 10, 30, 0, tzinfo=UTC)
    formatted = format_local_datetime(dt_utc)
    assert "WIB" in formatted
    assert "27 Oct 2023, 17:30 WIB" == formatted

def test_format_local_datetime_seconds_appends_wib():
    dt_utc = datetime(2023, 10, 27, 10, 30, 15, tzinfo=UTC)
    formatted = format_local_datetime_seconds(dt_utc)
    assert "WIB" in formatted
    assert "27 Oct 2023, 17:30:15 WIB" == formatted

def test_none_handling():
    assert to_local_timezone(None) is None
    assert format_local_datetime(None) == "-"
    assert format_local_datetime_seconds(None) == "-"
