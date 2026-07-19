from app.security.urls import extract_tiktok_url, is_safe_tiktok_url, normalize_phone_number


def test_extract_tiktok_url() -> None:
    text1 = "halo min tolong download https://vt.tiktok.com/ZS12345ab/ dong makasih"
    assert extract_tiktok_url(text1) == "https://vt.tiktok.com/ZS12345ab/"

    text2 = "Lihat video ini di https://www.tiktok.com/@username/video/7123456789012345678 seru bgt"
    assert extract_tiktok_url(text2) == "https://www.tiktok.com/@username/video/7123456789012345678"

    text3 = "nggak ada link tiktok di sini cuma ada https://google.com"
    assert extract_tiktok_url(text3) is None


def test_is_safe_tiktok_url() -> None:
    assert is_safe_tiktok_url("https://www.tiktok.com/@creator/video/123456789") is True
    assert is_safe_tiktok_url("https://vt.tiktok.com/ZSabcde12/") is True
    assert is_safe_tiktok_url("https://vm.tiktok.com/ZSabcde12/") is True
    assert is_safe_tiktok_url("https://t.tiktok.com/12345/") is True

    # Malicious or SSRF URLs must be rejected
    assert is_safe_tiktok_url("http://127.0.0.1:8000/admin") is False
    assert is_safe_tiktok_url("https://evil-tiktok.com/phishing") is False
    assert is_safe_tiktok_url("file:///etc/passwd") is False


def test_normalize_phone_number() -> None:
    assert normalize_phone_number("08123456789") == "628123456789"
    assert normalize_phone_number("+62 812-3456-789") == "628123456789"
    assert normalize_phone_number("628123456789") == "628123456789"
    assert normalize_phone_number("8123456789") == "628123456789"

    # Too short or invalid
    assert normalize_phone_number("12345") is None
    assert normalize_phone_number("abc") is None
