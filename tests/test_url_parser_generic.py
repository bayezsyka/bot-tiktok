from app.security.urls import check_url_security, extract_supported_media_url


def test_valid_media_urls() -> None:
    valid_cases = [
        ("https://www.tiktok.com/@username/video/7123456789012345678", "tiktok"),
        ("https://tiktok.com/@username/video/7123456789012345678", "tiktok"),
        ("https://vt.tiktok.com/ZS8XXXXXX/", "tiktok"),
        ("https://vm.tiktok.com/ZS8XXXXXX/", "tiktok"),
        ("https://www.tiktok.com/@username/photo/7123456789012345678", "tiktok"),
        ("https://www.instagram.com/reel/C1234567890/", "instagram"),
        ("https://instagram.com/reel/C1234567890/", "instagram"),
        ("https://www.instagram.com/reels/C1234567890/", "instagram"),
        ("https://instagram.com/reels/C1234567890/?igsh=MWF2&utm_source=copy", "instagram"),
        ("https://m.instagram.com/reel/C1234567890/", "instagram"),
        ("Tolong downloadin dong https://www.instagram.com/reel/C1234567890/?igsh=test ya bang", "instagram"),
        ("cek video tiktok ini: https://vm.tiktok.com/ZS8XXXXXX/ bagus banget!", "tiktok"),
    ]

    for text, expected_platform in valid_cases:
        res = extract_supported_media_url(text)
        assert res is not None, f"Failed to extract valid URL from text: {text}"
        assert res.platform == expected_platform, f"Expected platform {expected_platform}, got {res.platform} for {text}"


def test_invalid_media_urls() -> None:
    invalid_cases = [
        "http://www.instagram.com/reel/C1234567890/",  # HTTP scheme
        "https://www.instagram.com/username",  # Profile page
        "https://www.instagram.com/stories/username/123456",  # Story
        "https://www.instagram.com/direct/t/123456",  # DM
        "https://www.instagram.com/explore/",  # Explore
        "https://instagram.com.evil.com/reel/C1234567890/",  # Subdomain trick
        "https://evilinstagram.com/reel/C1234567890/",  # Evil domain
        "https://localhost/reel/C1234567890/",  # Localhost
        "https://127.0.0.1/reel/C1234567890/",  # IP literal
        "https://www.instagram.com:8443/reel/C1234567890/",  # Custom port
        "https://user:pass@www.instagram.com/reel/C1234567890/",  # Username/password URL
        "https://www.instagram.com/reel/",  # Empty shortcode
        "https://youtube.com/watch?v=dQw4w9WgXcQ",  # Unsupported platform
    ]

    for text in invalid_cases:
        res = extract_supported_media_url(text)
        assert res is None, f"Should have rejected invalid URL in text: {text}"
        is_safe, platform = check_url_security(text)
        assert not is_safe or not platform, f"check_url_security should reject: {text}"
