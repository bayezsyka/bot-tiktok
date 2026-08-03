from app.security.urls import extract_supported_media_url

# --- Tests 1-7: URL parser acceptance/rejection & hints ---

def test_p_shortcode_accepted() -> None:
    res = extract_supported_media_url("https://www.instagram.com/p/DbgFWkXMQXa/")
    assert res is not None
    assert res.platform == "instagram"
    assert res.content_hint == "post"


def test_reel_shortcode_still_accepted() -> None:
    res = extract_supported_media_url("https://www.instagram.com/reel/C1234567890/")
    assert res is not None
    assert res.platform == "instagram"
    assert res.content_hint == "reel"


def test_reels_shortcode_still_accepted() -> None:
    res = extract_supported_media_url("https://www.instagram.com/reels/C1234567890/")
    assert res is not None
    assert res.platform == "instagram"
    assert res.content_hint == "reel"


def test_p_without_shortcode_rejected() -> None:
    res = extract_supported_media_url("https://www.instagram.com/p/")
    assert res is None


def test_instagram_profile_rejected() -> None:
    res = extract_supported_media_url("https://www.instagram.com/username")
    assert res is None


def test_p_produces_post_hint() -> None:
    res = extract_supported_media_url("cek https://www.instagram.com/p/Abc_-123/ ya")
    assert res is not None
    assert res.content_hint == "post"


def test_reel_produces_reel_hint() -> None:
    res = extract_supported_media_url("cek https://www.instagram.com/reel/Xyz456/ ya")
    assert res is not None
    assert res.content_hint == "reel"


def test_p_with_invalid_shortcode_chars_rejected() -> None:
    res = extract_supported_media_url("https://www.instagram.com/p/AB!code/")
    assert res is None


def test_p_shortcode_with_dash_underscore_accepted() -> None:
    res = extract_supported_media_url("https://www.instagram.com/p/Abc-123_xyz/")
    assert res is not None
    assert res.content_hint == "post"


def test_p_in_text_extracted() -> None:
    text = "tolong downloadin https://www.instagram.com/p/DbgFWkXMQXa/?igshid=abc dong"
    res = extract_supported_media_url(text)
    assert res is not None
    assert res.platform == "instagram"
    assert res.content_hint == "post"


def test_accounts_login_rejected() -> None:
    assert extract_supported_media_url("https://www.instagram.com/accounts/login/") is None


def test_stories_rejected() -> None:
    assert extract_supported_media_url("https://www.instagram.com/stories/user/123/") is None


def test_explore_rejected() -> None:
    assert extract_supported_media_url("https://www.instagram.com/explore/") is None


def test_direct_rejected() -> None:
    assert extract_supported_media_url("https://www.instagram.com/direct/t/123/") is None


def test_root_instagram_rejected() -> None:
    assert extract_supported_media_url("https://www.instagram.com/") is None
