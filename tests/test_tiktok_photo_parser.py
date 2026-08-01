import json

from app.downloader.tiktok_photo_provider import (
    extract_item_id_from_url,
    extract_json_script_blobs,
    extract_photo_urls,
    find_photo_post_payload,
    is_challenge_page,
    parse_tiktok_photo_post_html,
)


def test_extract_item_id_from_url() -> None:
    assert extract_item_id_from_url("https://www.tiktok.com/@ade_meliora/photo/7668360024648846599") == "7668360024648846599"
    assert extract_item_id_from_url("https://www.tiktok.com/@user/video/7123456789012345678") == "7123456789012345678"
    assert extract_item_id_from_url("https://www.tiktok.com/api/item/detail/?itemId=7668360024648846599") == "7668360024648846599"


def test_script_attributes_different_order_and_extra_attrs() -> None:
    html = """
    <html>
    <head>
        <script type="application/json" id="__UNIVERSAL_DATA_FOR_REHYDRATION__" nonce="random123" async>{"test": 1}</script>
        <script data-info="sigi" id="SIGI_STATE" type="application/json">{"test": 2}</script>
    </head>
    </html>
    """
    blobs = extract_json_script_blobs(html)
    assert len(blobs) >= 2
    blob_dict = {b[0]: b[1] for b in blobs if b[0]}
    assert "__UNIVERSAL_DATA_FOR_REHYDRATION__" in blob_dict
    assert "SIGI_STATE" in blob_dict
    assert json.loads(blob_dict["__UNIVERSAL_DATA_FOR_REHYDRATION__"]) == {"test": 1}
    assert json.loads(blob_dict["SIGI_STATE"]) == {"test": 2}


def test_parse_tiktok_photo_post_rehydration() -> None:
    fake_json_data = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "7668360024648846599",
                        "imagePost": {
                            "images": [
                                {
                                    "imageURL": {
                                        "urlList": [
                                            "https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/image1_low.jpeg",
                                            "https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/image1_high.jpeg",
                                        ]
                                    }
                                },
                                {
                                    "imageURL": {
                                        "urlList": [
                                            "https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/image2_high.jpeg"
                                        ]
                                    }
                                },
                            ]
                        },
                    }
                }
            }
        }
    }

    html = f"""
    <html>
    <head>
        <script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">{json.dumps(fake_json_data)}</script>
    </head>
    <body></body>
    </html>
    """

    metadata = parse_tiktok_photo_post_html(html, "https://www.tiktok.com/@ade_meliora/photo/7668360024648846599")
    assert metadata is not None
    assert metadata.content_type == "photo"
    assert len(metadata.items) == 2
    assert metadata.items[0].position == 1
    assert metadata.items[0].source_url == "https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/image1_high.jpeg"
    assert metadata.items[1].position == 2
    assert metadata.items[1].source_url == "https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/image2_high.jpeg"


def test_parse_tiktok_photo_post_sigi_state() -> None:
    fake_sigi = {
        "ItemModule": {
            "7668360024648846599": {
                "id": "7668360024648846599",
                "images": [
                    {"urlList": ["https://p16-sign-va.tiktokcdn.com/imgA_1.png", "https://p16-sign-va.tiktokcdn.com/imgA_best.png"]},
                    {"urlList": ["https://p16-sign-va.tiktokcdn.com/imgB_best.png"]},
                ],
            }
        }
    }

    html = f"""
    <html>
    <head>
        <script id="SIGI_STATE" type="application/json">{json.dumps(fake_sigi)}</script>
    </head>
    </html>
    """
    metadata = parse_tiktok_photo_post_html(html, "https://www.tiktok.com/@ade_meliora/photo/7668360024648846599")
    assert metadata is not None
    assert metadata.content_type == "photo"
    assert len(metadata.items) == 2
    assert metadata.items[0].source_url == "https://p16-sign-va.tiktokcdn.com/imgA_best.png"


def test_payload_with_avatar_and_recommendation_filtering() -> None:
    data = {
        "author": {
            "id": "avatar_12345",
            "avatarThumb": {"urlList": ["https://p16-sign-va.tiktokcdn.com/avatar.jpg"]},
        },
        "recommendations": [
            {
                "id": "9999999999999999999",
                "imagePost": {
                    "images": [
                        {"imageURL": {"urlList": ["https://p16-sign-va.tiktokcdn.com/recom1.jpg"]}}
                    ]
                },
            }
        ],
        "itemStruct": {
            "id": "7668360024648846599",
            "imagePost": {
                "images": [
                    {"imageURL": {"urlList": ["https://p16-sign-va.tiktokcdn.com/target_post.jpg"]}}
                ]
            },
        },
    }

    payload = find_photo_post_payload(data, expected_item_id="7668360024648846599")
    assert payload is not None
    assert str(payload.get("id")) == "7668360024648846599"

    urls = extract_photo_urls(payload)
    assert urls == ["https://p16-sign-va.tiktokcdn.com/target_post.jpg"]


def test_expected_item_id_mismatch() -> None:
    data = {
        "itemStruct": {
            "id": "1111111111111111111",
            "imagePost": {
                "images": [
                    {"imageURL": {"urlList": ["https://p16-sign-va.tiktokcdn.com/wrong.jpg"]}}
                ]
            },
        }
    }
    payload = find_photo_post_payload(data, expected_item_id="7668360024648846599")
    assert payload is None


def test_slide_order_and_deduplication() -> None:
    payload = {
        "imagePost": {
            "images": [
                {"imageURL": {"urlList": ["https://p16-sign-va.tiktokcdn.com/slide1_low.jpg", "https://p16-sign-va.tiktokcdn.com/slide1_high.jpg"]}},
                {"imageURL": {"urlList": ["https://p16-sign-va.tiktokcdn.com/slide1_high.jpg"]}},  # Duplicate of slide 1
                {"imageURL": {"urlList": ["https://p16-sign-va.tiktokcdn.com/slide2_high.jpg"]}},
            ]
        }
    }
    urls = extract_photo_urls(payload)
    assert urls == [
        "https://p16-sign-va.tiktokcdn.com/slide1_high.jpg",
        "https://p16-sign-va.tiktokcdn.com/slide2_high.jpg",
    ]


def test_is_challenge_page_detection() -> None:
    html_challenge = """
    <html>
    <head><title>TikTok - Make Your Day</title></head>
    <body><div id="captcha">verify captcha</div></body>
    </html>
    """
    assert is_challenge_page(html_challenge, has_valid_payload=False) is True
    assert is_challenge_page(html_challenge, has_valid_payload=True) is False

    html_normal = """
    <html>
    <head><title>Ade Meliora on TikTok</title></head>
    <body><div>Normal page content</div></body>
    </html>
    """
    assert is_challenge_page(html_normal, has_valid_payload=False) is False
