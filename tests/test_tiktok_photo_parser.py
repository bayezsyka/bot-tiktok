import json

from app.downloader.tiktok_photo_provider import parse_tiktok_photo_post_html


def test_parse_tiktok_photo_post_rehydration() -> None:
    fake_json_data = {
        "__DEFAULT_SCOPE__": {
            "webapp.video-detail": {
                "itemInfo": {
                    "itemStruct": {
                        "id": "7123456789012345678",
                        "imagePost": {
                            "images": [
                                {
                                    "imageURL": {
                                        "urlList": [
                                            "https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/image1_high.jpeg",
                                            "https://p16-sign-va.tiktokcdn.com/tos-maliva-p-0068/image1_low.jpeg",
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

    metadata = parse_tiktok_photo_post_html(html, "https://www.tiktok.com/@test/video/7123456789012345678")
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
            "7123456789012345678": {
                "images": [
                    {"urlList": ["https://tiktok.com/imgA_1.png", "https://tiktok.com/imgA_best.png"]},
                    {"urlList": ["https://tiktok.com/imgB_best.png"]},
                ]
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
    metadata = parse_tiktok_photo_post_html(html, "https://vt.tiktok.com/ZSabc123/")
    assert metadata is not None
    assert metadata.content_type == "photo"
    assert len(metadata.items) == 2
    assert metadata.items[0].source_url == "https://tiktok.com/imgA_best.png"
