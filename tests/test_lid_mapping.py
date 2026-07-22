import json
from unittest.mock import AsyncMock, patch

import pytest
from app.config import get_settings
from app.database.repositories import AllowedNumberRepository, JobRepository
from app.security.urls import parse_lid_mapping, resolve_lid_to_phone
from app.webhooks.parser import parse_inbound_message
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from test_webhook_router import _make_signed_headers


def test_parser_detects_lid_from_remote_jid_even_if_data_from_only_digits() -> None:
    payload = {
        "data": {
            "from": "84306181542117",
            "remote_jid": "84306181542117@lid",
            "message": {
                "id": "msg-lid-test-1",
                "text": "https://www.tiktok.com/@creator/video/1234567890123456789",
            },
        }
    }
    parsed = parse_inbound_message(payload)
    assert parsed is not None
    assert parsed.is_lid is True
    assert parsed.lid_number == "84306181542117"


def test_parse_lid_mapping_and_resolve() -> None:
    # Valid mapping string with multiple entries
    mapping_str = "84306181542117:628111222333, 12345678901234:628999888777"
    parsed = parse_lid_mapping(mapping_str)
    assert parsed == {
        "84306181542117": "628111222333",
        "12345678901234": "628999888777",
    }
    assert resolve_lid_to_phone("84306181542117", mapping_str) == "628111222333"
    assert resolve_lid_to_phone("12345678901234@lid", mapping_str) == "628999888777"
    assert resolve_lid_to_phone("99999999", mapping_str) is None

    # Invalid entries ignored
    invalid_map = "abc:628111222333, 84306181542117:12345, 99999999:628123456789"
    parsed_invalid = parse_lid_mapping(invalid_map)
    assert parsed_invalid == {"99999999": "628123456789"}


@pytest.mark.asyncio
async def test_webhook_lid_with_mapping_creates_job_with_628(client: AsyncClient, test_db: AsyncSession) -> None:
    num_repo = AllowedNumberRepository(test_db)
    await num_repo.create_number(name="Mapped User", phone_number="628111222333")
    await test_db.commit()

    tiktok_url = "https://www.tiktok.com/@creator/video/1234567890123456789"
    body = json.dumps({
        "data": {
            "from": "84306181542117",
            "remote_jid": "84306181542117@lid",
            "message": {
                "id": "msg-mapped-lid-01",
                "text": f"Download {tiktok_url}",
            },
        }
    }).encode("utf-8")

    headers = _make_signed_headers(body, event_id="evt-mapped-lid-01")

    with patch.object(get_settings(), "FARROS_WA_LID_MAP", "84306181542117:628111222333"), \
         patch("app.webhooks.router._send_initial_reply_background", new_callable=AsyncMock) as mock_reply:
        resp = await client.post("/webhooks/farros-wa", content=body, headers=headers)
        assert resp.status_code == 200
        assert resp.json().get("message") == "Job queued successfully"
        mock_reply.assert_called_once_with("628111222333", "msg-mapped-lid-01")

    job_repo = JobRepository(test_db)
    job = await job_repo.get_by_inbound_message_id("msg-mapped-lid-01")
    assert job is not None
    assert job.status == "queued"
    assert job.sender_number == "628111222333"


@pytest.mark.asyncio
async def test_webhook_lid_without_mapping_returns_200_without_job(client: AsyncClient, test_db: AsyncSession) -> None:
    tiktok_url = "https://www.tiktok.com/@creator/video/1234567890123456789"
    body = json.dumps({
        "data": {
            "from": "99999999",
            "remote_jid": "99999999@lid",
            "message": {
                "id": "msg-unmapped-lid-01",
                "text": f"Download {tiktok_url}",
            },
        }
    }).encode("utf-8")

    headers = _make_signed_headers(body, event_id="evt-unmapped-lid-01")

    with patch.object(get_settings(), "FARROS_WA_LID_MAP", "84306181542117:628111222333"), \
         patch("app.webhooks.router._send_initial_reply_background", new_callable=AsyncMock) as mock_reply:
        resp = await client.post("/webhooks/farros-wa", content=body, headers=headers)
        assert resp.status_code == 200
        assert resp.json().get("message") == "Ignored LID sender without routable phone number"
        mock_reply.assert_not_called()

    job_repo = JobRepository(test_db)
    job = await job_repo.get_by_inbound_message_id("msg-unmapped-lid-01")
    assert job is None


@pytest.mark.asyncio
async def test_webhook_normal_sender_unchanged(client: AsyncClient, test_db: AsyncSession) -> None:
    num_repo = AllowedNumberRepository(test_db)
    await num_repo.create_number(name="Normal User", phone_number="628111222333")
    await test_db.commit()

    tiktok_url = "https://www.tiktok.com/@creator/video/1234567890123456789"
    body = json.dumps({
        "data": {
            "from": "628111222333",
            "message": {
                "id": "msg-normal-sender-01",
                "text": f"Download {tiktok_url}",
            },
        }
    }).encode("utf-8")

    headers = _make_signed_headers(body, event_id="evt-normal-sender-01")

    with patch("app.webhooks.router._send_initial_reply_background", new_callable=AsyncMock) as mock_reply:
        resp = await client.post("/webhooks/farros-wa", content=body, headers=headers)
        assert resp.status_code == 200
        assert resp.json().get("message") == "Job queued successfully"
        mock_reply.assert_called_once_with("628111222333", "msg-normal-sender-01")

    job_repo = JobRepository(test_db)
    job = await job_repo.get_by_inbound_message_id("msg-normal-sender-01")
    assert job is not None
    assert job.status == "queued"
    assert job.sender_number == "628111222333"
