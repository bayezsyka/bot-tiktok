import hashlib
import hmac
import json
import time
from unittest.mock import AsyncMock, patch

import pytest
from app.database.repositories import AllowedNumberRepository, JobRepository
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


def _make_signed_headers(body_bytes: bytes, secret: str = "test-webhook-secret-123456", event_type: str = "message.inbound", event_id: str = "evt-test-01") -> dict:
    ts = str(int(time.time()))
    msg = f"{ts}.{body_bytes.decode('utf-8')}".encode()
    sig = hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return {
        "X-FWAG-Event": event_type,
        "X-FWAG-Event-Id": event_id,
        "X-FWAG-Timestamp": ts,
        "X-FWAG-Signature": sig,
        "Content-Type": "application/json",
    }


@pytest.mark.asyncio
async def test_webhook_invalid_signature(client: AsyncClient) -> None:
    body = b'{"hello":"world"}'
    headers = {
        "X-FWAG-Event": "message.inbound",
        "X-FWAG-Event-Id": "evt-bad-sig",
        "X-FWAG-Timestamp": str(int(time.time())),
        "X-FWAG-Signature": "bad-signature-123",
        "Content-Type": "application/json",
    }
    resp = await client.post("/webhooks/farros-wa", content=body, headers=headers)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_webhook_ignore_non_message_inbound(client: AsyncClient) -> None:
    body = b'{"hello":"world"}'
    headers = _make_signed_headers(body, event_type="message.status")
    resp = await client.post("/webhooks/farros-wa", content=body, headers=headers)
    assert resp.status_code == 200
    assert resp.json().get("message") == "Ignored non-message.inbound event"


@pytest.mark.asyncio
async def test_webhook_ignore_from_me_and_group(client: AsyncClient) -> None:
    body_self = json.dumps({"data": {"message": {"id": "msg-self", "from": "628123456789", "text": "test", "from_me": True}}}).encode("utf-8")
    resp = await client.post("/webhooks/farros-wa", content=body_self, headers=_make_signed_headers(body_self, event_id="evt-self"))
    assert resp.status_code == 200
    assert "Ignored" in resp.json().get("message", "")

    body_group = json.dumps({"data": {"message": {"id": "msg-grp", "from": "12345@g.us", "text": "test", "is_group": True}}}).encode("utf-8")
    resp2 = await client.post("/webhooks/farros-wa", content=body_group, headers=_make_signed_headers(body_group, event_id="evt-grp"))
    assert resp2.status_code == 200
    assert "Ignored" in resp2.json().get("message", "")


@pytest.mark.asyncio
async def test_webhook_ignore_sender_not_in_whitelist(client: AsyncClient, test_db: AsyncSession) -> None:
    body = json.dumps({"data": {"message": {"id": "msg-unauth", "from": "628999999999", "text": "https://www.tiktok.com/@creator/video/1234567890123456789"}}}).encode("utf-8")
    resp = await client.post("/webhooks/farros-wa", content=body, headers=_make_signed_headers(body, event_id="evt-unauth"))
    assert resp.status_code == 200
    assert resp.json().get("message") == "Sender not in active whitelist"


@pytest.mark.asyncio
async def test_webhook_queue_job_success(client: AsyncClient, test_db: AsyncSession) -> None:
    # Add sender to whitelist
    num_repo = AllowedNumberRepository(test_db)
    await num_repo.create_number(name="Test User", phone_number="628111222333")
    await test_db.commit()

    tiktok_url = "https://www.tiktok.com/@creator/video/1234567890123456789"
    body = json.dumps({"data": {"message": {"id": "msg-valid-01", "from": "628111222333", "text": f"Tolong download {tiktok_url}"}}}).encode("utf-8")

    with patch("app.webhooks.router.resolve_canonical_tiktok_url", new_callable=AsyncMock) as mock_resolve, \
         patch("app.webhooks.router._send_initial_reply_background", new_callable=AsyncMock):
        mock_resolve.return_value = tiktok_url

        resp = await client.post("/webhooks/farros-wa", content=body, headers=_make_signed_headers(body, event_id="evt-valid-01"))
        assert resp.status_code == 200
        assert resp.json().get("message") == "Job queued successfully"

    # Verify job in database
    job_repo = JobRepository(test_db)
    job = await job_repo.get_by_inbound_message_id("msg-valid-01")
    assert job is not None
    assert job.status == "queued"
    assert job.sender_number == "628111222333"


@pytest.mark.asyncio
async def test_webhook_duplicate_event_id(client: AsyncClient) -> None:
    body = json.dumps({"data": {"message": {"id": "msg-dup", "from": "628111222333", "text": "https://www.tiktok.com/@a/video/1"}}}).encode("utf-8")
    headers = _make_signed_headers(body, event_id="evt-dup-check")

    resp1 = await client.post("/webhooks/farros-wa", content=body, headers=headers)
    assert resp1.status_code == 200

    resp2 = await client.post("/webhooks/farros-wa", content=body, headers=headers)
    assert resp2.status_code == 200
    assert resp2.json().get("message") == "Duplicate event ignored"
