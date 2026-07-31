import pytest
from app.database.models import DownloadItem, DownloadJob
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_webhook_idempotency(client: AsyncClient, test_db: AsyncSession):
    # Setup test data
    job = DownloadJob(id="job_webhook_1", status="sent", sender_number="628123456789", inbound_message_id="inbound-2", webhook_event_id="wh-2", original_url="http://example.com")
    test_db.add(job)
    item = DownloadItem(
        job_id="job_webhook_1",
        status="sent",
        media_type="video",
        gateway_message_id="msg-webhook-1",
        gateway_queue_status="sent",
        gateway_delivery_status=None
    )
    test_db.add(item)
    await test_db.commit()

    import hashlib
    import hmac
    import json
    import time

    # Create webhook payload for 'delivered'
    payload = {
        "event": "message.delivered",
        "data": {
            "id": "msg-webhook-1",
            "status": "delivered"
        }
    }
    payload_bytes = json.dumps(payload).encode("utf-8")
    timestamp = str(int(time.time()))
    message = timestamp.encode("utf-8") + b"." + payload_bytes
    sig1 = hmac.new(b"test-webhook-secret-123456", message, hashlib.sha256).hexdigest()

    headers1 = {
        "X-FWAG-Event": "message.delivered",
        "X-FWAG-Event-Id": "wh-evt-1",
        "X-FWAG-Timestamp": timestamp,
        "X-FWAG-Signature": sig1,
        "Content-Type": "application/json"
    }

    # Send webhook
    resp1 = await client.post("/webhooks/farros-wa", content=payload_bytes, headers=headers1)
    assert resp1.status_code == 200

    await test_db.refresh(item)
    assert item.gateway_delivery_status == "delivered"
    assert item.status == "completed"

    # Send the exact same webhook again (idempotency test)
    resp2 = await client.post("/webhooks/farros-wa", content=payload_bytes, headers=headers1)
    assert resp2.status_code == 200

    # Send a 'failed' webhook after 'delivered' (monotonic check)
    fail_payload = {
        "event": "message.failed",
        "data": {
            "id": "msg-webhook-1",
            "error_message": "Network issue"
        }
    }
    fail_payload_bytes = json.dumps(fail_payload).encode("utf-8")
    message2 = timestamp.encode("utf-8") + b"." + fail_payload_bytes
    sig2 = hmac.new(b"test-webhook-secret-123456", message2, hashlib.sha256).hexdigest()

    headers2 = {
        "X-FWAG-Event": "message.failed",
        "X-FWAG-Event-Id": "wh-evt-2",
        "X-FWAG-Timestamp": timestamp,
        "X-FWAG-Signature": sig2,
        "Content-Type": "application/json"
    }

    resp3 = await client.post("/webhooks/farros-wa", content=fail_payload_bytes, headers=headers2)
    assert resp3.status_code == 200

    await test_db.refresh(item)
    # Status should still be 'completed' and 'delivered'
    assert item.gateway_delivery_status == "delivered"
    assert item.status == "completed"
