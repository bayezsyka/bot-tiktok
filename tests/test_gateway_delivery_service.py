from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem, DownloadJob
from app.gateway.delivery_service import GatewayDeliveryService


@pytest.fixture
def mock_db():
    return AsyncMock()

@pytest.fixture
def item():
    return DownloadItem(id="item1", job_id="job1", status="gateway_queued", gateway_queue_status="queued", gateway_delivery_status=None)

@pytest.fixture
def job():
    return DownloadJob(id="job1", status="gateway_queued", sender_number="628123456789")

@pytest.mark.asyncio
async def test_monotonic_delivery_status(mock_db, item):
    service = GatewayDeliveryService(mock_db)

    # Mock sync_job_status so it doesn't try to query DB
    with patch.object(service, "sync_job_status", new_callable=AsyncMock):
        # Move to delivered
        await service.process_outbound_status(item, d_status="delivered")
        assert item.gateway_delivery_status == "delivered"
        assert item.status == "completed"
        assert item.gateway_delivered_at is not None

        # Try to move backwards to sent
        await service.process_outbound_status(item, d_status="sent")
        assert item.gateway_delivery_status == "delivered" # Still delivered
        assert item.status == "completed"

        # Move forward to read
        await service.process_outbound_status(item, d_status="read")
        assert item.gateway_delivery_status == "read"
        assert item.status == "completed"

@pytest.mark.asyncio
async def test_failed_status_after_delivered(mock_db, item):
    service = GatewayDeliveryService(mock_db)

    with patch.object(service, "sync_job_status", new_callable=AsyncMock):
        # Move to delivered
        await service.process_outbound_status(item, d_status="delivered")
        assert item.status == "completed"

        # A delayed failure webhook comes in
        await service.process_outbound_status(item, d_status="failed", error_message="Delayed fail")
        assert item.gateway_delivery_status == "delivered" # Ignored
        assert item.status == "completed"

@pytest.mark.asyncio
async def test_delivery_unknown(mock_db, item):
    service = GatewayDeliveryService(mock_db)

    with patch.object(service, "sync_job_status", new_callable=AsyncMock):
        await service.process_outbound_status(item, d_status="delivery_unknown")
    assert item.gateway_delivery_status is None
    assert item.status == "delivery_unknown"
    assert item.gateway_error_code == "MESSAGE_NOT_FOUND"
