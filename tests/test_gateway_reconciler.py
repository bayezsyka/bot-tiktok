from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem, DownloadJob
from app.queue.reconciler import GatewayReconciler
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_reconciler_selects_null_delivery_status(test_db: AsyncSession):
    # Setup test data
    job = DownloadJob(id="job_recon_1", status="gateway_queued", sender_number="628123456789", inbound_message_id="inbound-1", webhook_event_id="wh-1", original_url="http://example.com")
    test_db.add(job)

    item1 = DownloadItem(
        job_id="job_recon_1",
        status="gateway_queued",
        media_type="video",
        gateway_message_id="msg-1",
        gateway_queue_status="queued",
        gateway_delivery_status=None # NULL status
    )
    item2 = DownloadItem(
        job_id="job_recon_1",
        status="completed",
        media_type="video",
        gateway_message_id="msg-2",
        gateway_queue_status="sent",
        gateway_delivery_status="delivered" # Final status
    )
    test_db.add_all([item1, item2])
    await test_db.commit()

    # Mock gateway client
    class MockSessionMaker:
        def __call__(self):
            class AsyncContextManager:
                async def __aenter__(self):
                    return test_db
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass
            return AsyncContextManager()

    session_maker = MockSessionMaker()

    reconciler = GatewayReconciler(session_maker) # type: ignore

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status = "ok"
        mock_get.return_value.data = {"status": "sent", "delivery_status": "delivered"}
        mock_get.return_value.queue_status = "sent"
        mock_get.return_value.delivery_status = "delivered"

        await reconciler._reconcile_batch()

        # It should have called get_message only for item1 (NULL delivery status)
        mock_get.assert_called_once_with("msg-1")

        # item1 should now be updated to delivered
        await test_db.refresh(item1)
        assert item1.gateway_delivery_status == "delivered"
        assert item1.status == "completed"

@pytest.mark.asyncio
async def test_reconciler_reconcile_item_ids(test_db: AsyncSession):
    # Setup test data
    job = DownloadJob(id="job_recon_2", status="gateway_queued", sender_number="628123456789", inbound_message_id="inbound-2", webhook_event_id="wh-2", original_url="http://example.com")
    test_db.add(job)

    item1 = DownloadItem(
        job_id="job_recon_2",
        status="gateway_queued",
        media_type="video",
        gateway_message_id="msg-recon-2a",
        gateway_queue_status="queued",
        gateway_delivery_status=None
    )
    item2 = DownloadItem(
        job_id="job_recon_2",
        status="gateway_queued",
        media_type="video",
        gateway_message_id="msg-recon-2b",
        gateway_queue_status="queued",
        gateway_delivery_status=None
    )
    test_db.add_all([item1, item2])
    await test_db.commit()

    class MockSessionMaker:
        def __call__(self):
            class AsyncContextManager:
                async def __aenter__(self):
                    return test_db
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass
            return AsyncContextManager()

    reconciler = GatewayReconciler(MockSessionMaker())  # type: ignore

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status = "ok"
        mock_get.return_value.data = {"status": "sent", "delivery_status": "delivered"}
        mock_get.return_value.queue_status = "sent"
        mock_get.return_value.delivery_status = "delivered"
        mock_get.return_value.http_status = 200

        # Reconcile only item1
        await reconciler.reconcile_item_ids([item1.id])

        mock_get.assert_called_once_with("msg-recon-2a")

        await test_db.refresh(item1)
        await test_db.refresh(item2)
        assert item1.gateway_delivery_status == "delivered"
        assert item1.status == "completed"
        assert item2.gateway_delivery_status is None
        assert item2.status == "gateway_queued"

@pytest.mark.asyncio
async def test_reconciler_handles_404_by_setting_delivery_unknown(test_db: AsyncSession):
    # Setup test data
    job = DownloadJob(id="job_recon_3", status="gateway_queued", sender_number="628123456789", inbound_message_id="inbound-3", webhook_event_id="wh-3", original_url="http://example.com")
    test_db.add(job)

    item1 = DownloadItem(
        job_id="job_recon_3",
        status="gateway_queued",
        media_type="video",
        gateway_message_id="msg-recon-3",
        gateway_queue_status="queued",
        gateway_delivery_status=None
    )
    test_db.add(item1)
    await test_db.commit()

    class MockSessionMaker:
        def __call__(self):
            class AsyncContextManager:
                async def __aenter__(self):
                    return test_db
                async def __aexit__(self, exc_type, exc_val, exc_tb):
                    pass
            return AsyncContextManager()

    reconciler = GatewayReconciler(MockSessionMaker())  # type: ignore

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status = "not_found"
        mock_get.return_value.http_status = 404
        mock_get.return_value.data = {"error_code": "MESSAGE_NOT_FOUND"}

        await reconciler.reconcile_item_ids([item1.id])

        await test_db.refresh(item1)
        assert item1.gateway_delivery_status is None  # Should remain None
        assert item1.status == "delivery_unknown"
        assert item1.gateway_error_code == "MESSAGE_NOT_FOUND"
