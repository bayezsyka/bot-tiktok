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
