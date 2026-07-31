from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem, DownloadJob
from app.queue.reconciler import GatewayReconciler
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.asyncio
async def test_reconciler_selects_null_delivery_status(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    # Setup test data
    async with session_maker() as session:
        job = DownloadJob(id="job_recon_1", status="gateway_queued", sender_number="628123456789", inbound_message_id="inbound-1", webhook_event_id="wh-1", original_url="http://example.com")
        session.add(job)

        item1 = DownloadItem(
            job_id="job_recon_1",
            status="gateway_queued",
            media_type="video",
            gateway_message_id="msg-1",
            gateway_queue_status="queued",
            gateway_delivery_status=None  # NULL status
        )
        item2 = DownloadItem(
            job_id="job_recon_1",
            status="completed",
            media_type="video",
            gateway_message_id="msg-2",
            gateway_queue_status="sent",
            gateway_delivery_status="delivered"  # Final status
        )
        session.add_all([item1, item2])
        await session.commit()
        item1_id = item1.id

    reconciler = GatewayReconciler(session_maker)

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status = "ok"
        mock_get.return_value.data = {"status": "sent", "delivery_status": "delivered"}
        mock_get.return_value.queue_status = "sent"
        mock_get.return_value.delivery_status = "delivered"
        mock_get.return_value.http_status = 200

        await reconciler._reconcile_batch()

        # It should have called get_message only for item1 (NULL delivery status)
        mock_get.assert_called_once_with("msg-1")

    # item1 should now be updated to delivered
    async with session_maker() as session:
        from sqlalchemy import select
        result = await session.execute(select(DownloadItem).where(DownloadItem.id == item1_id))
        item1_refreshed = result.scalar_one_or_none()
        assert item1_refreshed is not None
        assert item1_refreshed.gateway_delivery_status == "delivered"
        assert item1_refreshed.status == "completed"

@pytest.mark.asyncio
async def test_reconciler_reconcile_item_ids(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    # Setup test data
    async with session_maker() as session:
        job = DownloadJob(id="job_recon_2", status="gateway_queued", sender_number="628123456789", inbound_message_id="inbound-2", webhook_event_id="wh-2", original_url="http://example.com")
        session.add(job)

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
        session.add_all([item1, item2])
        await session.commit()
        item1_id = item1.id
        item2_id = item2.id

    reconciler = GatewayReconciler(session_maker)

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status = "ok"
        mock_get.return_value.data = {"status": "sent", "delivery_status": "delivered"}
        mock_get.return_value.queue_status = "sent"
        mock_get.return_value.delivery_status = "delivered"
        mock_get.return_value.http_status = 200

        # Reconcile only item1
        await reconciler.reconcile_item_ids([item1_id])

        mock_get.assert_called_once_with("msg-recon-2a")

    async with session_maker() as session:
        from sqlalchemy import select
        result1 = await session.execute(select(DownloadItem).where(DownloadItem.id == item1_id))
        item1_refreshed = result1.scalar_one_or_none()
        result2 = await session.execute(select(DownloadItem).where(DownloadItem.id == item2_id))
        item2_refreshed = result2.scalar_one_or_none()

        assert item1_refreshed is not None
        assert item1_refreshed.gateway_delivery_status == "delivered"
        assert item1_refreshed.status == "completed"
        assert item2_refreshed is not None
        assert item2_refreshed.gateway_delivery_status is None
        assert item2_refreshed.status == "gateway_queued"

@pytest.mark.asyncio
async def test_reconciler_handles_404_by_setting_delivery_unknown(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    # Setup test data
    async with session_maker() as session:
        job = DownloadJob(id="job_recon_3", status="gateway_queued", sender_number="628123456789", inbound_message_id="inbound-3", webhook_event_id="wh-3", original_url="http://example.com")
        session.add(job)

        item1 = DownloadItem(
            job_id="job_recon_3",
            status="gateway_queued",
            media_type="video",
            gateway_message_id="msg-recon-3",
            gateway_queue_status="queued",
            gateway_delivery_status=None
        )
        session.add(item1)
        await session.commit()
        item1_id = item1.id

    reconciler = GatewayReconciler(session_maker)

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value.status = "not_found"
        mock_get.return_value.http_status = 404
        mock_get.return_value.data = {"error_code": "MESSAGE_NOT_FOUND"}

        await reconciler.reconcile_item_ids([item1_id])

    async with session_maker() as session:
        from sqlalchemy import select
        result = await session.execute(select(DownloadItem).where(DownloadItem.id == item1_id))
        item1_refreshed = result.scalar_one_or_none()
        assert item1_refreshed is not None
        assert item1_refreshed.gateway_delivery_status is None  # Should remain None
        assert item1_refreshed.status == "delivery_unknown"
        assert item1_refreshed.gateway_error_code == "MESSAGE_NOT_FOUND"
