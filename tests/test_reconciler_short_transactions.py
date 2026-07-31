"""Tests for reconciler short transactions, backoff, batch limits, and special statuses."""
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem, DownloadJob, utc_now
from app.gateway.schemas import GatewayMessageResponse
from app.queue.reconciler import GatewayReconciler
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.asyncio
async def test_reconciler_batch_size_respected(test_db: AsyncSession):
    """Reconciler should not poll more items than GATEWAY_RECONCILE_BATCH_SIZE."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-batch-1", status="gateway_queued", sender_number="628000000300",
            inbound_message_id="inb-batch-1", webhook_event_id="wh-batch-1",
            original_url="http://example.com",
        )
        session.add(job)
        # Add 10 items but batch size should be 5 (default)
        for i in range(10):
            session.add(DownloadItem(
                job_id="job-batch-1", position=i+1, media_type="video",
                status="gateway_queued", gateway_message_id=f"msg-batch-{i}",
            ))
        await session.commit()

    reconciler = GatewayReconciler(session_maker)

    call_count = 0

    async def count_calls(msg_id):
        nonlocal call_count
        call_count += 1
        return GatewayMessageResponse(
            status="ok", message_id=msg_id, http_status=200,
            data={"status": "queued"}, queue_status="queued",
        )

    with patch.object(reconciler.gateway, "get_message", side_effect=count_calls):
        await reconciler._reconcile_batch()

    # Should not exceed batch size (default 5)
    assert call_count <= reconciler.settings.GATEWAY_RECONCILE_BATCH_SIZE


@pytest.mark.asyncio
async def test_old_items_use_backoff(test_db: AsyncSession):
    """Items that were synced recently should be skipped (backoff)."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-backoff-1", status="gateway_queued", sender_number="628000000301",
            inbound_message_id="inb-backoff-1", webhook_event_id="wh-backoff-1",
            original_url="http://example.com",
        )
        session.add(job)
        # Item synced just 5 seconds ago
        session.add(DownloadItem(
            job_id="job-backoff-1", position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-backoff-1",
            last_gateway_sync_at=utc_now(),  # Just synced
        ))
        await session.commit()

    reconciler = GatewayReconciler(session_maker)

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = GatewayMessageResponse(status="ok", http_status=200, data={})
        await reconciler._reconcile_batch()

    # Item was just synced, so it should be skipped due to backoff
    mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_items_beyond_max_age_excluded(test_db: AsyncSession):
    """Items older than GATEWAY_RECONCILE_MAX_AGE_HOURS should not be polled."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        now = utc_now()
        job = DownloadJob(
            id="job-age-1", status="gateway_queued", sender_number="628000000302",
            inbound_message_id="inb-age-1", webhook_event_id="wh-age-1",
            original_url="http://example.com",
        )
        session.add(job)
        # Item created 48 hours ago (beyond default 24h max age)
        old_item = DownloadItem(
            job_id="job-age-1", position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-age-old",
        )
        old_item.created_at = now - timedelta(hours=48)
        session.add(old_item)
        await session.commit()

    reconciler = GatewayReconciler(session_maker)

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        await reconciler._reconcile_batch()

    # Old item should be excluded
    mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_send_result_pending_temporary_error_no_resend(test_db: AsyncSession):
    """processing + SEND_RESULT_PENDING_TEMPORARY_ERROR should NOT resend or mark failed."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-pending-1", status="gateway_processing", sender_number="628000000303",
            inbound_message_id="inb-pending-1", webhook_event_id="wh-pending-1",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id="job-pending-1", position=1, media_type="video",
            status="gateway_processing", gateway_message_id="msg-pending-1",
            gateway_queue_status="processing",
        )
        session.add(item)
        await session.commit()
        item_id = item.id

    reconciler = GatewayReconciler(session_maker)

    # Gateway returns processing + SEND_RESULT_PENDING_TEMPORARY_ERROR
    async def fake_get_message(msg_id):
        return GatewayMessageResponse(
            status="ok", message_id=msg_id, http_status=200,
            data={
                "status": "processing",
                "delivery_status": None,
                "last_error_code": "SEND_RESULT_PENDING_TEMPORARY_ERROR",
                "whatsapp_message_id": "wa-12345",
            },
            queue_status="processing",
            delivery_status=None,
        )

    with patch.object(reconciler.gateway, "get_message", side_effect=fake_get_message):
        await reconciler._reconcile_batch()

    async with session_maker() as session:
        result = await session.execute(select(DownloadItem).where(DownloadItem.id == item_id))
        item = result.scalar_one()
        # Should be delivery_unknown_pending, NOT failed
        assert item.status == "delivery_unknown_pending"
        assert item.gateway_error_code == "SEND_RESULT_PENDING_TEMPORARY_ERROR"
        assert item.gateway_queue_status == "processing"


@pytest.mark.asyncio
async def test_delivery_unknown_pending_promoted_after_timeout(test_db: AsyncSession):
    """After 30 min timeout, delivery_unknown_pending should become delivery_unknown."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        now = utc_now()
        job = DownloadJob(
            id="job-timeout-1", status="delivery_unknown_pending", sender_number="628000000304",
            inbound_message_id="inb-timeout-1", webhook_event_id="wh-timeout-1",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id="job-timeout-1", position=1, media_type="video",
            status="delivery_unknown_pending", gateway_message_id="msg-timeout-1",
            gateway_error_code="SEND_RESULT_PENDING_TEMPORARY_ERROR",
        )
        # Set last_gateway_sync_at to 35 minutes ago (beyond 30-min timeout)
        item.last_gateway_sync_at = now - timedelta(minutes=35)
        item.created_at = now - timedelta(minutes=40)
        session.add(item)
        await session.commit()
        item_id = item.id

    reconciler = GatewayReconciler(session_maker)

    # The reconciler should promote to delivery_unknown without calling gateway
    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock):
        await reconciler._reconcile_batch()

    async with session_maker() as session:
        result = await session.execute(select(DownloadItem).where(DownloadItem.id == item_id))
        item = result.scalar_one()
        assert item.status == "delivery_unknown"
        assert item.gateway_error_code == "PENDING_TIMEOUT"


@pytest.mark.asyncio
async def test_recovery_with_gateway_message_id_no_reupload(test_db: AsyncSession):
    """Recovery: item with gateway_message_id should NOT be re-uploaded."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-recovery-1", status="downloading", sender_number="628000000305",
            inbound_message_id="inb-recovery-1", webhook_event_id="wh-recovery-1",
            original_url="http://example.com", attempt_count=0,
        )
        session.add(job)
        item = DownloadItem(
            job_id="job-recovery-1", position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-already-sent",
        )
        session.add(item)
        await session.commit()

    # Run recovery
    from app.queue.recovery import recover_incomplete_jobs
    async with session_maker() as session:
        count = await recover_incomplete_jobs(session)
        await session.commit()

    assert count == 1

    # Job should be gateway_queued (NOT re-queued for download)
    async with session_maker() as session:
        result = await session.execute(select(DownloadJob).where(DownloadJob.id == "job-recovery-1"))
        job = result.scalar_one()
        assert job.status == "gateway_queued"


@pytest.mark.asyncio
async def test_startup_does_not_flood_gateway(test_db: AsyncSession):
    """Startup reconciliation should not immediately poll many items."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        for i in range(20):
            job = DownloadJob(
                id=f"job-startup-{i}", status="gateway_queued",
                sender_number=f"62800000{i:04d}",
                inbound_message_id=f"inb-startup-{i}",
                webhook_event_id=f"wh-startup-{i}",
                original_url="http://example.com",
            )
            session.add(job)
            session.add(DownloadItem(
                job_id=f"job-startup-{i}", position=1, media_type="video",
                status="gateway_queued", gateway_message_id=f"msg-startup-{i}",
            ))
        await session.commit()

    reconciler = GatewayReconciler(session_maker)

    call_count = 0

    async def count_get(msg_id):
        nonlocal call_count
        call_count += 1
        return GatewayMessageResponse(
            status="ok", message_id=msg_id, http_status=200,
            data={"status": "queued"}, queue_status="queued",
        )

    with patch.object(reconciler.gateway, "get_message", side_effect=count_get):
        await reconciler._reconcile_batch()

    # Should be capped at batch size (5), not 20
    assert call_count <= reconciler.settings.GATEWAY_RECONCILE_BATCH_SIZE


@pytest.mark.asyncio
async def test_final_statuses_not_polled(test_db: AsyncSession):
    """Items with final statuses (failed, cancelled, completed) should not be polled."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-final-1", status="completed", sender_number="628000000306",
            inbound_message_id="inb-final-1", webhook_event_id="wh-final-1",
            original_url="http://example.com",
        )
        session.add(job)
        for status in ["completed", "failed", "cancelled"]:
            session.add(DownloadItem(
                job_id="job-final-1", position=1, media_type="video",
                status=status, gateway_message_id=f"msg-final-{status}",
            ))
        await session.commit()

    reconciler = GatewayReconciler(session_maker)

    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        await reconciler._reconcile_batch()

    mock_get.assert_not_called()
