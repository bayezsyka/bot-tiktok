"""Tests for reconciler short transactions, backoff, batch limits, and special statuses."""
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem, DownloadJob, utc_now
from app.downloader.dtos import ProcessedItemResult, ProcessedJobResult
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.gateway.schemas import GatewayMessageResponse
from app.queue.reconciler import GatewayReconciler
from app.queue.worker import QueueWorker
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
            pending_since_at=now - timedelta(minutes=35),
            last_gateway_sync_at=now,
        )
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
async def test_reconciler_old_item_recently_polled_is_not_due(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        now = utc_now()
        job = DownloadJob(
            id="job-age-70-recent", status="gateway_queued", sender_number="628000000307",
            inbound_message_id="inb-age-70-recent", webhook_event_id="wh-age-70-recent",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id=job.id, position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-age-70-recent",
            gateway_accepted_at=now - timedelta(minutes=70),
            last_gateway_sync_at=now - timedelta(seconds=30),
        )
        item.created_at = now - timedelta(minutes=70)
        session.add(item)
        await session.commit()

    reconciler = GatewayReconciler(session_maker)
    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        await reconciler._reconcile_batch()

    mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_reconciler_old_item_polled_16_minutes_ago_is_due(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        now = utc_now()
        job = DownloadJob(
            id="job-age-70-due", status="gateway_queued", sender_number="628000000308",
            inbound_message_id="inb-age-70-due", webhook_event_id="wh-age-70-due",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id=job.id, position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-age-70-due",
            gateway_accepted_at=now - timedelta(minutes=70),
            last_gateway_sync_at=now - timedelta(minutes=16),
        )
        item.created_at = now - timedelta(minutes=70)
        session.add(item)
        await session.commit()

    reconciler = GatewayReconciler(session_maker)
    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = GatewayMessageResponse(status="ok", http_status=200, data={"status": "queued"}, queue_status="queued")
        await reconciler._reconcile_batch()

    mock_get.assert_called_once_with("msg-age-70-due")


@pytest.mark.asyncio
async def test_reconciler_fresh_item_polled_20_seconds_ago_is_due(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        now = utc_now()
        job = DownloadJob(
            id="job-age-1-due", status="gateway_queued", sender_number="628000000309",
            inbound_message_id="inb-age-1-due", webhook_event_id="wh-age-1-due",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id=job.id, position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-age-1-due",
            gateway_accepted_at=now - timedelta(minutes=1),
            last_gateway_sync_at=now - timedelta(seconds=20),
        )
        item.created_at = now - timedelta(minutes=1)
        session.add(item)
        await session.commit()

    reconciler = GatewayReconciler(session_maker)
    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = GatewayMessageResponse(status="ok", http_status=200, data={"status": "queued"}, queue_status="queued")
        await reconciler._reconcile_batch()

    mock_get.assert_called_once_with("msg-age-1-due")


@pytest.mark.asyncio
async def test_reconciler_poll_interval_increases_as_item_ages(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        now = utc_now()
        job = DownloadJob(
            id="job-age-cycle", status="gateway_queued", sender_number="628000000310",
            inbound_message_id="inb-age-cycle", webhook_event_id="wh-age-cycle",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id=job.id, position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-age-cycle",
            gateway_accepted_at=now - timedelta(minutes=1),
            last_gateway_sync_at=now - timedelta(seconds=20),
        )
        item.created_at = now - timedelta(minutes=1)
        session.add(item)
        await session.commit()
        item_id = item.id

    reconciler = GatewayReconciler(session_maker)

    async def poll_count() -> int:
        with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = GatewayMessageResponse(status="ok", http_status=200, data={"status": "queued"}, queue_status="queued")
            await reconciler._reconcile_batch()
            return mock_get.call_count

    assert await poll_count() == 1

    async with session_maker() as session:
        now = utc_now()
        item_for_update = await session.get(DownloadItem, item_id)
        assert item_for_update is not None
        item_for_update.gateway_accepted_at = now - timedelta(minutes=3)
        item_for_update.created_at = now - timedelta(minutes=3)
        item_for_update.last_gateway_sync_at = now - timedelta(seconds=20)
        await session.commit()
    assert await poll_count() == 0

    async with session_maker() as session:
        now = utc_now()
        item_for_update = await session.get(DownloadItem, item_id)
        assert item_for_update is not None
        item_for_update.gateway_accepted_at = now - timedelta(minutes=70)
        item_for_update.created_at = now - timedelta(minutes=70)
        item_for_update.last_gateway_sync_at = now - timedelta(minutes=16)
        await session.commit()
    assert await poll_count() == 1


@pytest.mark.asyncio
async def test_delivery_unknown_pending_timeout_uses_fixed_pending_since(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)
    base = utc_now()

    async with session_maker() as session:
        job = DownloadJob(
            id="job-pending-fixed", status="gateway_processing", sender_number="628000000311",
            inbound_message_id="inb-pending-fixed", webhook_event_id="wh-pending-fixed",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id=job.id, position=1, media_type="video",
            status="gateway_processing", gateway_message_id="msg-pending-fixed",
            gateway_queue_status="processing",
            gateway_accepted_at=base,
        )
        item.created_at = base
        session.add(item)
        await session.commit()
        item_id = item.id

    reconciler = GatewayReconciler(session_maker)
    response = GatewayMessageResponse(
        status="ok",
        message_id="msg-pending-fixed",
        http_status=200,
        data={
            "status": "processing",
            "last_error_code": "SEND_RESULT_PENDING_TEMPORARY_ERROR",
            "last_error_message": "Still waiting for WhatsApp callback",
        },
        queue_status="processing",
    )

    for minute in range(10):
        with patch("app.gateway.delivery_service.utc_now", return_value=base + timedelta(minutes=minute)):
            await reconciler._apply_response_to_item(item_id, response)

    async with session_maker() as session:
        pending_item = await session.get(DownloadItem, item_id)
        assert pending_item is not None
        assert pending_item.status == "delivery_unknown_pending"
        assert pending_item.pending_since_at == base.replace(tzinfo=None)
        assert pending_item.last_gateway_sync_at is not None
        assert pending_item.last_gateway_sync_at != pending_item.pending_since_at
        assert pending_item.gateway_error_message == "Still waiting for WhatsApp callback"

    with patch("app.queue.reconciler.utc_now", return_value=base + timedelta(minutes=31)), \
         patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        await reconciler._reconcile_batch()

    mock_get.assert_not_called()
    async with session_maker() as session:
        final_item = await session.get(DownloadItem, item_id)
        assert final_item is not None
        assert final_item.status == "delivery_unknown"
        assert final_item.pending_since_at == base.replace(tzinfo=None)


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
async def test_partial_send_recovery_skips_sent_item_and_sends_remaining_slideshow(test_db: AsyncSession):
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-recovery-partial", status="sending", sender_number="628000000312",
            inbound_message_id="inb-recovery-partial", webhook_event_id="wh-recovery-partial",
            original_url="https://www.tiktok.com/@creator/photo/123",
            canonical_url="https://www.tiktok.com/@creator/photo/123",
            content_type="photo",
            media_count=3,
            attempt_count=0,
        )
        session.add(job)
        session.add_all([
            DownloadItem(
                job_id=job.id, position=1, media_type="photo",
                status="gateway_queued", gateway_message_id="msg-already-sent-1",
            ),
            DownloadItem(
                job_id=job.id, position=2, media_type="photo",
                status="pending", source_url="http://src/2.jpg",
            ),
            DownloadItem(
                job_id=job.id, position=3, media_type="photo",
                status="pending", source_url="http://src/3.jpg",
            ),
        ])
        await session.commit()

    from app.queue.recovery import recover_incomplete_jobs
    async with session_maker() as session:
        recovered = await recover_incomplete_jobs(session)
        await session.commit()

    assert recovered == 1
    async with session_maker() as session:
        job = (await session.execute(select(DownloadJob).where(DownloadJob.id == "job-recovery-partial"))).scalar_one()
        assert job.status == "queued"
        assert job.error_code == "RECOVERY_PARTIAL_SEND"

    worker = QueueWorker(session_maker)
    metadata = TikTokContentMetadata(
        content_type="photo",
        title="Slideshow",
        author="Creator",
        duration_seconds=0,
        items=[
            TikTokMediaItemMetadata(position=1, source_url="http://src/1.jpg", media_type="photo"),
            TikTokMediaItemMetadata(position=2, source_url="http://src/2.jpg", media_type="photo"),
            TikTokMediaItemMetadata(position=3, source_url="http://src/3.jpg", media_type="photo"),
        ],
    )

    async def fake_download(_url, meta, job_dir):
        items = []
        for item in meta.items:
            path = job_dir / f"photo-{item.position}.jpg"
            path.write_bytes(f"photo-{item.position}".encode())
            items.append(item.model_copy(update={"local_path": str(path)}))
        return meta.model_copy(update={"items": items})

    async def fake_process(items, _job_dir):
        return ProcessedJobResult(
            items=tuple(
                ProcessedItemResult(
                    item_id=item.id,
                    status="pending",
                    local_filename=item.local_filename,
                    final_size_bytes=100,
                )
                for item in items
                if not item.gateway_message_id
            ),
            final_size_bytes=200,
        )

    sent_positions: list[int] = []

    async def fake_send_media(**kwargs):
        key = kwargs["idempotency_key"]
        position = int(key.rsplit("-", 1)[1])
        sent_positions.append(position)
        return GatewayMessageResponse(status="ok", message_id=f"msg-sent-{position}", queue_status="queued")

    with patch("app.downloader.service.YtDlpProvider.extract_metadata", new_callable=AsyncMock, return_value=None), \
         patch("app.downloader.service.TikTokPhotoProvider.extract_metadata", new_callable=AsyncMock, return_value=metadata), \
         patch("app.downloader.tiktok_photo_provider.TikTokPhotoProvider.download_content", side_effect=fake_download), \
         patch("app.media.processor.MediaProcessor.process_job_media", side_effect=fake_process), \
         patch.object(worker.gateway, "send_media", side_effect=fake_send_media):
        await worker._process_job_safely("job-recovery-partial")

    assert sent_positions == [2, 3]
    async with session_maker() as session:
        job = (await session.execute(select(DownloadJob).where(DownloadJob.id == "job-recovery-partial"))).scalar_one()
        items = (
            await session.execute(
                select(DownloadItem).where(DownloadItem.job_id == job.id).order_by(DownloadItem.position)
            )
        ).scalars().all()
        assert job.status == "gateway_queued"
        assert [item.gateway_message_id for item in items] == [
            "msg-already-sent-1",
            "msg-sent-2",
            "msg-sent-3",
        ]


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
