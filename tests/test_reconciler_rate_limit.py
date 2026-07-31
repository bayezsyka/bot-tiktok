"""Tests for reconciler rate limit handling: 429 stops batch, cooldown, no failed marking."""
import time
from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem, DownloadJob
from app.gateway.exceptions import GatewayRateLimitError
from app.gateway.schemas import GatewayMessageResponse
from app.queue.reconciler import GatewayReconciler
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest.mark.asyncio
async def test_429_stops_batch_immediately(test_db: AsyncSession):
    """When gateway returns 429, the reconciler should stop the batch immediately."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-rl-1", status="gateway_queued", sender_number="628000000200",
            inbound_message_id="inb-rl-1", webhook_event_id="wh-rl-1",
            original_url="http://example.com",
        )
        session.add(job)
        for i in range(3):
            session.add(DownloadItem(
                job_id="job-rl-1", position=i+1, media_type="video",
                status="gateway_queued", gateway_message_id=f"msg-rl-1-{i}",
            ))
        await session.commit()

    reconciler = GatewayReconciler(session_maker)

    call_count = 0

    async def get_message_429(msg_id):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise GatewayRateLimitError(retry_after=60.0, message="Rate limit exceeded")
        return GatewayMessageResponse(status="ok", message_id=msg_id, http_status=200,
                                       data={"status": "sent", "delivery_status": "delivered"},
                                       queue_status="sent", delivery_status="delivered")

    with patch.object(reconciler.gateway, "get_message", side_effect=get_message_429):
        await reconciler._reconcile_batch()

    # Should have called get_message only once before stopping
    assert call_count == 1, f"Expected 1 call, got {call_count}"


@pytest.mark.asyncio
async def test_429_does_not_mark_items_as_failed(test_db: AsyncSession):
    """After 429, items should remain in their original status, not marked as failed."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-rl-2", status="gateway_queued", sender_number="628000000201",
            inbound_message_id="inb-rl-2", webhook_event_id="wh-rl-2",
            original_url="http://example.com",
        )
        session.add(job)
        item = DownloadItem(
            job_id="job-rl-2", position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-rl-2",
        )
        session.add(item)
        await session.commit()
        item_id = item.id

    reconciler = GatewayReconciler(session_maker)

    with patch.object(reconciler.gateway, "get_message",
                      side_effect=GatewayRateLimitError(retry_after=30.0)):
        await reconciler._reconcile_batch()

    # Item should still be gateway_queued, NOT failed
    async with session_maker() as session:
        from sqlalchemy import select
        result = await session.execute(select(DownloadItem).where(DownloadItem.id == item_id))
        item = result.scalar_one()
        assert item.status == "gateway_queued"
        assert item.gateway_queue_status != "failed"


@pytest.mark.asyncio
async def test_cooldown_applied_after_429(test_db: AsyncSession):
    """After 429, subsequent batches should be skipped until cooldown expires."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-rl-3", status="gateway_queued", sender_number="628000000202",
            inbound_message_id="inb-rl-3", webhook_event_id="wh-rl-3",
            original_url="http://example.com",
        )
        session.add(job)
        session.add(DownloadItem(
            job_id="job-rl-3", position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-rl-3",
        ))
        await session.commit()

    reconciler = GatewayReconciler(session_maker)

    # Trigger 429
    with patch.object(reconciler.gateway, "get_message",
                      side_effect=GatewayRateLimitError(retry_after=120.0)):
        await reconciler._reconcile_batch()

    # Cooldown should be set
    assert reconciler._rate_limit_until > time.monotonic()

    # Next batch should be skipped (no gateway calls)
    with patch.object(reconciler.gateway, "get_message", new_callable=AsyncMock) as mock_get:
        await reconciler._reconcile_batch()
        mock_get.assert_not_called()


@pytest.mark.asyncio
async def test_retry_after_header_respected(test_db: AsyncSession):
    """The retry_after value from GatewayRateLimitError should determine cooldown duration."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    reconciler = GatewayReconciler(session_maker)

    # Apply cooldown with specific retry_after
    reconciler._apply_cooldown(retry_after=45.0)
    expected_earliest = time.monotonic() + 44.0  # Allow 1s margin
    assert reconciler._rate_limit_until > expected_earliest


@pytest.mark.asyncio
async def test_gateway_rate_limit_error_raised_correctly():
    """GatewayRateLimitError should carry retry_after and message."""
    err = GatewayRateLimitError(retry_after=30.0, message="Too many requests")
    assert err.retry_after == 30.0
    assert "30.0s" in str(err)
    assert "Too many requests" in str(err)

    err_no_retry = GatewayRateLimitError()
    assert err_no_retry.retry_after is None


@pytest.mark.asyncio
async def test_reconcile_item_ids_stops_on_429(test_db: AsyncSession):
    """reconcile_item_ids should also stop on 429."""
    session_maker = async_sessionmaker(bind=test_db.bind, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        job = DownloadJob(
            id="job-rl-4", status="gateway_queued", sender_number="628000000203",
            inbound_message_id="inb-rl-4", webhook_event_id="wh-rl-4",
            original_url="http://example.com",
        )
        session.add(job)
        item1 = DownloadItem(
            job_id="job-rl-4", position=1, media_type="video",
            status="gateway_queued", gateway_message_id="msg-rl-4a",
        )
        item2 = DownloadItem(
            job_id="job-rl-4", position=2, media_type="video",
            status="gateway_queued", gateway_message_id="msg-rl-4b",
        )
        session.add_all([item1, item2])
        await session.commit()
        item1_id = item1.id
        item2_id = item2.id

    reconciler = GatewayReconciler(session_maker)

    call_count = 0

    async def first_item_429(msg_id):
        nonlocal call_count
        call_count += 1
        raise GatewayRateLimitError(retry_after=60.0)

    with patch.object(reconciler.gateway, "get_message", side_effect=first_item_429):
        await reconciler.reconcile_item_ids([item1_id, item2_id])

    assert call_count == 1  # Should stop after first 429
