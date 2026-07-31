import argparse
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from app.database.connection import AsyncSessionLocal
from app.database.models import DownloadItem, DownloadJob, utc_now
from app.database.repositories import AdminRepository, AllowedNumberRepository
from cli import add_number_cmd, create_admin_cmd, reconcile_gateway_cmd, remove_number_cmd


@pytest.mark.asyncio
async def test_cli_create_admin() -> None:
    args = argparse.Namespace(username="testcliadmin", password="SecretPassword123", email="admin@test.com")
    await create_admin_cmd(args)

    async with AsyncSessionLocal() as session:
        repo = AdminRepository(session)
        admin = await repo.get_by_username("testcliadmin")
        assert admin is not None
        assert admin.email == "admin@test.com"


@pytest.mark.asyncio
async def test_cli_add_and_remove_number() -> None:
    args_add = argparse.Namespace(phone="081999888777", name="CLI Test Number", notes="Added via test")
    await add_number_cmd(args_add)

    async with AsyncSessionLocal() as session:
        repo = AllowedNumberRepository(session)
        num = await repo.get_by_phone("6281999888777")
        assert num is not None
        assert num.name == "CLI Test Number"

    args_rm = argparse.Namespace(phone="6281999888777")
    await remove_number_cmd(args_rm)

    async with AsyncSessionLocal() as session:
        repo = AllowedNumberRepository(session)
        num_after = await repo.get_by_phone("6281999888777")
        assert num_after is None


@pytest.mark.asyncio
async def test_cli_reconcile_gateway() -> None:
    # Setup test database records
    async with AsyncSessionLocal() as session:
        # Create a job
        job = DownloadJob(
            id="job_cli_recon",
            status="gateway_queued",
            sender_number="628123456789",
            inbound_message_id="inbound-cli-recon",
            webhook_event_id="wh-cli-recon",
            original_url="http://example.com"
        )
        session.add(job)

        now = utc_now()
        # Item 1: Within 2 days, gateway_queued, delivery_status is None (should be processed)
        item1 = DownloadItem(
            job_id="job_cli_recon",
            status="gateway_queued",
            media_type="video",
            gateway_message_id="msg-cli-1",
            gateway_queue_status="queued",
            gateway_delivery_status=None,
            created_at=now - timedelta(days=1)
        )
        # Item 2: Within 2 days, delivery_unknown, delivery_status is None (should be processed)
        item2 = DownloadItem(
            job_id="job_cli_recon",
            status="delivery_unknown",
            media_type="video",
            gateway_message_id="msg-cli-2",
            gateway_queue_status="queued",
            gateway_delivery_status=None,
            created_at=now - timedelta(days=1)
        )
        # Item 3: Outside 2 days (old), gateway_queued, delivery_status is None (should NOT be processed)
        item3 = DownloadItem(
            job_id="job_cli_recon",
            status="gateway_queued",
            media_type="video",
            gateway_message_id="msg-cli-3",
            gateway_queue_status="queued",
            gateway_delivery_status=None,
            created_at=now - timedelta(days=5)
        )
        session.add_all([item1, item2, item3])
        await session.commit()
        item1_id = item1.id
        item2_id = item2.id
        item3_id = item3.id

    args = argparse.Namespace(days=2)

    # We patch reconcile_item_ids to see what it is called with
    with patch("app.queue.reconciler.GatewayReconciler.reconcile_item_ids", new_callable=AsyncMock) as mock_reconcile:
        await reconcile_gateway_cmd(args)

        # Check that it called reconcile_item_ids with exactly item1 and item2 IDs, and not item3
        mock_reconcile.assert_called_once()
        called_ids = mock_reconcile.call_args[0][0]
        assert item1_id in called_ids
        assert item2_id in called_ids
        assert item3_id not in called_ids
