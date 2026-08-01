import argparse
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem, utc_now
from app.database.repositories import JobRepository
from app.downloader.dtos import JobDownloadSnapshot
from app.downloader.metadata import TikTokContentMetadata, TikTokMediaItemMetadata
from app.downloader.service import DownloaderService
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.delivery_service import GatewayDeliveryService
from app.gateway.exceptions import GatewayError
from app.gateway.schemas import GatewayMessageResponse
from app.queue.worker import QueueWorker
from cli import retry_job_cmd
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_send_media_photo_maps_to_image(tmp_path: Path) -> None:
    client = FarrosWAGatewayClient()
    file_path = tmp_path / "photo_001.jpg"
    file_path.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 20)

    with patch.object(client, "_execute_request", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = GatewayMessageResponse(
            status="ok", message_id="gw_msg_001", queue_status="queued", delivery_status=None, http_status=200
        )
        res = await client.send_media(
            to="628123456789",
            media_type="photo",
            file_path=str(file_path),
            caption="Caption photo 1",
            external_reference="job_123",
            idempotency_key="tiktok-msg1-photo-001",
        )
        assert res.message_id == "gw_msg_001"
        data_arg = mock_exec.call_args[1]["data"]
        file_info_arg = mock_exec.call_args[1]["file_info"]

        # 1. Gateway type MUST be 'image'
        assert data_arg["type"] == "image"
        # 6. JPG produces image/jpeg
        assert file_info_arg[1] == "image/jpeg"


@pytest.mark.asyncio
async def test_send_media_image_video_audio_mime_types(tmp_path: Path) -> None:
    client = FarrosWAGatewayClient()

    with patch.object(client, "_execute_request", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = GatewayMessageResponse(status="ok", message_id="gw_1", http_status=200)

        # 7. PNG -> image/png
        png_file = tmp_path / "photo.png"
        png_file.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 20)
        await client.send_media(to="123", media_type="photo", file_path=str(png_file), idempotency_key="test-idemp-001")
        assert mock_exec.call_args[1]["file_info"][1] == "image/png"

        # 8. WEBP -> image/webp
        webp_file = tmp_path / "photo.webp"
        webp_file.write_bytes(b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 20)
        await client.send_media(to="123", media_type="photo", file_path=str(webp_file), idempotency_key="test-idemp-002")
        assert mock_exec.call_args[1]["file_info"][1] == "image/webp"

        # 9. GIF -> image/gif
        gif_file = tmp_path / "photo.gif"
        gif_file.write_bytes(b"GIF89a" + b"\x00" * 20)
        await client.send_media(to="123", media_type="photo", file_path=str(gif_file), idempotency_key="test-idemp-003")
        assert mock_exec.call_args[1]["file_info"][1] == "image/gif"

        # 4. video -> video/mp4
        mp4_file = tmp_path / "video.mp4"
        mp4_file.write_bytes(b"\x00\x00\x00\x1cftypisom" + b"\x00" * 20)
        await client.send_media(to="123", media_type="video", file_path=str(mp4_file), idempotency_key="test-idemp-004")
        assert mock_exec.call_args[1]["data"]["type"] == "video"
        assert mock_exec.call_args[1]["file_info"][1] == "video/mp4"


@pytest.mark.asyncio
async def test_unsupported_media_type_fails_before_network_call(tmp_path: Path) -> None:
    client = FarrosWAGatewayClient()
    file_path = tmp_path / "file.jpg"
    file_path.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")

    with patch.object(client, "_execute_request", new_callable=AsyncMock) as mock_exec:
        # 5. Unsupported media type fails before network call
        with pytest.raises(GatewayError) as exc_info:
            await client.send_media(
                to="123",
                media_type="unknown_type",
                file_path=str(file_path),
                idempotency_key="test-idemp-005",
            )
        assert "tidak didukung" in str(exc_info.value)
        mock_exec.assert_not_called()


@pytest.mark.asyncio
async def test_send_all_media_items_7_photos_and_caption_only_on_pos_1(
    test_db: AsyncSession, tmp_path: Path
) -> None:
    repo = JobRepository(test_db)
    job = await repo.create_job(
        inbound_message_id="msg_7_photos",
        webhook_event_id="evt_7_photos",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
    )

    items = []
    for i in range(1, 8):
        fn = tmp_path / f"photo_{i:03d}.jpg"
        fn.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 20)
        item = DownloadItem(
            job_id=job.id,
            position=i,
            media_type="photo",
            source_url=f"https://p16.tiktokcdn.com/slide_{i}.jpg",
            local_filename=str(fn),
            status="pending",
        )
        test_db.add(item)
        items.append(item)

    await test_db.commit()

    from app.database.connection import get_session_maker
    sm = get_session_maker()

    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)

    def mock_send_media_side_effect(**kwargs: str) -> GatewayMessageResponse:
        pos_str = kwargs["idempotency_key"].split("-")[-1]
        return GatewayMessageResponse(
            status="ok",
            message_id=f"gw_msg_{pos_str}",
            queue_status="queued",
            delivery_status=None,
            http_status=200,
        )

    mock_gateway.send_media.side_effect = mock_send_media_side_effect

    worker = QueueWorker(sm)
    worker.gateway = mock_gateway
    await worker._send_all_media_items(job.id)

    # 11. All 7 photo items accepted by Gateway
    assert mock_gateway.send_media.call_count == 7

    calls = mock_gateway.send_media.call_args_list
    for idx, call in enumerate(calls, start=1):
        k = call.kwargs
        # 2. Internal media_type stays "photo"
        assert k["media_type"] == "photo"
        # 10. Idempotency key uses "photo"
        assert k["idempotency_key"] == f"tiktok-msg_7_photos-photo-{idx:03d}"
        # 12. Caption only sent on position 1
        if idx == 1:
            assert "foto tiktok berhasil diproses" in (k["caption"] or "")
        else:
            assert k["caption"] == ""

    # Re-fetch job from DB
    async with sm() as session:
        r_repo = JobRepository(session)
        updated_job = await r_repo.get_by_id(job.id)
        assert updated_job is not None
        assert updated_job.status in ("sent", "gateway_queued")
        assert updated_job.media_count == 7
        assert updated_job.sent_count == 7
        assert updated_job.failed_count == 0


@pytest.mark.asyncio
async def test_deferred_job_sync_on_failure_and_final_counters(
    test_db: AsyncSession, tmp_path: Path
) -> None:
    repo = JobRepository(test_db)
    job = await repo.create_job(
        inbound_message_id="msg_fail_photos",
        webhook_event_id="evt_fail_photos",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
    )

    for i in range(1, 8):
        fn = tmp_path / f"photo_{i:03d}.jpg"
        fn.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
        item = DownloadItem(
            job_id=job.id,
            position=i,
            media_type="photo",
            source_url=f"https://p16.tiktokcdn.com/slide_{i}.jpg",
            local_filename=str(fn),
            status="pending",
        )
        test_db.add(item)
    await test_db.commit()

    from app.database.connection import get_session_maker
    sm = get_session_maker()
    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)

    # All 7 fails
    mock_gateway.send_media.side_effect = GatewayError("INVALID_TYPE Upload unsupported")

    worker = QueueWorker(sm)
    worker.gateway = mock_gateway

    with patch("app.queue.worker.dispatch_failure_notification", new_callable=AsyncMock) as mock_notify:
        await worker._send_all_media_items(job.id)
        # 14 & 15. Failure notification sent at most once after final sync
        assert mock_notify.call_count == 1

    async with sm() as session:
        r_repo = JobRepository(session)
        updated_job = await r_repo.get_by_id(job.id)
        assert updated_job is not None
        # 16. 7 failed items produces DELIVERY_FAILED and failed_count=7
        assert updated_job.status == "failed"
        assert updated_job.error_code == "DELIVERY_FAILED"
        assert updated_job.media_count == 7
        assert updated_job.failed_count == 7
        # 20. Job never failed with failed_count=0
        assert updated_job.failed_count > 0


@pytest.mark.asyncio
async def test_partial_failure_2_out_of_7(test_db: AsyncSession, tmp_path: Path) -> None:
    repo = JobRepository(test_db)
    job = await repo.create_job(
        inbound_message_id="msg_partial_photos",
        webhook_event_id="evt_partial_photos",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
    )

    for i in range(1, 8):
        fn = tmp_path / f"photo_{i:03d}.jpg"
        fn.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
        item = DownloadItem(
            job_id=job.id,
            position=i,
            media_type="photo",
            source_url=f"https://p16.tiktokcdn.com/slide_{i}.jpg",
            local_filename=str(fn),
            status="pending",
        )
        test_db.add(item)
    await test_db.commit()

    from app.database.connection import get_session_maker
    sm = get_session_maker()
    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)

    def side_effect(**kwargs: str) -> GatewayMessageResponse:
        pos = int(kwargs["idempotency_key"].split("-")[-1])
        if pos in (1, 2):
            raise GatewayError("Gateway error on item")
        return GatewayMessageResponse(
            status="ok",
            message_id=f"gw_msg_{pos}",
            queue_status="queued",
            delivery_status=None,
            http_status=200,
        )

    mock_gateway.send_media.side_effect = side_effect

    worker = QueueWorker(sm)
    worker.gateway = mock_gateway
    await worker._send_all_media_items(job.id)

    async with sm() as session:
        r_repo = JobRepository(session)
        updated_job = await r_repo.get_by_id(job.id)
        assert updated_job is not None
        # 17. 2 out of 7 failed produces PARTIAL_FAILURE
        assert updated_job.status == "failed"
        assert updated_job.error_code == "PARTIAL_FAILURE"
        assert updated_job.media_count == 7
        assert updated_job.failed_count == 2
        assert updated_job.sent_count == 5


@pytest.mark.asyncio
async def test_sync_job_status_updates_error_and_counters_even_if_status_same(
    test_db: AsyncSession
) -> None:
    service = GatewayDeliveryService(test_db)
    repo = JobRepository(test_db)
    job = await repo.create_job(
        inbound_message_id="msg_sync_err",
        webhook_event_id="evt_sync_err",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
    )
    job.status = "failed"
    job.error_code = "PARTIAL_FAILURE"
    job.error_message = "Sebagian media gagal dikirim (1/2 item)."
    job.sent_count = 1
    job.failed_count = 1

    item1 = DownloadItem(job_id=job.id, position=1, media_type="photo", source_url="http://s1", status="failed")
    item2 = DownloadItem(job_id=job.id, position=2, media_type="photo", source_url="http://s2", status="failed")
    test_db.add(item1)
    test_db.add(item2)
    await test_db.commit()

    # 18. Sync job status updates error_code and counters even when status remains 'failed'
    await service.sync_job_status(job.id)
    assert job.error_code == "DELIVERY_FAILED"
    assert job.failed_count == 2
    assert job.sent_count == 0


@pytest.mark.asyncio
async def test_retry_job_cleans_terminal_state_and_rejects_gateway_message_id(
    test_db: AsyncSession
) -> None:
    repo = JobRepository(test_db)

    # Job 1: Clean retry (no items have gateway_message_id)
    job1 = await repo.create_job(
        inbound_message_id="msg_retry_clean",
        webhook_event_id="evt_retry_clean",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
    )
    job1.status = "failed"
    job1.error_code = "DELIVERY_FAILED"
    job1.completed_at = utc_now()
    job1.attempt_count = 2

    item1 = DownloadItem(job_id=job1.id, position=1, media_type="photo", source_url="http://s1", status="failed")
    item1.gateway_error_code = "GATEWAY_ERROR"
    test_db.add(item1)

    await test_db.commit()

    # Test CLI retry-job command
    with patch("app.database.connection.AsyncSessionLocal", return_value=test_db):
        args = argparse.Namespace(id=job1.id)
        await retry_job_cmd(args)

    await test_db.refresh(job1)
    await test_db.refresh(item1)

    # 21 & 22. Retry cleans completed_at, error fields, and resets status to queued/pending
    assert job1.status == "queued"
    assert job1.completed_at is None
    assert job1.error_code is None
    assert job1.attempt_count == 0
    assert item1.status == "pending"
    assert item1.gateway_error_code is None

    # Job 2: Rejects retry if any item has gateway_message_id
    job2 = await repo.create_job(
        inbound_message_id="msg_retry_gw",
        webhook_event_id="evt_retry_gw",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
    )
    job2.status = "failed"

    item2 = DownloadItem(job_id=job2.id, position=1, media_type="photo", source_url="http://s2", status="failed")
    item2.gateway_message_id = "gw_already_sent_123"
    test_db.add(item2)
    await test_db.commit()

    # 23 & 24. Retry rejected when item has gateway_message_id
    with patch("app.database.connection.AsyncSessionLocal", return_value=test_db), \
         patch("sys.exit") as mock_exit:
        args2 = argparse.Namespace(id=job2.id)
        await retry_job_cmd(args2)
        mock_exit.assert_called_once_with(1)
        assert item2.gateway_message_id == "gw_already_sent_123"


@pytest.mark.asyncio
async def test_regressions_video_reconciler_and_concurrency(test_db: AsyncSession, tmp_path: Path) -> None:
    # 25. TikTok Video regression
    service = DownloaderService()
    snapshot = JobDownloadSnapshot(
        id="job_vid",
        original_url="https://www.tiktok.com/@user/video/7123456789012345678",
        canonical_url="https://www.tiktok.com/@user/video/7123456789012345678",
        platform="tiktok",
        items=(),
    )
    video_meta = TikTokContentMetadata(
        content_type="video",
        title="Video Post",
        author="User",
        duration_seconds=15,
        items=[TikTokMediaItemMetadata(position=1, source_url="http://vid.mp4", media_type="video")],
    )
    with patch.object(service.yt_dlp, "extract_metadata", new_callable=AsyncMock) as mock_yt:
        mock_yt.return_value = video_meta
        res = await service.extract_metadata(snapshot, tmp_path)
        assert res.metadata.content_type == "video"
