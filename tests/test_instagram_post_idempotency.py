from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from app.database.models import DownloadItem
from app.database.repositories import JobRepository
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.schemas import GatewayMessageResponse
from app.queue.worker import QueueWorker
from sqlalchemy.ext.asyncio import AsyncSession


async def _create_ig_post_job(test_db: AsyncSession, tmp_path: Path, job_id_suffix: str) -> str:
    job = await JobRepository(test_db).create_job(
        inbound_message_id=f"msg_ig_post_{job_id_suffix}",
        webhook_event_id=f"evt_ig_post_{job_id_suffix}",
        sender_number="628111222333",
        original_url="https://www.instagram.com/p/DbgFWkXMQXa/",
        canonical_url="https://www.instagram.com/p/DbgFWkXMQXa/",
        platform="instagram",
    )
    for pos, (mtype, fname) in enumerate(
        [("photo", "ig_001.jpg"), ("photo", "ig_002.jpg"), ("video", "ig_003.mp4")],
        start=1,
    ):
        fn = tmp_path / fname
        if mtype == "photo":
            fn.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
        else:
            fn.write_bytes(b"\x00\x00\x00\x1cftypisom")
        test_db.add(
            DownloadItem(
                job_id=job.id,
                position=pos,
                media_type=mtype,
                source_url=f"http://src/{pos}",
                local_filename=str(fn),
                status="pending",
            )
        )
    await test_db.commit()
    return job.id


@pytest.mark.asyncio
async def test_ig_post_idempotency_key_per_item(test_db: AsyncSession, tmp_path: Path) -> None:
    from app.database.connection import get_session_maker
    sm = get_session_maker()
    job_id = await _create_ig_post_job(test_db, tmp_path, "idemp01")

    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)
    mock_gateway.send_media.return_value = GatewayMessageResponse(
        status="ok", message_id="gw_ok", queue_status="queued", http_status=200
    )
    worker = QueueWorker(sm)
    worker.gateway = mock_gateway

    await worker._send_all_media_items(job_id)

    calls = mock_gateway.send_media.call_args_list
    assert len(calls) == 3
    # All captions empty
    for c in calls:
        assert c.kwargs["caption"] == ""
    # Idempotency keys per media_type + position
    assert calls[0].kwargs["idempotency_key"] == "instagram-msg_ig_post_idemp01-photo-001"
    assert calls[1].kwargs["idempotency_key"] == "instagram-msg_ig_post_idemp01-photo-002"
    assert calls[2].kwargs["idempotency_key"] == "instagram-msg_ig_post_idemp01-video-003"


@pytest.mark.asyncio
async def test_ig_post_photo_sent_as_image_to_gateway(test_db: AsyncSession, tmp_path: Path) -> None:
    from app.database.connection import get_session_maker
    sm = get_session_maker()
    job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_ig_photo_gw",
        webhook_event_id="evt_ig_photo_gw",
        sender_number="628111222333",
        original_url="https://www.instagram.com/p/PhotoGw1/",
        canonical_url="https://www.instagram.com/p/PhotoGw1/",
        platform="instagram",
    )
    fn = tmp_path / "ig_photo.jpg"
    fn.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
    test_db.add(
        DownloadItem(
            job_id=job.id,
            position=1,
            media_type="photo",
            source_url="http://src/1.jpg",
            local_filename=str(fn),
            status="pending",
        )
    )
    await test_db.commit()

    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)
    mock_gateway.send_media.return_value = GatewayMessageResponse(
        status="ok", message_id="gw_ok", queue_status="queued", http_status=200
    )
    worker = QueueWorker(sm)
    worker.gateway = mock_gateway

    await worker._send_all_media_items(job.id)

    call = mock_gateway.send_media.call_args_list[0]
    assert call.kwargs["media_type"] == "photo"
    assert call.kwargs["caption"] == ""


@pytest.mark.asyncio
async def test_ig_post_video_sent_as_video_to_gateway(test_db: AsyncSession, tmp_path: Path) -> None:
    from app.database.connection import get_session_maker
    sm = get_session_maker()
    job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_ig_video_gw",
        webhook_event_id="evt_ig_video_gw",
        sender_number="628111222333",
        original_url="https://www.instagram.com/p/VidGw1/",
        canonical_url="https://www.instagram.com/p/VidGw1/",
        platform="instagram",
    )
    fn = tmp_path / "ig_video.mp4"
    fn.write_bytes(b"\x00\x00\x00\x1cftypisom")
    test_db.add(
        DownloadItem(
            job_id=job.id,
            position=1,
            media_type="video",
            source_url="http://src/1.mp4",
            local_filename=str(fn),
            status="pending",
        )
    )
    await test_db.commit()

    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)
    mock_gateway.send_media.return_value = GatewayMessageResponse(
        status="ok", message_id="gw_ok", queue_status="queued", http_status=200
    )
    worker = QueueWorker(sm)
    worker.gateway = mock_gateway

    await worker._send_all_media_items(job.id)

    call = mock_gateway.send_media.call_args_list[0]
    assert call.kwargs["media_type"] == "video"
    assert call.kwargs["caption"] == ""


@pytest.mark.asyncio
async def test_ig_reel_idempotency_key_unchanged(test_db: AsyncSession, tmp_path: Path) -> None:
    """Regression: IG /reel/ still uses single video key, not per-item."""
    from app.database.connection import get_session_maker
    sm = get_session_maker()
    job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_ig_reel_reg",
        webhook_event_id="evt_ig_reel_reg",
        sender_number="628111222333",
        original_url="https://www.instagram.com/reel/C123/",
        canonical_url="https://www.instagram.com/reel/C123/",
        platform="instagram",
    )
    fn = tmp_path / "reel.mp4"
    fn.write_bytes(b"\x00\x00\x00\x1cftypisom")
    test_db.add(
        DownloadItem(
            job_id=job.id,
            position=1,
            media_type="video",
            source_url="http://src/reel.mp4",
            local_filename=str(fn),
            status="pending",
        )
    )
    await test_db.commit()

    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)
    mock_gateway.send_media.return_value = GatewayMessageResponse(
        status="ok", message_id="gw_ok", queue_status="queued", http_status=200
    )
    worker = QueueWorker(sm)
    worker.gateway = mock_gateway

    await worker._send_all_media_items(job.id)

    call = mock_gateway.send_media.call_args_list[0]
    assert call.kwargs["idempotency_key"] == "instagram-msg_ig_reel_reg-video"
    assert call.kwargs["caption"] == ""


@pytest.mark.asyncio
async def test_item_with_gateway_message_id_not_resent(test_db: AsyncSession, tmp_path: Path) -> None:
    from app.database.connection import get_session_maker
    sm = get_session_maker()
    job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_ig_skip",
        webhook_event_id="evt_ig_skip",
        sender_number="628111222333",
        original_url="https://www.instagram.com/p/Skip1/",
        canonical_url="https://www.instagram.com/p/Skip1/",
        platform="instagram",
    )
    fn1 = tmp_path / "already_sent.jpg"
    fn1.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
    fn2 = tmp_path / "not_sent.jpg"
    fn2.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
    test_db.add(
        DownloadItem(
            job_id=job.id,
            position=1,
            media_type="photo",
            source_url="http://src/1.jpg",
            local_filename=str(fn1),
            status="sent",
            gateway_message_id="already_delivered_123",
        )
    )
    test_db.add(
        DownloadItem(
            job_id=job.id,
            position=2,
            media_type="photo",
            source_url="http://src/2.jpg",
            local_filename=str(fn2),
            status="pending",
        )
    )
    await test_db.commit()

    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)
    mock_gateway.send_media.return_value = GatewayMessageResponse(
        status="ok", message_id="gw_ok_new", queue_status="queued", http_status=200
    )
    worker = QueueWorker(sm)
    worker.gateway = mock_gateway

    await worker._send_all_media_items(job.id)

    # Only 1 call (position 2), position 1 skipped due to gateway_message_id
    calls = mock_gateway.send_media.call_args_list
    assert len(calls) == 1
    assert calls[0].kwargs["idempotency_key"] == "instagram-msg_ig_skip-photo-002"


@pytest.mark.asyncio
async def test_retry_does_not_create_duplicate_items(test_db: AsyncSession, tmp_path: Path) -> None:
    """Regression: re-extraction must not duplicate existing items by position."""
    from app.database.connection import get_session_maker
    from app.downloader.dtos import ExtractedMetadataResult
    from app.downloader.metadata import MediaContentMetadata, MediaItemMetadata
    sm = get_session_maker()
    job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_ig_retry",
        webhook_event_id="evt_ig_retry",
        sender_number="628111222333",
        original_url="https://www.instagram.com/p/Retry1/",
        canonical_url="https://www.instagram.com/p/Retry1/",
        platform="instagram",
    )
    fn = tmp_path / "retry_001.jpg"
    fn.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
    test_db.add(
        DownloadItem(
            job_id=job.id,
            position=1,
            media_type="photo",
            source_url="http://src/old.jpg",
            local_filename=str(fn),
            status="pending",
        )
    )
    await test_db.commit()

    worker = QueueWorker(sm)
    new_meta = MediaContentMetadata(
        content_type="photo",
        title="Retry",
        author="u",
        duration_seconds=0,
        items=[MediaItemMetadata(position=1, source_url="http://src/new.jpg", media_type="photo")],
    )
    extracted = ExtractedMetadataResult(
        canonical_url="https://www.instagram.com/p/Retry1/",
        provider=AsyncMock(),
        metadata=new_meta,
    )
    await worker._save_extracted_metadata(job.id, extracted)

    from sqlalchemy import select
    async with sm() as session:
        stmt = select(DownloadItem).where(DownloadItem.job_id == job.id)
        result = await session.execute(stmt)
        items = result.scalars().all()
    assert len(items) == 1
    assert items[0].position == 1
