from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import DownloadItem
from app.database.repositories import JobRepository
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.schemas import GatewayMessageResponse
from app.queue.worker import QueueWorker
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_no_caption_policy_for_all_media(test_db: AsyncSession, tmp_path: Path) -> None:
    # 1. TikTok Photo (Position 1 & Position 2..N) -> caption == "" & idempotency key intact
    photo_job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_photo_nocap",
        webhook_event_id="evt_photo_nocap",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        canonical_url="https://www.tiktok.com/@user/photo/7668360024648846599",
        platform="tiktok",
    )
    for pos in range(1, 4):
        fn = tmp_path / f"photo_{pos:03d}.jpg"
        fn.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")
        test_db.add(
            DownloadItem(
                job_id=photo_job.id,
                position=pos,
                media_type="photo",
                source_url=f"http://src/{pos}.jpg",
                local_filename=str(fn),
                status="pending",
            )
        )

    # 2. TikTok Video -> caption == "" & idempotency key intact
    video_job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_video_nocap",
        webhook_event_id="evt_video_nocap",
        sender_number="628111222333",
        original_url="https://www.tiktok.com/@user/video/7123456789012345678",
        canonical_url="https://www.tiktok.com/@user/video/7123456789012345678",
        platform="tiktok",
    )
    fn_vid = tmp_path / "video.mp4"
    fn_vid.write_bytes(b"\x00\x00\x00\x1cftypisom")
    test_db.add(
        DownloadItem(
            job_id=video_job.id,
            position=1,
            media_type="video",
            source_url="http://src/vid.mp4",
            local_filename=str(fn_vid),
            status="pending",
        )
    )

    # 3. Instagram Reels -> caption == "" & idempotency key intact
    ig_job = await JobRepository(test_db).create_job(
        inbound_message_id="msg_ig_nocap",
        webhook_event_id="evt_ig_nocap",
        sender_number="628111222333",
        original_url="https://www.instagram.com/reels/C12345678/",
        canonical_url="https://www.instagram.com/reels/C12345678/",
        platform="instagram",
    )
    fn_ig = tmp_path / "ig_reel.mp4"
    fn_ig.write_bytes(b"\x00\x00\x00\x1cftypisom")
    test_db.add(
        DownloadItem(
            job_id=ig_job.id,
            position=1,
            media_type="video",
            source_url="http://src/ig.mp4",
            local_filename=str(fn_ig),
            status="pending",
        )
    )

    await test_db.commit()

    from app.database.connection import get_session_maker
    sm = get_session_maker()
    mock_gateway = AsyncMock(spec=FarrosWAGatewayClient)
    mock_gateway.send_media.return_value = GatewayMessageResponse(
        status="ok", message_id="gw_ok", queue_status="queued", http_status=200
    )

    worker = QueueWorker(sm)
    worker.gateway = mock_gateway

    # Run sending for photo job
    await worker._send_all_media_items(photo_job.id)
    photo_calls = mock_gateway.send_media.call_args_list[:3]

    # Test Rule 1: First photo position 1 HAS NO CAPTION
    assert photo_calls[0].kwargs["caption"] == ""
    assert photo_calls[0].kwargs["idempotency_key"] == "tiktok-msg_photo_nocap-photo-001"

    # Test Rule 2: Photo position 2..N HAVE NO CAPTION
    assert photo_calls[1].kwargs["caption"] == ""
    assert photo_calls[1].kwargs["idempotency_key"] == "tiktok-msg_photo_nocap-photo-002"
    assert photo_calls[2].kwargs["caption"] == ""
    assert photo_calls[2].kwargs["idempotency_key"] == "tiktok-msg_photo_nocap-photo-003"

    mock_gateway.send_media.reset_mock()

    # Run sending for TikTok video job
    await worker._send_all_media_items(video_job.id)
    vid_call = mock_gateway.send_media.call_args_list[0]

    # Test Rule 3: TikTok video HAS NO CAPTION
    assert vid_call.kwargs["caption"] == ""
    # Test Rule 6: Idempotency key for TikTok video remains tiktok-{inbound_message_id}-video
    assert vid_call.kwargs["idempotency_key"] == "tiktok-msg_video_nocap-video"

    mock_gateway.send_media.reset_mock()

    # Run sending for Instagram Reels job
    await worker._send_all_media_items(ig_job.id)
    ig_call = mock_gateway.send_media.call_args_list[0]

    # Test Rule 4: Instagram Reels HAS NO CAPTION
    assert ig_call.kwargs["caption"] == ""
    # Test Rule 6: Idempotency key for Instagram Reels remains instagram-{inbound_message_id}-video
    assert ig_call.kwargs["idempotency_key"] == "instagram-msg_ig_nocap-video"


@pytest.mark.asyncio
async def test_client_send_media_omits_caption_multipart_field_when_caption_empty(tmp_path: Path) -> None:
    client = FarrosWAGatewayClient()
    img_file = tmp_path / "photo_001.jpg"
    img_file.write_bytes(b"\xff\xd8\xff\xe0\x00\x10JFIF")

    with patch.object(client, "_execute_request", new_callable=AsyncMock) as mock_exec:
        mock_exec.return_value = GatewayMessageResponse(status="ok", message_id="gw_msg_001", http_status=200)

        await client.send_media(
            to="628123456789",
            media_type="photo",
            file_path=str(img_file),
            caption="",  # empty caption
            external_reference="job_123",
            idempotency_key="tiktok-msg1-photo-001",
        )

        # Test Rule 5: Multipart request does NOT contain "caption" field when caption is empty/falsy
        data_payload = mock_exec.call_args.kwargs.get("data", {})
        assert "caption" not in data_payload
