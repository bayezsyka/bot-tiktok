import asyncio
import hashlib
import json
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.connection import get_db, get_session_maker
from app.database.models import DownloadItem, utc_now
from app.database.repositories import (
    AllowedNumberRepository,
    JobRepository,
    UnmappedLidRepository,
    WebhookEventRepository,
)
from app.downloader.dtos import JobDownloadSnapshot
from app.downloader.service import DownloaderService
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.delivery_service import (
    GatewayDeliveryService,
    StatusSyncResult,
    dispatch_failure_notification,
)
from app.security.rate_limit import check_webhook_rate_limit
from app.security.urls import (
    extract_supported_media_url,
    normalize_phone_number,
    resolve_lid_to_phone,
)
from app.webhooks.parser import parse_inbound_message
from app.webhooks.schemas import WebhookEventResponse
from app.webhooks.signature import validate_webhook_headers_and_signature

logger = logging.getLogger(__name__)
router = APIRouter()


async def _send_initial_reply_background(sender_number: str, inbound_id: str, text: str | None = None) -> None:
    """Send acknowledgment message in background so webhook returns 200 immediately."""
    try:
        client = FarrosWAGatewayClient()
        ack_text = text or "oke, konten sedang diunduh dan diproses. kalau sudah selesai, akan langsung kami kirim."
        await client.send_text(
            to=sender_number,
            text=ack_text,
            external_reference=f"media-{inbound_id}",
            idempotency_key=f"media-{inbound_id}-processing",
        )
    except Exception as e:
        logger.error(f"Failed to send initial reply for inbound {inbound_id}: {e}")


async def _send_text_reply_background(sender_number: str, text: str, ref_id: str, key_suffix: str = "msg") -> None:
    """Send text reply asynchronously in background."""
    try:
        client = FarrosWAGatewayClient()
        await client.send_text(
            to=sender_number,
            text=text,
            external_reference=f"media-{ref_id}",
            idempotency_key=f"media-{ref_id}-{key_suffix}",
        )
    except Exception as e:
        logger.error(f"Failed to send background reply to {sender_number}: {e}")


async def _handle_outbound_status_event(db: AsyncSession, event_type: str, payload: dict) -> StatusSyncResult | None:
    data = payload.get("data", {})
    msg_id = data.get("id") or data.get("message_id")
    if not msg_id:
        return None

    stmt = select(DownloadItem).where(DownloadItem.gateway_message_id == msg_id)
    result = await db.execute(stmt)
    item = result.scalar_one_or_none()

    if not item:
        return None

    q_status = None
    d_status = None
    error_message = data.get("error_message") or data.get("last_error_message") or data.get("error")
    error_code = data.get("error_code") or data.get("last_error_code") or data.get("code")

    if event_type == "message.sent":
        q_status = "sent"
    elif event_type == "message.delivered":
        d_status = "delivered"
    elif event_type == "message.read":
        d_status = "read"
    elif event_type == "message.played":
        d_status = "played"
    elif event_type == "message.failed":
        q_status = "failed"

    delivery_service = GatewayDeliveryService(db)

    return await delivery_service.process_outbound_status(
        item=item,
        d_status=d_status,
        q_status=q_status,
        error_message=error_message,
        error_code=error_code
    )


@router.post("/farros-wa", response_model=WebhookEventResponse)
async def handle_farros_wa_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> WebhookEventResponse:
    raw_body = await request.body()
    headers = request.headers

    # 1. Verify headers & signature (raises 401 if invalid)
    event_type, event_id, timestamp = validate_webhook_headers_and_signature(headers, raw_body)

    # 2. Process message.inbound and outbound status events
    supported_events = ["message.inbound", "message.sent", "message.delivered", "message.read", "message.played", "message.failed"]
    if event_type not in supported_events:
        return WebhookEventResponse(status="ok", message=f"Ignored unsupported event type: {event_type}")

    # 3. Check idempotency X-FWAG-Event-Id
    payload_hash = hashlib.sha256(raw_body).hexdigest()
    event_repo = WebhookEventRepository(db)
    existing_event = await event_repo.get_by_event_id(event_id)
    if existing_event:
        if existing_event.payload_hash == payload_hash:
            return WebhookEventResponse(status="ok", message="Duplicate event ignored")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Duplicate event ID with different payload",
        )

    # 4. Parse payload defensively
    try:
        payload_dict = json.loads(raw_body.decode("utf-8"))
    except Exception:
        return WebhookEventResponse(status="ok", message="Invalid JSON payload")

    if event_type in ["message.sent", "message.delivered", "message.read", "message.played", "message.failed"]:
        try:
            await event_repo.create_event(event_id=event_id, event_type=event_type, payload_hash=payload_hash)
            sync_result = await _handle_outbound_status_event(db, event_type, payload_dict)
            await db.commit()
            await db.close()
            if sync_result and sync_result.notification_required:
                await dispatch_failure_notification(get_session_maker(), sync_result.job_id)
            return WebhookEventResponse(status="ok", message="Status updated successfully")
        except IntegrityError:
            await db.rollback()
            return WebhookEventResponse(status="ok", message="Concurrent duplicate event ignored")
        except Exception as e:
            await db.rollback()
            logger.error(f"Error handling outbound status event: {e}")
            raise HTTPException(status_code=500, detail="Internal Server Error") from e

    parsed = parse_inbound_message(payload_dict)
    if not parsed:
        return WebhookEventResponse(status="ok", message="Could not parse inbound message")

    if parsed.is_group or parsed.from_me:
        return WebhookEventResponse(status="ok", message="Ignored group/self message")

    # 5. Resolve sender (LID vs Phone)
    number_repo = AllowedNumberRepository(db)
    allowed_number = None

    if parsed.is_lid or "@lid" in parsed.sender_number:
        lid_to_lookup = parsed.lid_number
        if not lid_to_lookup:
            lid_to_lookup = parsed.sender_number.split("@")[0].strip() if parsed.sender_number else ""
            lid_to_lookup = "".join(ch for ch in lid_to_lookup if ch.isdigit())

        if lid_to_lookup:
            allowed_number = await number_repo.get_by_lid(lid_to_lookup)
            if not allowed_number:
                mapped_phone = resolve_lid_to_phone(lid_to_lookup)
                if mapped_phone:
                    allowed_number = await number_repo.get_by_phone(mapped_phone)

        if not allowed_number:
            if lid_to_lookup:
                unmapped_repo = UnmappedLidRepository(db)
                await unmapped_repo.upsert_unmapped(
                    lid_number=lid_to_lookup,
                    inbound_message_id=parsed.inbound_message_id,
                    message_preview=parsed.message_text,
                )
                await db.commit()
                logger.warning(
                    f"Recorded unmapped LID {lid_to_lookup} for inbound_id {parsed.inbound_message_id}"
                )
            return WebhookEventResponse(status="ok", message="Ignored LID sender without routable phone number")
    else:
        norm_phone_candidate = normalize_phone_number(parsed.sender_number)
        if not norm_phone_candidate:
            return WebhookEventResponse(status="ok", message="Invalid sender phone format")
        allowed_number = await number_repo.get_by_phone(norm_phone_candidate)

    if not allowed_number or not allowed_number.is_active:
        return WebhookEventResponse(status="ok", message="Sender not in active whitelist")

    norm_phone = allowed_number.phone_number

    # 6. Check rate limit
    try:
        check_webhook_rate_limit(norm_phone)
    except Exception:
        return WebhookEventResponse(status="ok", message="Rate limit exceeded for sender")

    job_repo = JobRepository(db)
    msg_text_clean = (parsed.message_text or "").strip().lower()

    # 7. Check if user is replying to an awaiting_choice job
    awaiting_job = await job_repo.get_awaiting_choice_job_for_number(norm_phone)
    if awaiting_job:
        is_photo_choice = msg_text_clean in ("1", "foto", "photo", "gambar", "f", "1️⃣")
        is_video_choice = msg_text_clean in ("2", "video", "vidio", "v", "mp4", "2️⃣")

        if is_photo_choice or is_video_choice:
            awaiting_job.selected_mode = "photo" if is_photo_choice else "video"
            awaiting_job.status = "queued"
            awaiting_job.queued_at = utc_now()
            await event_repo.create_event(event_id=event_id, event_type=event_type, payload_hash=payload_hash)
            await db.commit()

            ack_text = (
                "oke, foto-foto asli sedang diunduh dan diproses. akan segera dikirim."
                if is_photo_choice
                else "oke, video ber-sound (kualitas foto asli) sedang diproses. akan segera dikirim."
            )
            asyncio.create_task(_send_text_reply_background(norm_phone, ack_text, parsed.inbound_message_id, "choice-ack"))
            return WebhookEventResponse(status="ok", message=f"Choice applied: {awaiting_job.selected_mode}")

    # 8. Check if sender already has an active job
    active_job = await job_repo.get_active_job_for_number(norm_phone)
    if active_job:
        return WebhookEventResponse(status="ok", message="Sender already has an active job")

    # 9. Extract & validate supported media URL
    extracted = extract_supported_media_url(parsed.message_text)
    if not extracted:
        if awaiting_job:
            reminder_text = (
                "📸 *Konten TikTok Foto & Musik*\n\n"
                "Silakan balas angka atau format pilihanmu:\n"
                "1️⃣ Balas *1* (atau *foto*) untuk unduh Foto saja\n"
                "2️⃣ Balas *2* (atau *video*) untuk unduh Video ber-musik\n\n"
                "_(Kirim angka 1 atau 2)_"
            )
            asyncio.create_task(_send_text_reply_background(norm_phone, reminder_text, parsed.inbound_message_id, "reminder"))
            return WebhookEventResponse(status="ok", message="Sent choice reminder to sender")
        return WebhookEventResponse(status="ok", message="No valid media URL found in message")

    # Check duplicate inbound_message_id
    existing_job = await job_repo.get_by_inbound_message_id(parsed.inbound_message_id)
    if existing_job:
        return WebhookEventResponse(status="ok", message="Inbound message already processed")

    # 10. Check if this is a TikTok Photo + Sound content to offer interactive choice
    downloader = DownloaderService()
    canonical_url = None
    music_url = None
    is_interactive_photo = False

    if extracted.platform == "tiktok" and ("/photo/" in extracted.original_url.lower() or "vt.tiktok.com" in extracted.original_url.lower() or "vm.tiktok.com" in extracted.original_url.lower()):
        try:
            snapshot_temp = JobDownloadSnapshot(
                id="temp",
                original_url=extracted.original_url,
                canonical_url=None,
                platform="tiktok",
                items=(),
            )
            resolved_tmp = await downloader.resolve_canonical_url(snapshot_temp)
            if resolved_tmp and "/photo/" in resolved_tmp.lower():
                canonical_url = resolved_tmp
                meta_res = await downloader.extract_metadata(
                    JobDownloadSnapshot(
                        id="temp",
                        original_url=extracted.original_url,
                        canonical_url=canonical_url,
                        platform="tiktok",
                        items=(),
                    ),
                    job_dir=Path("/tmp"),
                )
                if meta_res and meta_res.metadata and meta_res.metadata.content_type == "photo":
                    music_url = meta_res.metadata.music_url
                    if music_url:
                        is_interactive_photo = True
        except Exception as e:
            logger.warning(f"Preliminary metadata probe failed: {e}")

    # 11. Create Job
    job_status = "awaiting_choice" if is_interactive_photo else "queued"
    try:
        await event_repo.create_event(event_id=event_id, event_type=event_type, payload_hash=payload_hash)
        await job_repo.create_job(
            inbound_message_id=parsed.inbound_message_id,
            webhook_event_id=event_id,
            sender_number=norm_phone,
            original_url=extracted.original_url,
            canonical_url=canonical_url,
            platform=extracted.platform,
            status=job_status,
            music_url=music_url,
        )
        await number_repo.increment_job_stats(norm_phone)
        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        concurrent_event = await event_repo.get_by_event_id(event_id)
        if concurrent_event:
            if concurrent_event.payload_hash == payload_hash:
                return WebhookEventResponse(status="ok", message="Duplicate event ignored")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Duplicate event ID with different payload",
            ) from e
        concurrent_job = await job_repo.get_by_inbound_message_id(parsed.inbound_message_id)
        if concurrent_job:
            return WebhookEventResponse(status="ok", message="Inbound message already processed")
        raise

    # 12. Send initial message
    if is_interactive_photo:
        prompt_text = (
            "📸 *Konten TikTok Foto & Musik Terdeteksi!*\n\n"
            "Silakan pilih format yang ingin kamu download:\n"
            "1️⃣ Balas *1* (atau *foto*) untuk unduh Foto saja\n"
            "2️⃣ Balas *2* (atau *video*) untuk unduh Video ber-musik (resolusi & kualitas asli)\n\n"
            "_(Balas dengan mengetik 1 atau 2)_"
        )
        asyncio.create_task(_send_text_reply_background(norm_phone, prompt_text, parsed.inbound_message_id, "prompt"))
    else:
        asyncio.create_task(_send_initial_reply_background(norm_phone, parsed.inbound_message_id))

    return WebhookEventResponse(status="ok", message="Job queued successfully")
