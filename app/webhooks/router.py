import asyncio
import hashlib
import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.connection import get_db
from app.database.repositories import AllowedNumberRepository, JobRepository, WebhookEventRepository
from app.gateway.client import FarrosWAGatewayClient
from app.security.rate_limit import check_webhook_rate_limit
from app.security.urls import extract_tiktok_url, normalize_phone_number, resolve_lid_to_phone
from app.webhooks.parser import parse_inbound_message
from app.webhooks.schemas import WebhookEventResponse
from app.webhooks.signature import validate_webhook_headers_and_signature

logger = logging.getLogger(__name__)
router = APIRouter()


async def _send_initial_reply_background(sender_number: str, inbound_id: str) -> None:
    """Send acknowledgment message in background so webhook returns 200 immediately."""
    try:
        client = FarrosWAGatewayClient()
        ack_text = "oke, konten tiktok sedang diunduh dan diproses. kalau sudah selesai, akan langsung kami kirim."
        await client.send_text(
            to=sender_number,
            text=ack_text,
            external_reference=f"tiktok-{inbound_id}",
            idempotency_key=f"tiktok-{inbound_id}-processing",
        )
    except Exception as e:
        logger.error(f"Failed to send initial reply for inbound {inbound_id}: {e}")


@router.post("/farros-wa", response_model=WebhookEventResponse)
async def handle_farros_wa_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> WebhookEventResponse:
    raw_body = await request.body()
    headers = request.headers

    # 1. Verify headers & signature (raises 401 if invalid)
    event_type, event_id, timestamp = validate_webhook_headers_and_signature(headers, raw_body)

    # 2. Only process message.inbound event
    if event_type != "message.inbound":
        return WebhookEventResponse(status="ok", message="Ignored non-message.inbound event")

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

    parsed = parse_inbound_message(payload_dict)
    if not parsed:
        return WebhookEventResponse(status="ok", message="Could not parse inbound message")

    # Ignore group messages, from_me, status, broadcast
    if parsed.is_group or parsed.from_me:
        return WebhookEventResponse(status="ok", message="Ignored group/self message")

    # 5. Resolve LID or normalize phone number
    norm_phone = None
    if parsed.is_lid or "@lid" in parsed.sender_number:
        lid_to_lookup = parsed.lid_number
        if not lid_to_lookup:
            lid_to_lookup = parsed.sender_number.split("@")[0].strip() if parsed.sender_number else ""
            lid_to_lookup = "".join(ch for ch in lid_to_lookup if ch.isdigit())

        mapped_phone = resolve_lid_to_phone(lid_to_lookup) if lid_to_lookup else None
        if not mapped_phone:
            logger.warning(
                f"Received webhook payload with unmapped LID sender for inbound_id: {parsed.inbound_message_id}"
            )
            return WebhookEventResponse(status="ok", message="Ignored LID sender without routable phone number")
        norm_phone = mapped_phone
    else:
        norm_phone = normalize_phone_number(parsed.sender_number)
        if not norm_phone:
            return WebhookEventResponse(status="ok", message="Invalid sender phone format")


    number_repo = AllowedNumberRepository(db)
    allowed_number = await number_repo.get_by_phone(norm_phone)
    if not allowed_number or not allowed_number.is_active:
        return WebhookEventResponse(status="ok", message="Sender not in active whitelist")

    # 6. Check rate limit
    try:
        check_webhook_rate_limit(norm_phone)
    except Exception:
        # Rate limited: ignore without response or return HTTP 200
        return WebhookEventResponse(status="ok", message="Rate limit exceeded for sender")

    # 7. Check if sender already has an active job
    job_repo = JobRepository(db)
    active_job = await job_repo.get_active_job_for_number(norm_phone)
    if active_job:
        return WebhookEventResponse(status="ok", message="Sender already has an active job")

    # 8. Extract & validate TikTok URL (locally without network requests)
    tiktok_url = extract_tiktok_url(parsed.message_text)
    if not tiktok_url:
        return WebhookEventResponse(status="ok", message="No valid TikTok URL found in message")

    # Check if this exact inbound_message_id is already in DownloadJob
    existing_job = await job_repo.get_by_inbound_message_id(parsed.inbound_message_id)
    if existing_job:
        return WebhookEventResponse(status="ok", message="Inbound message already processed")

    # 9. Create WebhookEvent and DownloadJob in a single atomic transaction
    try:
        await event_repo.create_event(event_id=event_id, event_type=event_type, payload_hash=payload_hash)
        await job_repo.create_job(
            inbound_message_id=parsed.inbound_message_id,
            webhook_event_id=event_id,
            sender_number=norm_phone,
            original_url=tiktok_url,
            canonical_url=None,
        )
        await number_repo.increment_job_stats(norm_phone)
        await db.commit()
    except IntegrityError as e:
        await db.rollback()
        # Check if due to concurrent duplicate delivery
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


    # 10. Send initial reply asynchronously/in background
    asyncio.create_task(_send_initial_reply_background(norm_phone, parsed.inbound_message_id))

    return WebhookEventResponse(status="ok", message="Job queued successfully")

