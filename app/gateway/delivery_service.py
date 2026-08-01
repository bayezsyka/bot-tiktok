import logging
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database.models import DownloadItem, DownloadJob, utc_now
from app.gateway.client import FarrosWAGatewayClient
from app.queue.service import QueueService

logger = logging.getLogger(__name__)

DELIVERY_RANKS = {
    "pending": 10,
    "server_ack": 20,
    "delivered": 30,
    "read": 40,
    "played": 40,
}

QUEUE_RANKS = {
    "queued": 10,
    "scheduled": 10,
    "processing": 20,
    "sent": 30,
}


@dataclass(frozen=True)
class StatusSyncResult:
    job_id: str
    notification_required: bool = False


@dataclass(frozen=True)
class FailureNotificationSnapshot:
    job_id: str
    sender_number: str
    inbound_message_id: str
    message: str


class GatewayDeliveryService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.queue_service = QueueService(db)

    async def process_outbound_status(
        self,
        item: DownloadItem,
        d_status: str | None = None,
        q_status: str | None = None,
        error_message: str | None = None,
        error_code: str | None = None,
        send_dispatched_at: datetime | None = None,
        whatsapp_message_id: str | None = None,
        defer_job_sync: bool = False,
    ) -> StatusSyncResult:
        """
        Process outbound status idempotently and monotonically.
        """
        item.last_gateway_sync_at = utc_now()

        # Detect SEND_RESULT_PENDING_TEMPORARY_ERROR:
        #   q_status == "processing", error_code contains the marker,
        #   and item already has a whatsapp/gateway message ID.
        if (
            q_status == "processing"
            and error_code == "SEND_RESULT_PENDING_TEMPORARY_ERROR"
            and item.gateway_message_id
        ):
            # Message was forwarded to WhatsApp but final result is unknown.
            # Do NOT mark as failed. Do NOT resend.
            if item.status not in ("completed", "failed", "cancelled"):
                if item.pending_since_at is None:
                    item.pending_since_at = send_dispatched_at or item.gateway_accepted_at or utc_now()
                item.status = "delivery_unknown_pending"
                item.gateway_queue_status = "processing"
                item.gateway_error_code = error_code
                item.gateway_error_message = (
                    error_message
                    or "Gateway sudah meneruskan pesan, tetapi hasil final dari WhatsApp belum diterima."
                )
            await self.db.flush()
            if defer_job_sync:
                return StatusSyncResult(job_id=item.job_id, notification_required=False)
            return await self.sync_job_status(item.job_id)

        # Handle Queue Status
        if q_status:
            if q_status == "failed":
                if item.status == "completed":
                    logger.warning(f"Anomaly: Received q_status='failed' for already completed item {item.id}")
                else:
                    item.gateway_queue_status = "failed"
                    item.status = "failed"
                    item.pending_since_at = None
                    item.gateway_error_code = error_code or "GATEWAY_DELIVERY_FAILED"
                    item.gateway_error_message = error_message
                    item.error_message = error_message
                    if not item.gateway_failed_at:
                        item.gateway_failed_at = utc_now()
            elif q_status == "cancelled":
                if item.status == "completed":
                    logger.warning(f"Anomaly: Received q_status='cancelled' for already completed item {item.id}")
                else:
                    item.gateway_queue_status = "cancelled"
                    item.status = "cancelled"
                    item.pending_since_at = None
                    item.gateway_error_code = error_code or "GATEWAY_CANCELLED"
                    item.gateway_error_message = error_message
                    item.error_message = error_message
                    if not item.gateway_failed_at:
                        item.gateway_failed_at = utc_now()
            else:
                if item.status == "completed":
                    logger.warning(f"Anomaly: Received q_status='{q_status}' for already completed item {item.id}")
                else:
                    old_q_rank = QUEUE_RANKS.get(item.gateway_queue_status or "", 0)
                    new_q_rank = QUEUE_RANKS.get(q_status, 0)
                    if new_q_rank >= old_q_rank:
                        item.gateway_queue_status = q_status
                        if item.status not in ("failed", "cancelled"):
                            if q_status == "sent":
                                item.status = "sent"
                                item.pending_since_at = None
                                if send_dispatched_at and not item.gateway_sent_at:
                                    item.gateway_sent_at = send_dispatched_at
                                elif not item.gateway_sent_at:
                                    item.gateway_sent_at = utc_now()
                            elif q_status == "processing":
                                item.status = "gateway_processing"
                                if not item.gateway_accepted_at:
                                    item.gateway_accepted_at = send_dispatched_at or utc_now()
                            elif q_status in ("queued", "scheduled"):
                                item.status = "gateway_queued"
                                if not item.gateway_accepted_at:
                                    item.gateway_accepted_at = send_dispatched_at or utc_now()

        # Handle Delivery Status
        if d_status:
            if d_status in ("failed", "delivery_unknown"):
                # If we get a failure but we already delivered it successfully before, we keep it as completed
                # This handles out-of-order webhooks where failure arrives after delivered.
                old_d_rank = DELIVERY_RANKS.get(item.gateway_delivery_status or "", 0)
                if old_d_rank >= DELIVERY_RANKS["delivered"]:
                    logger.warning(f"Ignored '{d_status}' status for item {item.id} because it was already '{item.gateway_delivery_status}'")
                else:
                    if d_status == "failed":
                        item.gateway_delivery_status = d_status
                        if item.status != "completed":
                            item.status = "failed"
                            item.gateway_failed_at = utc_now()
                            if error_message:
                                item.gateway_error_message = error_message
                    elif d_status == "delivery_unknown":
                        if item.status != "completed":
                            item.status = "delivery_unknown"
                            item.gateway_error_code = "MESSAGE_NOT_FOUND"
                            if error_message:
                                item.gateway_error_message = error_message
            else:
                old_d_rank = DELIVERY_RANKS.get(item.gateway_delivery_status or "", 0)
                new_d_rank = DELIVERY_RANKS.get(d_status, 0)
                if new_d_rank >= old_d_rank:
                    item.gateway_delivery_status = d_status
                    if d_status in ("delivered", "read", "played"):
                        item.status = "completed"
                        item.pending_since_at = None
                        if d_status == "delivered" and not item.gateway_delivered_at:
                            item.gateway_delivered_at = utc_now()
                        if d_status in ("read", "played") and not item.gateway_read_at:
                            item.gateway_read_at = utc_now()

        await self.db.flush()

        if defer_job_sync:
            return StatusSyncResult(job_id=item.job_id, notification_required=False)

        # Sync overall job status
        return await self.sync_job_status(item.job_id)

    async def sync_job_status(self, job_id: str) -> StatusSyncResult:
        """
        Aggregate item statuses to determine job status and handle failure notifications.
        """
        stmt = select(DownloadItem).where(DownloadItem.job_id == job_id)
        res = await self.db.execute(stmt)
        items = res.scalars().all()

        if not items:
            return StatusSyncResult(job_id=job_id, notification_required=False)

        total = len(items)
        completed_count = sum(1 for i in items if i.status == "completed")
        failed_count = sum(1 for i in items if i.status in ("failed", "cancelled"))
        sent_count = sum(1 for i in items if i.status in ("sent", "completed") or i.gateway_message_id)
        queued_count = sum(1 for i in items if i.status == "gateway_queued")
        processing_count = sum(1 for i in items if i.status == "gateway_processing")
        unknown_count = sum(1 for i in items if i.status == "delivery_unknown")
        unknown_pending_count = sum(1 for i in items if i.status == "delivery_unknown_pending")
        cancelled_count = sum(1 for i in items if i.status == "cancelled")

        job_stmt = select(DownloadJob).where(DownloadJob.id == job_id)
        job_res = await self.db.execute(job_stmt)
        job = job_res.scalar_one_or_none()

        if not job:
            return StatusSyncResult(job_id=job_id, notification_required=False)

        new_job_status = job.status
        error_code = None
        error_message = None

        if cancelled_count == total:
            new_job_status = "cancelled"
        elif unknown_count > 0:
            new_job_status = "delivery_unknown"
        elif unknown_pending_count > 0:
            new_job_status = "delivery_unknown_pending"
        elif completed_count == total:
            new_job_status = "completed"
        elif failed_count == total:
            new_job_status = "failed"
            error_code = "DELIVERY_FAILED"
            error_message = "Semua item media gagal dikirimkan oleh Gateway."
        elif failed_count > 0:
            new_job_status = "failed"
            error_code = "PARTIAL_FAILURE"
            error_message = f"Sebagian media gagal dikirim ({failed_count}/{total} item)."
        elif processing_count > 0:
            new_job_status = "gateway_processing"
        elif queued_count > 0:
            new_job_status = "gateway_queued"
        elif sum(1 for i in items if i.status == "sent") + completed_count == total:
            new_job_status = "sent"

        if (
            job.status != new_job_status
            or job.error_code != error_code
            or job.error_message != error_message
            or job.sent_count != sent_count
            or job.failed_count != failed_count
            or job.media_count != total
        ):
            job.media_count = total
            job.sent_count = sent_count
            job.failed_count = failed_count
            await self.queue_service.update_job_status(
                job_id,
                new_status=new_job_status,
                error_code=error_code,
                error_message=error_message,
            )
            return StatusSyncResult(
                job_id=job_id,
                notification_required=new_job_status == "failed" and not job.failure_notification_sent_at,
            )

        return StatusSyncResult(job_id=job_id, notification_required=False)


async def load_failure_notification_snapshot(
    session_maker: async_sessionmaker[AsyncSession],
    job_id: str,
    custom_message: str | None = None,
) -> FailureNotificationSnapshot | None:
    async with session_maker() as session:
        stmt = select(
            DownloadJob.id,
            DownloadJob.sender_number,
            DownloadJob.inbound_message_id,
            DownloadJob.failure_notification_sent_at,
        ).where(DownloadJob.id == job_id)
        result = await session.execute(stmt)
        row = result.one_or_none()
        if not row:
            return None
        loaded_job_id, sender_number, inbound_message_id, sent_at = row
        if sent_at or not sender_number:
            return None

    message = custom_message or "video gagal dikirim melalui whatsapp. silakan kirim ulang link beberapa saat lagi."
    return FailureNotificationSnapshot(
        job_id=loaded_job_id,
        sender_number=sender_number,
        inbound_message_id=inbound_message_id,
        message=message,
    )


async def dispatch_failure_notification(
    session_maker: async_sessionmaker[AsyncSession],
    job_id: str,
    custom_message: str | None = None,
    gateway: FarrosWAGatewayClient | None = None,
) -> bool:
    snapshot = await load_failure_notification_snapshot(session_maker, job_id, custom_message)
    if not snapshot:
        return False

    client = gateway or FarrosWAGatewayClient()
    try:
        await client.send_text(
            to=snapshot.sender_number,
            text=snapshot.message,
            external_reference=f"media-{snapshot.inbound_message_id}-fail",
            idempotency_key=f"media-{snapshot.inbound_message_id}-failure",
        )
    except Exception as e:
        logger.error(f"Could not send failure notification to {snapshot.sender_number}: {e}")
        return False

    async with session_maker() as session:
        queue_service = QueueService(session)
        await queue_service.mark_job_failure_notification_sent(snapshot.job_id)
        await session.commit()
    logger.info(f"Sent failure notification for job {snapshot.job_id}")
    return True


def normalize_gateway_message_data(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        return {}
    nested = payload.get("data")
    if isinstance(nested, dict):
        return nested
    return payload
