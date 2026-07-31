import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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
    ) -> None:
        """
        Process outbound status idempotently and monotonically.
        """
        item.last_gateway_sync_at = utc_now()

        # Handle Queue Status
        if q_status:
            if q_status in ("failed", "cancelled"):
                item.gateway_queue_status = q_status
                if item.status != "completed":
                    item.status = q_status
            else:
                old_q_rank = QUEUE_RANKS.get(item.gateway_queue_status or "", 0)
                new_q_rank = QUEUE_RANKS.get(q_status, 0)
                if new_q_rank >= old_q_rank:
                    item.gateway_queue_status = q_status
                    if item.status not in ("completed", "failed", "cancelled"):
                        if q_status == "sent":
                            item.status = "sent"
                            if not item.gateway_sent_at:
                                item.gateway_sent_at = utc_now()
                        elif q_status == "processing":
                            item.status = "gateway_processing"
                        elif q_status in ("queued", "scheduled"):
                            item.status = "gateway_queued"

        # Handle Delivery Status
        if d_status:
            if d_status in ("failed", "delivery_unknown"):
                # If we get a failure but we already delivered it successfully before, we keep it as completed
                # This handles out-of-order webhooks where failure arrives after delivered.
                old_d_rank = DELIVERY_RANKS.get(item.gateway_delivery_status or "", 0)
                if old_d_rank >= DELIVERY_RANKS["delivered"]:
                    logger.warning(f"Ignored '{d_status}' status for item {item.id} because it was already '{item.gateway_delivery_status}'")
                else:
                    item.gateway_delivery_status = d_status
                    if item.status != "completed":
                        if d_status == "failed":
                            item.status = "failed"
                            item.gateway_failed_at = utc_now()
                            if error_message:
                                item.gateway_error_message = error_message
                        else:
                            item.status = "delivery_unknown"
            else:
                old_d_rank = DELIVERY_RANKS.get(item.gateway_delivery_status or "", 0)
                new_d_rank = DELIVERY_RANKS.get(d_status, 0)
                if new_d_rank >= old_d_rank:
                    item.gateway_delivery_status = d_status
                    if d_status in ("delivered", "read", "played"):
                        item.status = "completed"
                        if d_status == "delivered" and not item.gateway_delivered_at:
                            item.gateway_delivered_at = utc_now()
                        if d_status in ("read", "played") and not item.gateway_read_at:
                            item.gateway_read_at = utc_now()

        await self.db.flush()

        # Sync overall job status
        await self.sync_job_status(item.job_id)

    async def sync_job_status(self, job_id: str) -> None:
        """
        Aggregate item statuses to determine job status and handle failure notifications.
        """
        stmt = select(DownloadItem).where(DownloadItem.job_id == job_id)
        res = await self.db.execute(stmt)
        items = res.scalars().all()

        if not items:
            return

        total = len(items)
        completed_count = sum(1 for i in items if i.status == "completed")
        failed_count = sum(1 for i in items if i.status in ("failed", "cancelled"))
        sent_count = sum(1 for i in items if i.status == "sent")
        queued_count = sum(1 for i in items if i.status == "gateway_queued")
        processing_count = sum(1 for i in items if i.status == "gateway_processing")
        unknown_count = sum(1 for i in items if i.status == "delivery_unknown")
        cancelled_count = sum(1 for i in items if i.status == "cancelled")

        job_stmt = select(DownloadJob).where(DownloadJob.id == job_id)
        job_res = await self.db.execute(job_stmt)
        job = job_res.scalar_one_or_none()

        if not job:
            return

        new_job_status = job.status
        error_code = None
        error_message = None

        if cancelled_count == total:
            new_job_status = "cancelled"
        elif unknown_count > 0:
            new_job_status = "delivery_unknown"
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
        elif sent_count + completed_count == total:
            new_job_status = "sent"

        if job.status != new_job_status:
            await self.queue_service.update_job_status(
                job_id,
                new_status=new_job_status,
                error_code=error_code,
                error_message=error_message,
            )
            # Fetch updated job
            job_res = await self.db.execute(job_stmt)
            job = job_res.scalar_one_or_none()
            if job and new_job_status == "failed":
                await self.send_failure_notification(job)

    async def send_failure_notification(self, job: DownloadJob, custom_message: str | None = None) -> None:
        """
        Send a failure notification once per job.
        """
        if job.failure_notification_sent_at:
            return

        gateway = FarrosWAGatewayClient()
        try:
            fail_msg = custom_message or "video gagal dikirim melalui whatsapp. silakan kirim ulang link beberapa saat lagi."
            await gateway.send_text(
                to=job.sender_number,
                text=fail_msg,
                external_reference=f"media-{job.inbound_message_id}-fail",
                idempotency_key=f"media-{job.inbound_message_id}-failure",
            )
            await self.queue_service.mark_job_failure_notification_sent(job.id)
            logger.info(f"Sent failure notification for job {job.id}")
        except Exception as e:
            logger.error(f"Could not send failure notification to {job.sender_number}: {e}")
