import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database.models import DownloadItem, utc_now
from app.gateway.client import FarrosWAGatewayClient
from app.queue.service import QueueService

logger = logging.getLogger(__name__)


class GatewayReconciler:
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.session_maker = session_maker
        self.gateway = FarrosWAGatewayClient()
        self.is_running = False
        self.batch_size = 20

    def stop(self) -> None:
        self.is_running = False
        logger.info("GatewayReconciler stop signal received.")

    async def run(self) -> None:
        self.is_running = True
        logger.info("GatewayReconciler started.")

        while self.is_running:
            try:
                await self._reconcile_batch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in GatewayReconciler loop: {e}", exc_info=True)

            # Polling interval
            await asyncio.sleep(15.0)

        logger.info("GatewayReconciler loop stopped.")

    async def _reconcile_batch(self) -> None:
        async with self.session_maker() as session:
            # Find items that are queued/processing in gateway but not final
            stmt = (
                select(DownloadItem)
                .where(
                    DownloadItem.gateway_message_id.isnot(None),
                    DownloadItem.status.in_(["gateway_queued", "gateway_processing", "sent"]),
                    DownloadItem.gateway_delivery_status.notin_(["read", "played", "delivered", "failed", "delivery_unknown"])
                )
                .order_by(DownloadItem.last_gateway_sync_at.asc().nullsfirst())
                .limit(self.batch_size)
            )

            result = await session.execute(stmt)
            items = result.scalars().all()

            if not items:
                return

            for item in items:
                try:
                    response = await self.gateway.get_message(item.gateway_message_id) # type: ignore

                    if response.status == "ok" and response.data:
                        q_status = response.queue_status
                        d_status = response.delivery_status

                        # Determine overall item status based on queue and delivery
                        new_status = item.status
                        if d_status in ("delivered", "read", "played", "delivery_unknown"):
                            new_status = "completed"
                        elif d_status == "failed" or q_status == "failed":
                            new_status = "failed"
                        elif d_status == "sent" or q_status == "sent":
                            new_status = "sent"
                        elif q_status == "processing":
                            new_status = "gateway_processing"
                        elif q_status == "queued":
                            new_status = "gateway_queued"

                        # Keep it progressing forward
                        if new_status == "completed" or (new_status == "sent" and item.status != "completed"):
                            item.status = new_status
                        elif new_status == "failed" and item.status != "completed":
                            item.status = "failed"
                        elif new_status in ("gateway_processing", "gateway_queued") and item.status not in ("completed", "sent", "failed"):
                            item.status = new_status

                        item.gateway_queue_status = q_status
                        item.gateway_delivery_status = d_status

                        # Timestamps
                        if d_status == "delivered" and not item.gateway_delivered_at:
                            item.gateway_delivered_at = utc_now()
                        if d_status in ("read", "played") and not item.gateway_read_at:
                            item.gateway_read_at = utc_now()
                        if q_status == "sent" and not item.gateway_sent_at:
                            item.gateway_sent_at = utc_now()

                    item.last_gateway_sync_at = utc_now()
                    await session.flush()

                    # Update job status based on items
                    await self._sync_job_status(session, item.job_id)

                except Exception as e:
                    logger.warning(f"Reconciler error for item {item.id}: {e}")

            await session.commit()

    async def _sync_job_status(self, session: AsyncSession, job_id: str) -> None:
        queue_service = QueueService(session)

        stmt = select(DownloadItem).where(DownloadItem.job_id == job_id)
        res = await session.execute(stmt)
        items = res.scalars().all()

        if not items:
            return

        total = len(items)
        completed_count = sum(1 for i in items if i.status == "completed")
        failed_count = sum(1 for i in items if i.status == "failed")
        sent_count = sum(1 for i in items if i.status == "sent")

        if completed_count == total:
            await queue_service.update_job_status(job_id, "completed")
        elif failed_count == total:
            await queue_service.update_job_status(job_id, "failed", error_code="DELIVERY_FAILED", error_message="Semua item gagal terkirim oleh gateway.")
        elif completed_count + failed_count == total:
            await queue_service.update_job_status(job_id, "completed") # Partial completion
        elif sent_count > 0 or completed_count > 0:
            await queue_service.update_job_status(job_id, "sent")
