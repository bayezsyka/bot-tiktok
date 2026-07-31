import asyncio
import logging

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database.models import DownloadItem
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.delivery_service import GatewayDeliveryService

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

    async def reconcile_item_ids(self, item_ids: list[int]) -> None:
        if not item_ids:
            return

        async with self.session_maker() as session:
            stmt = (
                select(DownloadItem)
                .where(DownloadItem.id.in_(item_ids))
            )
            result = await session.execute(stmt)
            items = result.scalars().all()

            if not items:
                return

            delivery_service = GatewayDeliveryService(session)

            for item in items:
                try:
                    response = await self.gateway.get_message(item.gateway_message_id)  # type: ignore

                    if response.http_status == 404 or response.status == "not_found":
                        await delivery_service.process_outbound_status(
                            item=item,
                            d_status="delivery_unknown",
                            error_message="Message not found in Gateway (404)"
                        )
                    elif response.status == "ok" and response.data:
                        q_status = response.queue_status
                        d_status = response.delivery_status
                        error_code = response.data.get("error_code")
                        error_message = response.data.get("error_message") or response.data.get("error")
                        await delivery_service.process_outbound_status(
                            item=item,
                            d_status=d_status,
                            q_status=q_status,
                            error_message=error_message,
                            error_code=error_code
                        )
                except Exception as e:
                    logger.warning(f"Reconciler error for item {item.id}: {e}")

            await session.commit()

    async def _reconcile_batch(self, include_unknown: bool = False) -> None:
        async with self.session_maker() as session:
            # Find items that are queued/processing in gateway but not final

            status_list = ["gateway_queued", "gateway_processing", "sent"]
            if include_unknown:
                status_list.append("delivery_unknown")

            stmt = (
                select(DownloadItem)
                .where(
                    DownloadItem.gateway_message_id.isnot(None),
                    DownloadItem.status.in_(status_list),
                    or_(
                        DownloadItem.gateway_delivery_status.is_(None),
                        DownloadItem.gateway_delivery_status.notin_(["read", "played", "delivered"])
                    )
                )
                .order_by(DownloadItem.last_gateway_sync_at.asc().nullsfirst())
                .limit(self.batch_size)
            )

            result = await session.execute(stmt)
            items = result.scalars().all()

            if not items:
                return

            delivery_service = GatewayDeliveryService(session)

            for item in items:
                try:
                    response = await self.gateway.get_message(item.gateway_message_id)  # type: ignore

                    if response.http_status == 404 or response.status == "not_found":
                        await delivery_service.process_outbound_status(
                            item=item,
                            d_status="delivery_unknown",
                            error_message="Message not found in Gateway (404)"
                        )
                    elif response.status == "ok" and response.data:
                        q_status = response.queue_status
                        d_status = response.delivery_status
                        error_code = response.data.get("error_code")
                        error_message = response.data.get("error_message") or response.data.get("error")
                        await delivery_service.process_outbound_status(
                            item=item,
                            d_status=d_status,
                            q_status=q_status,
                            error_message=error_message,
                            error_code=error_code
                        )

                except Exception as e:
                    logger.warning(f"Reconciler error for item {item.id}: {e}")

            await session.commit()
