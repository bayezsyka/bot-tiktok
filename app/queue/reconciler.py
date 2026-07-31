import asyncio
import logging
import random
import time
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.database.models import DownloadItem, utc_now
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.delivery_service import GatewayDeliveryService
from app.gateway.exceptions import GatewayRateLimitError
from app.gateway.schemas import GatewayMessageResponse

logger = logging.getLogger(__name__)

# Backoff tiers: (staleness_seconds, next_poll_interval_seconds)
_BACKOFF_TIERS = [
    (3600, 900),   # > 60 min stale → poll every 15 min
    (900, 300),    # > 15 min stale → poll every 5 min
    (300, 120),    # > 5 min stale  → poll every 2 min
    (120, 60),     # > 2 min stale  → poll every 60s
    (0, 15),       # fresh          → poll every 15s
]

# Non-final statuses eligible for reconciliation
_RECONCILE_STATUSES = [
    "gateway_queued",
    "gateway_processing",
    "sent",
    "delivery_unknown_pending",
]

# Timeout for delivery_unknown_pending before promoting to delivery_unknown
_PENDING_TIMEOUT_SECONDS = 1800  # 30 minutes


class GatewayReconciler:
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.session_maker = session_maker
        self.gateway = FarrosWAGatewayClient()
        self.settings = get_settings()
        self.is_running = False
        self._rate_limit_until: float = 0.0  # monotonic timestamp

    def stop(self) -> None:
        self.is_running = False
        logger.info("GatewayReconciler stop signal received.")

    async def run(self) -> None:
        self.is_running = True
        logger.info("GatewayReconciler started.")

        # Startup jitter to avoid thundering herd
        jitter = random.uniform(0.5, 5.0)
        await asyncio.sleep(jitter)

        interval = max(self.settings.GATEWAY_RECONCILE_INTERVAL_SECONDS, 10)

        while self.is_running:
            try:
                await self._reconcile_batch()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in GatewayReconciler loop: {e}", exc_info=True)

            await asyncio.sleep(float(interval))

        logger.info("GatewayReconciler loop stopped.")

    async def reconcile_item_ids(self, item_ids: list[int]) -> None:
        """Reconcile specific items by ID.  Uses per-item short sessions."""
        if not item_ids:
            return

        # Phase 1: Read gateway_message_ids using a short session
        id_msg_pairs: list[tuple[int, str]] = []
        try:
            async with self.session_maker() as session:
                stmt = (
                    select(DownloadItem.id, DownloadItem.gateway_message_id)
                    .where(
                        DownloadItem.id.in_(item_ids),
                        DownloadItem.gateway_message_id.isnot(None),
                    )
                )
                result = await session.execute(stmt)
                id_msg_pairs = [(row[0], row[1]) for row in result.all()]
        except Exception as e:
            logger.error(f"Reconciler read phase failed: {e}")
            return

        if not id_msg_pairs:
            return

        # Phase 2 + 3: Fetch from gateway then write per-item
        for item_id, gateway_msg_id in id_msg_pairs:
            # Check rate limit cooldown
            if time.monotonic() < self._rate_limit_until:
                logger.info("Reconciler skipping remaining items due to rate limit cooldown.")
                break

            # Phase 2: Network call (no DB session)
            try:
                response = await self.gateway.get_message(gateway_msg_id)
            except GatewayRateLimitError as e:
                self._apply_cooldown(e.retry_after)
                logger.warning("Gateway 429 during reconcile_item_ids, cooldown applied.")
                break
            except Exception as e:
                logger.warning(f"Reconciler GET error for item {item_id}: {e}")
                continue

            # Phase 3: Write with a fresh session
            await self._apply_response_to_item(item_id, response)

    async def _reconcile_batch(self, include_unknown: bool = False) -> None:
        # Check global cooldown
        if time.monotonic() < self._rate_limit_until:
            return

        batch_size = max(self.settings.GATEWAY_RECONCILE_BATCH_SIZE, 1)
        max_age_hours = self.settings.GATEWAY_RECONCILE_MAX_AGE_HOURS

        # Phase 1: Read-only snapshot (short session)
        candidates: list[tuple[int, str, str, float, float | None]] = []
        try:
            async with self.session_maker() as session:
                now = utc_now()
                age_cutoff = now - timedelta(hours=max_age_hours)

                status_list = list(_RECONCILE_STATUSES)
                if include_unknown:
                    status_list.append("delivery_unknown")

                stmt = (
                    select(
                        DownloadItem.id,
                        DownloadItem.gateway_message_id,
                        DownloadItem.status,
                        DownloadItem.last_gateway_sync_at,
                        DownloadItem.created_at,
                        DownloadItem.gateway_accepted_at,
                        DownloadItem.pending_since_at,
                    )
                    .where(
                        DownloadItem.gateway_message_id.isnot(None),
                        DownloadItem.status.in_(status_list),
                        DownloadItem.created_at >= age_cutoff,
                    )
                    .order_by(DownloadItem.created_at.desc())
                    .limit(batch_size * 3)  # fetch extra to filter by backoff
                )

                result = await session.execute(stmt)
                rows = result.all()

                for row in rows:
                    item_id, gw_msg_id, item_status, last_sync, created, accepted, pending_since = row
                    if not gw_msg_id:
                        continue

                    item_age_reference = accepted or created or now
                    item_age_seconds = _seconds_between(now, item_age_reference)
                    pending_age_seconds = (
                        _seconds_between(now, pending_since or accepted or created)
                        if (pending_since or accepted or created)
                        else None
                    )

                    min_interval = _get_poll_interval(item_age_seconds)
                    pending_timed_out = (
                        item_status == "delivery_unknown_pending"
                        and pending_age_seconds is not None
                        and pending_age_seconds >= _PENDING_TIMEOUT_SECONDS
                    )
                    if last_sync is not None and not pending_timed_out:
                        since_last_sync = _seconds_between(now, last_sync)
                        if since_last_sync < min_interval:
                            continue  # Not yet due for re-poll

                    candidates.append((item_id, gw_msg_id, item_status, item_age_seconds, pending_age_seconds))

                    if len(candidates) >= batch_size:
                        break
        except Exception as e:
            logger.error(f"Reconciler read phase error: {e}")
            return

        if not candidates:
            return

        # Phase 2 + 3: Network calls then per-item writes
        for item_id, gw_msg_id, item_status, _item_age_seconds, pending_age_seconds in candidates:
            if time.monotonic() < self._rate_limit_until:
                logger.info("Reconciler batch aborted due to rate limit cooldown.")
                break

            # Check delivery_unknown_pending timeout
            if (
                item_status == "delivery_unknown_pending"
                and pending_age_seconds is not None
                and pending_age_seconds >= _PENDING_TIMEOUT_SECONDS
            ):
                await self._promote_to_delivery_unknown(item_id)
                continue

            # Phase 2: Network call (no DB session)
            try:
                response = await self.gateway.get_message(gw_msg_id)
            except GatewayRateLimitError as e:
                self._apply_cooldown(e.retry_after)
                logger.warning("Gateway 429 during reconcile batch, stopping batch and applying cooldown.")
                break
            except Exception as e:
                logger.warning(f"Reconciler GET error for item {item_id}: {e}")
                continue

            # Phase 3: Write with a fresh session
            await self._apply_response_to_item(item_id, response)

    async def _apply_response_to_item(self, item_id: int, response: "GatewayMessageResponse") -> None:
        """Open a fresh session, load the item, apply the gateway response, commit."""
        try:
            async with self.session_maker() as session:
                try:
                    stmt = select(DownloadItem).where(DownloadItem.id == item_id)
                    result = await session.execute(stmt)
                    item = result.scalar_one_or_none()

                    if not item:
                        return

                    delivery_service = GatewayDeliveryService(session)

                    if response.http_status == 404 or response.status == "not_found":
                        await delivery_service.process_outbound_status(
                            item=item,
                            d_status="delivery_unknown",
                            error_message="Message not found in Gateway (404)"
                        )
                    elif response.status == "ok" and response.data:
                        q_status = response.queue_status
                        d_status = response.delivery_status
                        error_code = response.data.get("error_code") or response.data.get("last_error_code")
                        error_message = (
                            response.data.get("error_message")
                            or response.data.get("last_error_message")
                            or response.data.get("error")
                        )
                        await delivery_service.process_outbound_status(
                            item=item,
                            d_status=d_status,
                            q_status=q_status,
                            error_message=error_message,
                            error_code=error_code
                        )

                    await session.commit()
                except Exception as e:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    logger.warning(f"Reconciler write error for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"Reconciler session error for item {item_id}: {e}")

    async def _promote_to_delivery_unknown(self, item_id: int) -> None:
        """Promote a delivery_unknown_pending item to delivery_unknown after timeout."""
        try:
            async with self.session_maker() as session:
                try:
                    stmt = select(DownloadItem).where(DownloadItem.id == item_id)
                    result = await session.execute(stmt)
                    item = result.scalar_one_or_none()
                    if item and item.status == "delivery_unknown_pending":
                        item.status = "delivery_unknown"
                        item.gateway_error_code = "PENDING_TIMEOUT"
                        item.gateway_error_message = (
                            "Konfirmasi WhatsApp tidak diterima setelah 30 menit."
                        )
                        item.last_gateway_sync_at = utc_now()
                        await session.flush()

                        delivery_service = GatewayDeliveryService(session)
                        await delivery_service.sync_job_status(item.job_id)

                    await session.commit()
                except Exception as e:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    logger.warning(f"Reconciler promote error for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"Reconciler session error promoting item {item_id}: {e}")

    def _apply_cooldown(self, retry_after: float | None) -> None:
        """Set a global cooldown timestamp after receiving a 429."""
        wait = retry_after if retry_after and retry_after > 0 else 60.0
        self._rate_limit_until = time.monotonic() + wait


def _get_poll_interval(staleness_seconds: float) -> float:
    """Return the minimum poll interval based on how stale an item is."""
    for threshold, interval in _BACKOFF_TIERS:
        if staleness_seconds > threshold:
            return float(interval)
    return 15.0


def _seconds_between(now: datetime, earlier: datetime) -> float:
    if earlier.tzinfo is None:
        return float((now.replace(tzinfo=None) - earlier).total_seconds())
    return float((now - earlier).total_seconds())
