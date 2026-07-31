import asyncio
import logging
import os

from typing import TypedDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.database.models import DownloadJob, utc_now
from app.database.repositories import JobRepository
from app.downloader.exceptions import (
    ContentNotSupportedError,
    DownloadError,
    DownloadSizeLimitExceededError,
    DownloadTimeoutError,
)
from app.downloader.service import DownloaderService
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.exceptions import GatewayError, GatewayResponseError
from app.media.cleanup import create_job_temp_dir, is_disk_space_sufficient, remove_job_temp_dir
from app.media.processor import MediaProcessor
from app.queue.service import QueueService

logger = logging.getLogger(__name__)

class ItemSnapshot(TypedDict):
    id: int
    status: str
    gateway_message_id: str | None
    local_filename: str | None
    media_type: str
    position: int


class QueueWorker:
    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.session_maker = session_maker
        self.settings = get_settings()
        self.is_running = False
        self.gateway = FarrosWAGatewayClient()

    async def get_queue_size(self) -> int:
        async with self.session_maker() as session:
            # count jobs with status queued
            from sqlalchemy import func, select
            stmt = select(func.count()).select_from(DownloadJob).where(DownloadJob.status == "queued")
            res = await session.execute(stmt)
            return int(res.scalar() or 0)

    def stop(self) -> None:
        self.is_running = False
        logger.info("QueueWorker stop signal received.")

    async def run(self) -> None:
        self.is_running = True
        logger.info("QueueWorker started. Processing ONE job at a time.")

        while self.is_running:
            try:
                # 1. Check disk space safety threshold (>= 1 GB)
                if not is_disk_space_sufficient(1024 * 1024 * 1024):
                    logger.error("Disk space critically low (< 1 GB free). Pausing queue worker for 30s.")
                    await asyncio.sleep(30.0)
                    continue

                # 2. Acquire oldest queued job
                job_id = await self._acquire_next_job()
                if not job_id:
                    await asyncio.sleep(2.0)
                    continue

                # 3. Process job
                await self._process_job_safely(job_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in worker loop: {e}", exc_info=True)
                await asyncio.sleep(5.0)

        logger.info("QueueWorker loop stopped.")

    async def _acquire_next_job(self) -> str | None:
        async with self.session_maker() as session:
            async with session.begin():
                repo = JobRepository(session)
                job = await repo.get_next_queued_job()
                if not job:
                    return None

                # Transition to extracting and increment attempts
                job.status = "extracting"
                job.attempt_count += 1
                if not job.started_at:
                    job.started_at = utc_now()
                job.updated_at = utc_now()
                await session.flush()
                return job.id

    async def _send_failure_notification(
        self, job_id: str, sender_number: str, inbound_message_id: str,
        custom_message: str | None = None
    ) -> None:
        """Send failure notification using a fresh session."""
        try:
            async with self.session_maker() as session:
                from app.gateway.delivery_service import GatewayDeliveryService
                delivery_service = GatewayDeliveryService(session)
                # Build a minimal job-like object for notification
                job_stmt = (
                    __import__("sqlalchemy", fromlist=["select"]).select(DownloadJob)
                    .where(DownloadJob.id == job_id)
                )
                result = await session.execute(job_stmt)
                job = result.scalar_one_or_none()
                if job:
                    await delivery_service.send_failure_notification(job, custom_message)
                    await session.commit()
        except Exception as e:
            logger.error(f"Failed to send failure notification for job {job_id}: {e}")

    async def _process_job_safely(self, job_id: str) -> None:
        job_dir = None
        try:
            job_dir = create_job_temp_dir(job_id)

            # ==========================================
            # PHASE 1: EXTRACTION (short session)
            # ==========================================
            attempt_count = 0
            sender_number = ""
            inbound_message_id = ""
            provider = None
            metadata = None

            try:
                async with self.session_maker() as session:
                    try:
                        job_repo = JobRepository(session)
                        downloader = DownloaderService(session)

                        job = await job_repo.get_by_id(job_id)
                        if not job:
                            return

                        attempt_count = job.attempt_count
                        sender_number = job.sender_number
                        inbound_message_id = job.inbound_message_id

                        provider, metadata = await downloader.extract_and_prepare_job(job, job_dir)
                        await session.commit()
                    except (ContentNotSupportedError, DownloadSizeLimitExceededError) as e:
                        logger.warning(f"[Stage: Extraction] Permanent error during extraction for job {job_id}: {e.message}")
                        try:
                            await session.rollback()
                        except Exception:
                            pass
                        await self._update_job_status_safe(
                            job_id, "failed",
                            error_code="UNSUPPORTED_CONTENT",
                            error_message=e.user_friendly_message
                        )
                        await self._send_failure_notification(
                            job_id, sender_number, inbound_message_id, e.user_friendly_message
                        )
                        return
                    except (DownloadTimeoutError, DownloadError, Exception) as e:
                        logger.error(f"[Stage: Extraction] Extraction error on job {job_id}: {e}")
                        try:
                            await session.rollback()
                        except Exception:
                            pass
                        user_msg = getattr(e, "user_friendly_message", None)
                        await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                        return
            except Exception as e:
                logger.error(f"[Stage: Extraction] Session error on job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

            # ==========================================
            # PHASE 2: DOWNLOADING (session for status + external process + session for results)
            # ==========================================
            try:
                await self._update_job_status_safe(job_id, "downloading")
            except Exception as e:
                logger.error(f"[Stage: Download] Status update failed for job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

            try:
                async with self.session_maker() as session:
                    try:
                        job_repo = JobRepository(session)
                        downloader = DownloaderService(session)
                        job = await job_repo.get_by_id(job_id)
                        if not job:
                            return
                        attempt_count = job.attempt_count

                        await downloader.download_job_content(job, provider, metadata, job_dir)
                        await session.commit()
                    except (ContentNotSupportedError, DownloadSizeLimitExceededError) as e:
                        logger.warning(f"[Stage: Download] Size exceeded on job {job_id}: {e.message}")
                        try:
                            await session.rollback()
                        except Exception:
                            pass
                        await self._update_job_status_safe(
                            job_id, "failed",
                            error_code="SIZE_EXCEEDED",
                            error_message=e.user_friendly_message
                        )
                        await self._send_failure_notification(
                            job_id, sender_number, inbound_message_id, e.user_friendly_message
                        )
                        return
                    except Exception as e:
                        logger.error(f"[Stage: Download] Download error on job {job_id}: {e}")
                        try:
                            await session.rollback()
                        except Exception:
                            pass
                        user_msg = getattr(e, "user_friendly_message", None)
                        await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                        return
            except Exception as e:
                logger.error(f"[Stage: Download] Session error on job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

            # ==========================================
            # VALIDATE FILES (short session)
            # ==========================================
            try:
                async with self.session_maker() as session:
                    job_repo = JobRepository(session)
                    job = await job_repo.get_by_id(job_id)
                    if not job:
                        return

                    items = list(job.items) if job.items else []
                    if not items or (job.media_count and len(items) != job.media_count):
                        logger.error(f"[Stage: Download] Job {job_id} item count mismatch before processing. Items: {len(items)}, expected: {job.media_count}")
                        queue_service = QueueService(session)
                        await queue_service.update_job_status(
                            job_id, "failed", error_code="INTERNAL_STATE_ERROR", error_message="Jumlah item unduhan tidak sesuai dengan metadata."
                        )
                        await session.commit()
                        await self._send_failure_notification(job_id, sender_number, inbound_message_id)
                        return

                    invalid_items = [
                        item for item in items
                        if item.status != "sent" and not item.gateway_message_id and (not item.local_filename or not os.path.exists(item.local_filename))
                    ]
                    if invalid_items:
                        logger.error(f"[Stage: Download] Job {job_id} has items without valid local_filename before processing.")
                        queue_service = QueueService(session)
                        await queue_service.update_job_status(
                            job_id, "failed", error_code="DOWNLOAD_FAILED", error_message="File media lokal tidak ditemukan atau rusak."
                        )
                        await session.commit()
                        await self._send_failure_notification(job_id, sender_number, inbound_message_id)
                        return
            except Exception as e:
                logger.error(f"[Stage: Validation] Error validating job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

            # ==========================================
            # PHASE 3: MEDIA PROCESSING (session for status + external process + session for results)
            # ==========================================
            try:
                await self._update_job_status_safe(job_id, "processing")
            except Exception as e:
                logger.error(f"[Stage: Processing] Status update failed for job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

            try:
                async with self.session_maker() as session:
                    try:
                        job_repo = JobRepository(session)
                        media_processor = MediaProcessor(session)
                        job = await job_repo.get_by_id(job_id)
                        if not job:
                            return

                        await media_processor.process_job_media(job, job_dir)
                        await session.commit()
                    except Exception as e:
                        logger.error(f"[Stage: Processing] Processing error on job {job_id}: {e}")
                        try:
                            await session.rollback()
                        except Exception:
                            pass
                        await self._handle_job_error(job_id, str(e), attempt_count)
                        return
            except Exception as e:
                logger.error(f"[Stage: Processing] Session error on job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

            # ==========================================
            # PHASE 4: SENDING (session for status + per-item sends)
            # ==========================================
            try:
                await self._update_job_status_safe(job_id, "sending")
            except Exception as e:
                logger.error(f"[Stage: Sending] Status update failed for job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

            await self._send_all_media_items(job_id)

        finally:
            if job_dir:
                remove_job_temp_dir(job_id)

    async def _update_job_status_safe(
        self, job_id: str, status: str,
        error_code: str | None = None, error_message: str | None = None
    ) -> None:
        """Update job status using a short-lived session."""
        async with self.session_maker() as session:
            try:
                queue_service = QueueService(session)
                await queue_service.update_job_status(job_id, status, error_code, error_message)
                await session.commit()
            except Exception:
                try:
                    await session.rollback()
                except Exception:
                    pass
                raise

    async def _send_all_media_items(self, job_id: str) -> None:
        """Send media items to gateway.  Uses a fresh session to load job snapshot,
        then sends each item individually with per-item commits."""

        # Load job snapshot
        async with self.session_maker() as session:
            job_repo = JobRepository(session)
            job = await job_repo.get_by_id(job_id)
            if not job:
                return

            items = list(job.items) if job.items else []
            total_items = len(items)
            platform = getattr(job, "platform", "tiktok") or "tiktok"
            sender_number = job.sender_number
            inbound_message_id = job.inbound_message_id

            if total_items == 0:
                logger.error(f"[Stage: Sending] Job {job_id} has total_items == 0.")
                queue_service = QueueService(session)
                await queue_service.update_job_status(
                    job_id, "failed", error_code="NO_MEDIA_ITEMS", error_message="Tidak ada item media untuk dikirim."
                )
                await session.commit()
                await self._send_failure_notification(job_id, sender_number, inbound_message_id)
                return

            # Build a snapshot of items to process
            item_snapshots: list[ItemSnapshot] = []
            for item in items:
                item_snapshots.append({
                    "id": item.id,
                    "status": item.status,
                    "gateway_message_id": item.gateway_message_id,
                    "local_filename": item.local_filename,
                    "media_type": item.media_type,
                    "position": item.position,
                })

        # Process each item
        sent_count = 0
        failed_count = 0

        for snap in item_snapshots:
            # Skip items already successfully sent or queued to gateway
            if snap["status"] in ("sent", "gateway_queued") or snap["gateway_message_id"]:
                sent_count += 1
                continue

            if not snap["local_filename"] or not os.path.exists(snap["local_filename"]):
                logger.error(f"[Stage: Sending] Item {snap['id']} of job {job_id} missing local_filename.")
                await self._update_item_status_safe(snap["id"], "failed", error_message="File media lokal tidak ditemukan atau rusak.")
                failed_count += 1
                continue

            if snap["status"] == "failed":
                failed_count += 1
                continue

            # Determine caption and idempotency key based on platform
            if platform == "instagram":
                caption = ""
                idemp_key = f"instagram-{inbound_message_id}-video"
            elif snap["media_type"] == "video":
                caption = ""
                idemp_key = f"tiktok-{inbound_message_id}-video"
            else:
                if snap["position"] == 1:
                    caption = f"foto tiktok berhasil diproses. total: {total_items} foto."
                else:
                    caption = ""
                idemp_key = f"tiktok-{inbound_message_id}-photo-{snap['position']:03d}"

            # Network call (no DB session open)
            try:
                response = await self.gateway.send_media(
                    to=sender_number,
                    media_type=snap["media_type"],
                    file_path=snap["local_filename"],
                    caption=caption,
                    external_reference=job_id,
                    idempotency_key=idemp_key,
                )
            except GatewayResponseError as e:
                logger.error(f"[Stage: Sending] GatewayResponseError sending item {snap['id']} for job {job_id}: status={e.status_code}, message={e.message}")
                await self._process_send_failure(snap["id"], f"Gateway error: {e.message}")
                failed_count += 1
                continue
            except GatewayError as e:
                logger.warning(f"[Stage: Sending] Network error sending item {snap['id']} for job {job_id}: {e}")
                await self._process_send_failure(snap["id"], str(e))
                failed_count += 1
                continue

            # Write result with fresh session
            try:
                async with self.session_maker() as session:
                    try:
                        from sqlalchemy import select

                        from app.database.models import DownloadItem
                        stmt2 = select(DownloadItem).where(DownloadItem.id == snap["id"])
                        result2 = await session.execute(stmt2)
                        item2 = result2.scalar_one_or_none()

                        if not item2:
                            failed_count += 1
                            continue

                        if not response.message_id:
                            logger.error(f"[Stage: Sending] Upload succeeded but gateway did not return a message ID for item {snap['id']} of job {job_id}")
                            from app.gateway.delivery_service import GatewayDeliveryService
                            delivery_service = GatewayDeliveryService(session)
                            await delivery_service.process_outbound_status(
                                item=item2,
                                q_status="failed",
                                error_code="GATEWAY_INVALID_RESPONSE",
                                error_message="Gateway menerima upload tetapi tidak memberikan message ID"
                            )
                            failed_count += 1
                            await session.commit()
                            continue

                        item2.gateway_message_id = response.message_id

                        from app.gateway.delivery_service import GatewayDeliveryService
                        delivery_service = GatewayDeliveryService(session)

                        q_status = response.queue_status or "queued"
                        await delivery_service.process_outbound_status(
                            item=item2,
                            d_status=response.delivery_status,
                            q_status=q_status
                        )
                        sent_count += 1
                        await session.commit()
                    except Exception as e:
                        try:
                            await session.rollback()
                        except Exception:
                            pass
                        logger.error(f"[Stage: Sending] DB error saving send result for item {snap['id']}: {e}")
                        failed_count += 1
            except Exception as e:
                logger.error(f"[Stage: Sending] Session error for item {snap['id']}: {e}")
                failed_count += 1

        # Final job status sync with fresh session
        try:
            async with self.session_maker() as session:
                try:
                    from sqlalchemy import select
                    from app.database.models import DownloadJob
                    stmt3 = select(DownloadJob).where(DownloadJob.id == job_id)
                    result3 = await session.execute(stmt3)
                    job = result3.scalar_one_or_none()
                    if job:
                        job.sent_count = sent_count
                        job.failed_count = failed_count

                        from app.gateway.delivery_service import GatewayDeliveryService
                        delivery_service = GatewayDeliveryService(session)
                        await delivery_service.sync_job_status(job_id)

                    await session.commit()
                except Exception as e:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    logger.error(f"[Stage: Sending] Final job sync failed for {job_id}: {e}")
        except Exception as e:
            logger.error(f"[Stage: Sending] Final session error for {job_id}: {e}")

    async def _process_send_failure(self, item_id: int, error_message: str) -> None:
        """Record a send failure for an item using a fresh session."""
        try:
            async with self.session_maker() as session:
                try:
                    from sqlalchemy import select

                    from app.database.models import DownloadItem
                    from app.gateway.delivery_service import GatewayDeliveryService
                    stmt = select(DownloadItem).where(DownloadItem.id == item_id)
                    result = await session.execute(stmt)
                    item = result.scalar_one_or_none()
                    if item:
                        delivery_service = GatewayDeliveryService(session)
                        await delivery_service.process_outbound_status(
                            item=item,
                            q_status="failed",
                            error_message=error_message
                        )
                    await session.commit()
                except Exception as e:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    logger.error(f"Failed to record send failure for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"Session error recording send failure for item {item_id}: {e}")

    async def _update_item_status_safe(
        self, item_id: int, status: str, error_message: str | None = None
    ) -> None:
        """Update item status using a short-lived session."""
        try:
            async with self.session_maker() as session:
                try:
                    queue_service = QueueService(session)
                    await queue_service.update_item_status(item_id, status=status, error_message=error_message)
                    await session.commit()
                except Exception:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    raise
        except Exception as e:
            logger.error(f"Failed to update item {item_id} status: {e}")

    async def _handle_job_error(
        self, job_id: str, error_msg: str, attempt_count: int, user_friendly_message: str | None = None
    ) -> None:
        """Handle job error using a FRESH session.  Never reuses a broken session."""
        try:
            async with self.session_maker() as recovery_session:
                try:
                    queue_service = QueueService(recovery_session)

                    # Load job to get sender info for failure notification
                    from sqlalchemy import select
                    stmt = select(DownloadJob).where(DownloadJob.id == job_id)
                    result = await recovery_session.execute(stmt)
                    job = result.scalar_one_or_none()

                    sender_number = job.sender_number if job else ""
                    inbound_message_id = job.inbound_message_id if job else ""

                    if attempt_count < self.settings.MAX_JOB_RETRIES:
                        # Requeue with backoff
                        backoff_sec = float(2 ** attempt_count * 5)
                        logger.info(f"Transient error on job {job_id}. Requeuing with {backoff_sec}s backoff. Error: {error_msg}")
                        await asyncio.sleep(backoff_sec)
                        await queue_service.update_job_status(
                            job_id, "queued", error_code="RETRY_SCHEDULED", error_message=error_msg[:300]
                        )
                    else:
                        logger.warning(f"Job {job_id} failed permanently after {attempt_count} attempts. Error: {error_msg}")
                        await queue_service.update_job_status(
                            job_id, "failed", error_code="MAX_RETRIES_EXCEEDED", error_message=error_msg[:300]
                        )

                    await recovery_session.commit()

                    # Send failure notification outside the transaction
                    if attempt_count >= self.settings.MAX_JOB_RETRIES and sender_number:
                        await self._send_failure_notification(
                            job_id, sender_number, inbound_message_id, user_friendly_message
                        )
                except Exception as inner_exc:
                    try:
                        await recovery_session.rollback()
                    except Exception:
                        pass
                    logger.error(f"Recovery handler failed for job {job_id}: {inner_exc}")
        except Exception as outer_exc:
            logger.error(f"Recovery session creation failed for job {job_id}: {outer_exc}")
