import asyncio
import logging
import os
from typing import TypedDict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database.models import DownloadItem, DownloadJob, utc_now
from app.database.repositories import JobRepository
from app.downloader.dtos import (
    DownloadedContentResult,
    ExtractedMetadataResult,
    ItemProcessingSnapshot,
    JobDownloadSnapshot,
    MissingLocalFile,
    ProcessedJobResult,
    ProcessingSnapshot,
)
from app.downloader.exceptions import (
    ContentNotSupportedError,
    DownloadError,
    DownloadSizeLimitExceededError,
    DownloadTimeoutError,
    TikTokChallengeError,
)
from app.downloader.service import DownloaderService
from app.gateway.client import FarrosWAGatewayClient
from app.gateway.delivery_service import dispatch_failure_notification
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

    async def _load_job_download_snapshot(self, job_id: str) -> tuple[JobDownloadSnapshot, int, str, str] | None:
        async with self.session_maker() as session:
            stmt = (
                select(DownloadJob)
                .options(selectinload(DownloadJob.items))
                .where(DownloadJob.id == job_id)
            )
            result = await session.execute(stmt)
            job = result.scalar_one_or_none()
            if not job:
                return None

            items = tuple(
                ItemProcessingSnapshot(
                    id=item.id,
                    position=item.position,
                    media_type=item.media_type,
                    status=item.status,
                    gateway_message_id=item.gateway_message_id,
                    local_filename=item.local_filename,
                    source_size_bytes=item.source_size_bytes,
                    final_size_bytes=item.final_size_bytes,
                    source_url=item.source_url,
                )
                for item in (job.items or [])
            )
            snapshot = JobDownloadSnapshot(
                id=job.id,
                original_url=job.original_url,
                canonical_url=job.canonical_url,
                platform=job.platform or "tiktok",
                items=items,
            )
            return snapshot, job.attempt_count, job.sender_number, job.inbound_message_id

    async def _save_extracted_metadata(
        self, job_id: str, result: ExtractedMetadataResult
    ) -> None:
        async with self.session_maker() as session:
            async with session.begin():
                stmt = (
                    select(DownloadJob)
                    .options(selectinload(DownloadJob.items))
                    .where(DownloadJob.id == job_id)
                )
                query_result = await session.execute(stmt)
                job = query_result.scalar_one_or_none()
                if not job:
                    return

                metadata = result.metadata
                job.canonical_url = result.canonical_url
                job.content_type = metadata.content_type
                job.media_count = len(metadata.items)
                job.duration_seconds = metadata.duration_seconds
                job.updated_at = utc_now()

                existing_items = {item.position: item for item in (job.items or [])}
                for item_meta in metadata.items:
                    db_item = existing_items.get(item_meta.position)
                    if db_item:
                        if db_item.status != "sent" and not db_item.gateway_message_id:
                            db_item.media_type = item_meta.media_type
                            db_item.source_url = item_meta.source_url
                            db_item.updated_at = utc_now()
                    else:
                        item = DownloadItem(
                            job_id=job.id,
                            position=item_meta.position,
                            media_type=item_meta.media_type,
                            status="pending",
                            source_url=item_meta.source_url,
                        )
                        session.add(item)

    async def _save_downloaded_results(
        self, job_id: str, result: DownloadedContentResult
    ) -> None:
        async with self.session_maker() as session:
            async with session.begin():
                stmt = (
                    select(DownloadJob)
                    .options(selectinload(DownloadJob.items))
                    .where(DownloadJob.id == job_id)
                )
                query_result = await session.execute(stmt)
                job = query_result.scalar_one_or_none()
                if not job:
                    return

                existing_items = {item.position: item for item in (job.items or [])}
                for item_result in result.items:
                    db_item = existing_items.get(item_result.position)
                    if not db_item:
                        db_item = DownloadItem(
                            job_id=job.id,
                            position=item_result.position,
                            media_type=item_result.media_type,
                            status="pending",
                            source_url=item_result.source_url,
                        )
                        session.add(db_item)
                        existing_items[item_result.position] = db_item

                    if db_item.status == "sent" or db_item.gateway_message_id:
                        continue

                    db_item.media_type = item_result.media_type
                    db_item.source_url = item_result.source_url
                    db_item.local_filename = item_result.local_filename
                    db_item.source_size_bytes = item_result.source_size_bytes
                    db_item.updated_at = utc_now()

                job.source_size_bytes = result.source_size_bytes
                job.updated_at = utc_now()

    async def _load_processing_snapshot(self, job_id: str) -> ProcessingSnapshot | None:
        async with self.session_maker() as session:
            stmt = (
                select(DownloadJob)
                .options(selectinload(DownloadJob.items))
                .where(DownloadJob.id == job_id)
            )
            result = await session.execute(stmt)
            job = result.scalar_one_or_none()
            if not job:
                return None

            item_snapshots = tuple(
                ItemProcessingSnapshot(
                    id=item.id,
                    position=item.position,
                    media_type=item.media_type,
                    status=item.status,
                    gateway_message_id=item.gateway_message_id,
                    local_filename=item.local_filename,
                    source_size_bytes=item.source_size_bytes,
                    final_size_bytes=item.final_size_bytes,
                    source_url=item.source_url,
                )
                for item in (job.items or [])
            )
            missing = tuple(
                MissingLocalFile(item_id=item.id, position=item.position)
                for item in item_snapshots
                if item.status != "sent"
                and not item.gateway_message_id
                and (not item.local_filename or not os.path.exists(item.local_filename))
            )
            return ProcessingSnapshot(
                job_id=job.id,
                media_count=job.media_count,
                items=item_snapshots,
                missing_local_files=missing,
            )

    async def _mark_items_processing(self, snapshot: ProcessingSnapshot) -> None:
        item_ids = [
            item.id
            for item in snapshot.items
            if item.status != "sent" and not item.gateway_message_id and item.local_filename
        ]
        if not item_ids:
            return

        async with self.session_maker() as session:
            async with session.begin():
                stmt = select(DownloadItem).where(DownloadItem.id.in_(item_ids))
                result = await session.execute(stmt)
                for item in result.scalars().all():
                    if item.status != "sent" and not item.gateway_message_id:
                        item.status = "processing"
                        item.updated_at = utc_now()

    async def _save_processed_results(
        self, job_id: str, result: ProcessedJobResult
    ) -> None:
        async with self.session_maker() as session:
            async with session.begin():
                job = await session.get(DownloadJob, job_id)
                if not job:
                    return

                result_by_id = {item.item_id: item for item in result.items}
                if result_by_id:
                    stmt = select(DownloadItem).where(DownloadItem.id.in_(result_by_id.keys()))
                    item_result = await session.execute(stmt)
                    for item in item_result.scalars().all():
                        processed = result_by_id[item.id]
                        if item.status == "sent" or item.gateway_message_id:
                            continue
                        item.status = processed.status
                        if processed.local_filename is not None:
                            item.local_filename = processed.local_filename
                        if processed.final_size_bytes is not None:
                            item.final_size_bytes = processed.final_size_bytes
                        if processed.error_message is not None:
                            item.error_message = processed.error_message
                        item.updated_at = utc_now()

                job.final_size_bytes = result.final_size_bytes
                job.updated_at = utc_now()

    async def _send_failure_notification(
        self, job_id: str, sender_number: str, inbound_message_id: str,
        custom_message: str | None = None
    ) -> None:
        """Send failure notification after all DB sessions are closed."""
        await dispatch_failure_notification(
            self.session_maker,
            job_id,
            custom_message=custom_message,
            gateway=self.gateway,
        )

    async def _process_job_safely(self, job_id: str) -> None:
        job_dir = None
        try:
            job_dir = create_job_temp_dir(job_id)
            attempt_count = 0
            sender_number = ""
            inbound_message_id = ""

            downloader = DownloaderService()

            extraction_snapshot = await self._load_job_download_snapshot(job_id)
            if not extraction_snapshot:
                return
            job_snapshot, attempt_count, sender_number, inbound_message_id = extraction_snapshot

            try:
                extracted = await downloader.extract_metadata(job_snapshot, job_dir)
                await self._save_extracted_metadata(job_id, extracted)
            except (ContentNotSupportedError, DownloadSizeLimitExceededError) as e:
                logger.warning(f"[Stage: Extraction] Permanent error during extraction for job {job_id}: {e.message}")
                await self._update_job_status_safe(
                    job_id, "failed",
                    error_code="UNSUPPORTED_CONTENT",
                    error_message=e.user_friendly_message,
                )
                await self._send_failure_notification(
                    job_id, sender_number, inbound_message_id, e.user_friendly_message
                )
                return
            except TikTokChallengeError as e:
                logger.warning(f"[Stage: Extraction] TikTok challenge detected for job {job_id}: {e.message}")
                await self._update_job_status_safe(
                    job_id, "failed",
                    error_code="TIKTOK_CHALLENGE_PAGE",
                    error_message=e.user_friendly_message,
                )
                await self._send_failure_notification(
                    job_id, sender_number, inbound_message_id, e.user_friendly_message
                )
                return
            except (DownloadTimeoutError, DownloadError, Exception) as e:
                logger.error(f"[Stage: Extraction] Extraction error on job {job_id}: {e}")
                user_msg = getattr(e, "user_friendly_message", None)
                await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                return

            try:
                await self._update_job_status_safe(job_id, "downloading")
                download_snapshot = await self._load_job_download_snapshot(job_id)
                if not download_snapshot:
                    return
                job_snapshot, attempt_count, sender_number, inbound_message_id = download_snapshot
                downloaded = await downloader.download_content(
                    job_snapshot,
                    extracted.provider,
                    extracted.metadata,
                    job_dir,
                )
                await self._save_downloaded_results(job_id, downloaded)
            except (ContentNotSupportedError, DownloadSizeLimitExceededError) as e:
                logger.warning(f"[Stage: Download] Size exceeded on job {job_id}: {e.message}")
                await self._update_job_status_safe(
                    job_id, "failed",
                    error_code="SIZE_EXCEEDED",
                    error_message=e.user_friendly_message,
                )
                await self._send_failure_notification(
                    job_id, sender_number, inbound_message_id, e.user_friendly_message
                )
                return
            except TikTokChallengeError as e:
                logger.warning(f"[Stage: Download] TikTok challenge detected for job {job_id}: {e.message}")
                await self._update_job_status_safe(
                    job_id, "failed",
                    error_code="TIKTOK_CHALLENGE_PAGE",
                    error_message=e.user_friendly_message,
                )
                await self._send_failure_notification(
                    job_id, sender_number, inbound_message_id, e.user_friendly_message
                )
                return
            except Exception as e:
                logger.error(f"[Stage: Download] Download error on job {job_id}: {e}")
                user_msg = getattr(e, "user_friendly_message", None)
                await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                return

            processing_snapshot = await self._load_processing_snapshot(job_id)
            if not processing_snapshot:
                return
            if not processing_snapshot.items or (
                processing_snapshot.media_count
                and len(processing_snapshot.items) != processing_snapshot.media_count
            ):
                logger.error(
                    f"[Stage: Download] Job {job_id} item count mismatch before processing. "
                    f"Items: {len(processing_snapshot.items)}, expected: {processing_snapshot.media_count}"
                )
                await self._update_job_status_safe(
                    job_id,
                    "failed",
                    error_code="INTERNAL_STATE_ERROR",
                    error_message="Jumlah item unduhan tidak sesuai dengan metadata.",
                )
                await self._send_failure_notification(job_id, sender_number, inbound_message_id)
                return

            if processing_snapshot.missing_local_files:
                logger.error(f"[Stage: Download] Job {job_id} has items without valid local_filename before processing.")
                await self._update_job_status_safe(
                    job_id,
                    "failed",
                    error_code="DOWNLOAD_FAILED",
                    error_message="File media lokal tidak ditemukan atau rusak.",
                )
                await self._send_failure_notification(job_id, sender_number, inbound_message_id)
                return

            try:
                await self._update_job_status_safe(job_id, "processing")
                await self._mark_items_processing(processing_snapshot)
                media_processor = MediaProcessor()
                processed = await media_processor.process_job_media(processing_snapshot.items, job_dir)
                await self._save_processed_results(job_id, processed)
            except Exception as e:
                logger.error(f"[Stage: Processing] Processing error on job {job_id}: {e}")
                await self._handle_job_error(job_id, str(e), attempt_count)
                return

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
            caption = ""
            if platform == "instagram":
                idemp_key = f"instagram-{inbound_message_id}-video"
            elif snap["media_type"] == "video":
                idemp_key = f"tiktok-{inbound_message_id}-video"
            else:
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
                await self._process_send_failure(snap["id"], f"Gateway error: {e.message}", defer_job_sync=True)
                failed_count += 1
                continue
            except GatewayError as e:
                logger.warning(f"[Stage: Sending] Network error sending item {snap['id']} for job {job_id}: {e}")
                await self._process_send_failure(snap["id"], str(e), defer_job_sync=True)
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
                                error_message="Gateway menerima upload tetapi tidak memberikan message ID",
                                defer_job_sync=True,
                            )
                            failed_count += 1
                            await session.commit()
                        else:
                            item2.gateway_message_id = response.message_id

                            from app.gateway.delivery_service import GatewayDeliveryService
                            delivery_service = GatewayDeliveryService(session)

                            q_status = response.queue_status or "queued"
                            await delivery_service.process_outbound_status(
                                item=item2,
                                d_status=response.delivery_status,
                                q_status=q_status,
                                defer_job_sync=True,
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

        # Final job status sync with fresh session (calculating real counters from database)
        notification_job_id: str | None = None
        notification_required = False
        try:
            async with self.session_maker() as session:
                try:
                    from sqlalchemy import select

                    from app.database.models import DownloadItem, DownloadJob
                    stmt_items = select(DownloadItem).where(DownloadItem.job_id == job_id)
                    res_items = await session.execute(stmt_items)
                    db_items = res_items.scalars().all()

                    stmt3 = select(DownloadJob).where(DownloadJob.id == job_id)
                    result3 = await session.execute(stmt3)
                    job = result3.scalar_one_or_none()
                    if job and db_items:
                        total_cnt = len(db_items)
                        s_cnt = sum(1 for i in db_items if i.status in ("sent", "completed") or i.gateway_message_id)
                        f_cnt = sum(1 for i in db_items if i.status in ("failed", "cancelled") and not i.gateway_message_id)

                        job.media_count = total_cnt
                        job.sent_count = s_cnt
                        job.failed_count = f_cnt

                        from app.gateway.delivery_service import GatewayDeliveryService
                        delivery_service = GatewayDeliveryService(session)
                        sync_result = await delivery_service.sync_job_status(job_id)
                        notification_job_id = sync_result.job_id
                        notification_required = sync_result.notification_required

                    await session.commit()
                except Exception as e:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    logger.error(f"[Stage: Sending] Final job sync failed for {job_id}: {e}")
        except Exception as e:
            logger.error(f"[Stage: Sending] Final session error for {job_id}: {e}")

        if notification_job_id and notification_required:
            await dispatch_failure_notification(
                self.session_maker,
                notification_job_id,
                gateway=self.gateway,
            )

    async def _process_send_failure(
        self, item_id: int, error_message: str, defer_job_sync: bool = False
    ) -> None:
        """Record a send failure for an item using a fresh session."""
        notification_job_id: str | None = None
        notification_required = False
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
                        sync_result = await delivery_service.process_outbound_status(
                            item=item,
                            q_status="failed",
                            error_message=error_message,
                            defer_job_sync=defer_job_sync,
                        )
                        notification_job_id = sync_result.job_id
                        notification_required = sync_result.notification_required
                    await session.commit()
                except Exception as e:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    logger.error(f"[Stage: Sending] DB error saving failure for item {item_id}: {e}")
        except Exception as e:
            logger.error(f"[Stage: Sending] Session error saving failure for item {item_id}: {e}")

        if not defer_job_sync and notification_job_id and notification_required:
            await dispatch_failure_notification(
                self.session_maker,
                notification_job_id,
                gateway=self.gateway,
            )

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
        """Handle job error without holding a DB session during retry backoff."""
        if attempt_count < self.settings.MAX_JOB_RETRIES:
            backoff_sec = float(2 ** attempt_count * 5)
            logger.info(f"Transient error on job {job_id}. Requeuing with {backoff_sec}s backoff. Error: {error_msg}")
            await asyncio.sleep(backoff_sec)

            try:
                async with self.session_maker() as recovery_session:
                    try:
                        stmt = select(DownloadJob).where(DownloadJob.id == job_id)
                        result = await recovery_session.execute(stmt)
                        job = result.scalar_one_or_none()
                        if not job:
                            return
                        if job.status in ("completed", "failed", "cancelled", "sent", "gateway_queued", "gateway_processing"):
                            logger.info(f"Skipping retry update for job {job_id}; current status is {job.status}.")
                            return

                        queue_service = QueueService(recovery_session)
                        await queue_service.update_job_status(
                            job_id, "queued", error_code="RETRY_SCHEDULED", error_message=error_msg[:300]
                        )
                        await recovery_session.commit()
                    except Exception as inner_exc:
                        try:
                            await recovery_session.rollback()
                        except Exception:
                            pass
                        logger.error(f"Recovery handler failed for job {job_id}: {inner_exc}")
            except Exception as outer_exc:
                logger.error(f"Recovery session creation failed for job {job_id}: {outer_exc}")
            return

        try:
            async with self.session_maker() as recovery_session:
                try:
                    queue_service = QueueService(recovery_session)

                    stmt = select(DownloadJob).where(DownloadJob.id == job_id)
                    result = await recovery_session.execute(stmt)
                    job = result.scalar_one_or_none()

                    sender_number = job.sender_number if job else ""
                    inbound_message_id = job.inbound_message_id if job else ""

                    logger.warning(f"Job {job_id} failed permanently after {attempt_count} attempts. Error: {error_msg}")
                    await queue_service.update_job_status(
                        job_id, "failed", error_code="MAX_RETRIES_EXCEEDED", error_message=error_msg[:300]
                    )

                    await recovery_session.commit()

                    if sender_number:
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
