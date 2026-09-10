import asyncio
import logging
import os
from dataclasses import replace
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
    DownloadSizeLimitExceededError,
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
                if not is_disk_space_sufficient(1024 * 1024 * 1024):
                    logger.error("Disk space critically low (< 1 GB free). Pausing queue worker for 30s.")
                    await asyncio.sleep(30.0)
                    continue

                job_id = await self._acquire_next_job()
                if not job_id:
                    await asyncio.sleep(2.0)
                    continue

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
                selected_mode=job.selected_mode,
                music_url=job.music_url,
                items=items,
            )
            return snapshot, job.attempt_count, job.sender_number, job.inbound_message_id

    async def _save_canonical_url(self, job_id: str, canonical_url: str) -> None:
        async with self.session_maker() as session:
            async with session.begin():
                stmt = select(DownloadJob).where(DownloadJob.id == job_id)
                result = await session.execute(stmt)
                job = result.scalar_one_or_none()
                if job:
                    job.canonical_url = canonical_url
                    job.updated_at = utc_now()

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
                if metadata.music_url:
                    job.music_url = metadata.music_url
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
                await session.flush()

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

                if len(result.items) == 1 and result.items[0].media_type == "video" and job.selected_mode == "video":
                    job.content_type = "video"
                    job.media_count = 1
                    for item in list(job.items or []):
                        if item.position != 1:
                            await session.delete(item)

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
                if len(result.items) == 1 and result.items[0].media_type == "video" and job.selected_mode == "video":
                    job.media_count = 1
                job.status = "downloading"
                job.updated_at = utc_now()
                await session.flush()

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
            missing = tuple(
                MissingLocalFile(item_id=item.id, position=item.position)
                for item in (job.items or [])
                if (not item.local_filename or not os.path.exists(item.local_filename))
                and item.status != "sent"
                and not item.gateway_message_id
            )
            return ProcessingSnapshot(
                job_id=job.id,
                media_count=job.media_count,
                items=items,
                missing_local_files=missing,
            )

    async def _mark_items_processing(self, snapshot: ProcessingSnapshot) -> None:
        async with self.session_maker() as session:
            async with session.begin():
                for item_snap in snapshot.items:
                    if item_snap.status != "sent" and not item_snap.gateway_message_id:
                        stmt = select(DownloadItem).where(DownloadItem.id == item_snap.id)
                        res = await session.execute(stmt)
                        item = res.scalar_one_or_none()
                        if item:
                            item.status = "processing"
                            item.updated_at = utc_now()

    async def _save_processed_results(
        self, job_id: str, result: ProcessedJobResult
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

                existing_items = {item.id: item for item in (job.items or [])}
                for item_res in result.items:
                    db_item = existing_items.get(item_res.item_id)
                    if db_item:
                        if db_item.status != "sent" and not db_item.gateway_message_id:
                            db_item.status = item_res.status
                            if item_res.local_filename:
                                db_item.local_filename = item_res.local_filename
                            if item_res.final_size_bytes is not None:
                                db_item.final_size_bytes = item_res.final_size_bytes
                            if item_res.error_message:
                                db_item.error_message = item_res.error_message
                            db_item.updated_at = utc_now()

                job.final_size_bytes = result.final_size_bytes
                job.status = "processing"
                job.updated_at = utc_now()
                await session.flush()

    async def _update_job_status_safe(
        self,
        job_id: str,
        status: str,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        try:
            async with self.session_maker() as session:
                try:
                    queue_service = QueueService(session)
                    await queue_service.update_job_status(
                        job_id,
                        status,
                        error_code=error_code,
                        error_message=error_message,
                    )
                    await session.commit()
                except Exception:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    raise
        except Exception as e:
            logger.error(f"Failed to update job {job_id} status to {status}: {e}")

    async def _update_item_status_safe(
        self, item_id: int, status: str, error_message: str | None = None
    ) -> None:
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

    async def _send_failure_notification(
        self,
        job_id: str,
        sender_number: str,
        inbound_message_id: str,
        user_friendly_message: str | None = None,
    ) -> None:
        try:
            fallback_text = (
                user_friendly_message
                or "Maaf, kami gagal memproses media dari tautan yang Anda kirimkan. "
                "Pastikan tautan bersifat publik dan dapat diakses, lalu silakan coba lagi beberapa saat lagi."
            )
            idempotency_key = f"media-{inbound_message_id}-failed"
            external_reference = f"media-{inbound_message_id}"

            await self.gateway.send_text(
                to=sender_number,
                text=fallback_text,
                external_reference=external_reference,
                idempotency_key=idempotency_key,
            )

            async with self.session_maker() as session:
                try:
                    stmt = select(DownloadJob).where(DownloadJob.id == job_id)
                    result = await session.execute(stmt)
                    job = result.scalar_one_or_none()
                    if job:
                        job.failure_notification_sent_at = utc_now()
                        await session.commit()
                except Exception as db_err:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    logger.error(f"Failed to update failure_notification_sent_at: {db_err}")

        except Exception as notify_err:
            logger.error(f"Failed to send failure notification to {sender_number}: {notify_err}")

    async def _process_job_safely(self, job_id: str) -> None:
        loaded = await self._load_job_download_snapshot(job_id)
        if not loaded:
            return

        job_snapshot, attempt_count, sender_number, inbound_message_id = loaded
        job_dir = create_job_temp_dir(job_id)
        downloader = DownloaderService()

        try:
            # 1. Canonical Resolution
            if not job_snapshot.canonical_url:
                try:
                    resolved_url = await downloader.resolve_canonical_url(job_snapshot)
                    await self._save_canonical_url(job_id, resolved_url)
                    job_snapshot = replace(job_snapshot, canonical_url=resolved_url)
                except Exception as e:
                    logger.error(f"[Stage: Canonical] Failed for job {job_id}: {e}")
                    user_msg = getattr(e, "user_friendly_message", None)
                    await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                    return

            # 2. Metadata Extraction
            try:
                extracted = await downloader.extract_metadata(job_snapshot, job_dir)
                await self._save_extracted_metadata(job_id, extracted)
            except TikTokChallengeError as e:
                logger.warning(f"[Stage: Extract] TikTok challenge detected for job {job_id}: {e.message}")
                await self._handle_job_error(
                    job_id,
                    e.message,
                    attempt_count,
                    e.user_friendly_message,
                    final_error_code="TIKTOK_CHALLENGE_PAGE",
                )
                return
            except ContentNotSupportedError as e:
                logger.warning(f"[Stage: Extract] Content not supported for job {job_id}: {e.message}")
                await self._update_job_status_safe(
                    job_id, "failed",
                    error_code="UNSUPPORTED_CONTENT",
                    error_message=e.user_friendly_message,
                )
                await self._send_failure_notification(
                    job_id, sender_number, inbound_message_id, e.user_friendly_message
                )
                return
            except Exception as e:
                logger.error(f"[Stage: Extract] Extraction failed for job {job_id}: {e}")
                user_msg = getattr(e, "user_friendly_message", None)
                await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                return

            loaded_after_extract = await self._load_job_download_snapshot(job_id)
            if not loaded_after_extract:
                return
            job_snapshot, attempt_count, sender_number, inbound_message_id = loaded_after_extract

            # 3. Download Stage
            try:
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
                await self._handle_job_error(
                    job_id,
                    e.message,
                    attempt_count,
                    e.user_friendly_message,
                    final_error_code="TIKTOK_CHALLENGE_PAGE",
                )
                return
            except Exception as e:
                logger.error(f"[Stage: Download] Download error on job {job_id}: {e}")
                user_msg = getattr(e, "user_friendly_message", None)
                await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                return

            # 4. Processing Stage
            processing_snapshot = await self._load_processing_snapshot(job_id)
            if not processing_snapshot:
                return
            if not processing_snapshot.items or (
                processing_snapshot.media_count
                and len(processing_snapshot.items) != processing_snapshot.media_count
            ):
                logger.error(f"[Stage: Processing] Items mismatch for job {job_id}.")
                await self._handle_job_error(
                    job_id,
                    "Incomplete items for processing",
                    attempt_count,
                    "Pengunduhan media belum lengkap.",
                )
                return

            if processing_snapshot.missing_local_files:
                logger.error(f"[Stage: Processing] Missing local files for job {job_id}.")
                await self._update_job_status_safe(
                    job_id,
                    "failed",
                    error_code="DOWNLOAD_FAILED",
                    error_message="File media tidak ditemukan di penyimpanan lokal.",
                )
                await self._send_failure_notification(
                    job_id, sender_number, inbound_message_id, "File media tidak ditemukan di penyimpanan lokal."
                )
                return

            await self._mark_items_processing(processing_snapshot)

            processor = MediaProcessor()
            try:
                processed_result = await processor.process_job_media(processing_snapshot.items, job_dir)
                await self._save_processed_results(job_id, processed_result)
            except Exception as e:
                logger.error(f"[Stage: Processing] Processing error on job {job_id}: {e}")
                user_msg = getattr(e, "user_friendly_message", None)
                await self._handle_job_error(job_id, str(e), attempt_count, user_msg)
                return

            # 5. Sending Stage
            await self._update_job_status_safe(job_id, "sending")
            await self._send_all_media_items(job_id)

        finally:
            remove_job_temp_dir(job_id)

    async def _send_all_media_items(self, job_id: str) -> None:
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
            canonical_url = job.canonical_url or job.original_url or ""

            if total_items == 0:
                logger.error(f"[Stage: Sending] Job {job_id} has total_items == 0.")
                queue_service = QueueService(session)
                await queue_service.update_job_status(
                    job_id, "failed", error_code="NO_MEDIA_ITEMS", error_message="Tidak ada item media untuk dikirim."
                )
                await session.commit()
                await self._send_failure_notification(job_id, sender_number, inbound_message_id)
                return

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

        sent_count = 0
        failed_count = 0

        for snap in item_snapshots:
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

            caption = ""
            if platform == "instagram":
                ig_path = canonical_url.lower()
                if "/p/" in ig_path:
                    idemp_key = f"instagram-{inbound_message_id}-{snap['media_type']}-{snap['position']:03d}"
                else:
                    idemp_key = f"instagram-{inbound_message_id}-video"
            elif snap["media_type"] == "video":
                idemp_key = f"tiktok-{inbound_message_id}-video"
            else:
                idemp_key = f"tiktok-{inbound_message_id}-photo-{snap['position']:03d}"

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

    async def _handle_job_error(
        self,
        job_id: str,
        error_msg: str,
        attempt_count: int,
        user_friendly_message: str | None = None,
        final_error_code: str = "MAX_RETRIES_EXCEEDED",
    ) -> None:
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
                        job_id, "failed", error_code=final_error_code, error_message=error_msg[:300]
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
