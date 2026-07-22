import asyncio
import logging
import os

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

    async def _send_failure_notification(self, sender_number: str, inbound_id: str) -> None:
        try:
            fail_msg = "konten tiktok tidak dapat diproses. pastikan link masih aktif, bersifat publik, dan dapat dibuka."
            await self.gateway.send_text(
                to=sender_number,
                text=fail_msg,
                external_reference=f"tiktok-{inbound_id}-fail",
                idempotency_key=f"tiktok-{inbound_id}-failure",
            )
        except Exception as e:
            logger.error(f"Could not send failure notification to {sender_number}: {e}")

    async def _process_job_safely(self, job_id: str) -> None:
        job_dir = None
        try:
            job_dir = create_job_temp_dir(job_id)
            async with self.session_maker() as session:
                job_repo = JobRepository(session)
                queue_service = QueueService(session)
                downloader = DownloaderService(session)
                media_processor = MediaProcessor(session)

                job = await job_repo.get_by_id(job_id)
                if not job:
                    return

                # STEP 1: Extracting metadata & setting up items
                try:
                    provider, metadata = await downloader.extract_and_prepare_job(job, job_dir)
                    await session.commit()
                except (ContentNotSupportedError, DownloadSizeLimitExceededError) as e:
                    logger.warning(f"[Stage: Extraction] Permanent error during extraction for job {job_id}: {e.message}")
                    await queue_service.update_job_status(
                        job_id, "failed", error_code="UNSUPPORTED_CONTENT", error_message=e.user_friendly_message
                    )
                    await session.commit()
                    await self._send_failure_notification(job.sender_number, job.inbound_message_id)
                    return
                except (DownloadTimeoutError, DownloadError, Exception) as e:
                    logger.error(f"[Stage: Extraction] Extraction error on job {job_id}: {e}")
                    await self._handle_job_error(job, str(e), session)
                    return

                # STEP 2: Downloading content
                await queue_service.update_job_status(job_id, "downloading")
                await session.commit()

                # refresh job inside new tx right before download_job_content
                job = await job_repo.get_by_id(job_id)
                if not job:
                    return
                try:
                    await downloader.download_job_content(job, provider, metadata, job_dir)
                    await session.commit()
                except (ContentNotSupportedError, DownloadSizeLimitExceededError) as e:
                    logger.warning(f"[Stage: Download] Size exceeded on job {job_id}: {e.message}")
                    await queue_service.update_job_status(
                        job_id, "failed", error_code="SIZE_EXCEEDED", error_message=e.user_friendly_message
                    )
                    await session.commit()
                    await self._send_failure_notification(job.sender_number, job.inbound_message_id)
                    return
                except Exception as e:
                    logger.error(f"[Stage: Download] Download error on job {job_id}: {e}")
                    await self._handle_job_error(job, str(e), session)
                    return

                # Refresh job right before pre-processing validation
                job = await job_repo.get_by_id(job_id)
                if not job:
                    return

                # VALIDATE FILES BEFORE PROCESSING
                items = list(job.items) if job.items else []
                if not items or (job.media_count and len(items) != job.media_count):
                    logger.error(f"[Stage: Download] Job {job_id} item count mismatch before processing. Items: {len(items)}, expected: {job.media_count}")
                    await queue_service.update_job_status(
                        job_id, "failed", error_code="INTERNAL_STATE_ERROR", error_message="Jumlah item unduhan tidak sesuai dengan metadata."
                    )
                    await session.commit()
                    await self._send_failure_notification(job.sender_number, job.inbound_message_id)
                    return

                invalid_items = [
                    item for item in items
                    if item.status != "sent" and not item.gateway_message_id and (not item.local_filename or not os.path.exists(item.local_filename))
                ]
                if invalid_items:
                    logger.error(f"[Stage: Download] Job {job_id} has items without valid local_filename before processing.")
                    await queue_service.update_job_status(
                        job_id, "failed", error_code="DOWNLOAD_FAILED", error_message="File media lokal tidak ditemukan atau rusak."
                    )
                    await session.commit()
                    await self._send_failure_notification(job.sender_number, job.inbound_message_id)
                    return

                # STEP 3: Processing media
                await queue_service.update_job_status(job_id, "processing")
                await session.commit()

                # Refresh job right before MediaProcessor.process_job_media
                job = await job_repo.get_by_id(job_id)
                if not job:
                    return
                try:
                    await media_processor.process_job_media(job, job_dir)
                    await session.commit()
                except Exception as e:
                    logger.error(f"[Stage: Processing] Processing error on job {job_id}: {e}")
                    await self._handle_job_error(job, str(e), session)
                    return

                # STEP 4: Sending media
                await queue_service.update_job_status(job_id, "sending")
                await session.commit()

                # Refresh job right before _send_all_media_items
                job = await job_repo.get_by_id(job_id)
                if not job:
                    return

                await self._send_all_media_items(job, session, queue_service)

        finally:
            if job_dir:
                remove_job_temp_dir(job_id)

    async def _send_all_media_items(
        self, job: DownloadJob, session: AsyncSession, queue_service: QueueService
    ) -> None:
        items = list(job.items) if job.items else []
        total_items = len(items)

        if total_items == 0:
            logger.error(f"[Stage: Sending] Job {job.id} has total_items == 0.")
            await queue_service.update_job_status(
                job.id, "failed", error_code="NO_MEDIA_ITEMS", error_message="Tidak ada item media untuk dikirim."
            )
            await self._send_failure_notification(job.sender_number, job.inbound_message_id)
            await session.commit()
            return

        sent_count = 0
        failed_count = 0

        for item in items:
            # Skip items already successfully sent
            if item.status == "sent" or item.gateway_message_id:
                sent_count += 1
                continue

            if not item.local_filename or not os.path.exists(item.local_filename):
                logger.error(f"[Stage: Sending] Item {item.id} of job {job.id} missing local_filename.")
                await queue_service.update_item_status(
                    item.id, status="failed", error_message="File media lokal tidak ditemukan atau rusak."
                )
                failed_count += 1
                continue

            if item.status == "failed":
                failed_count += 1
                continue

            # Determine caption and idempotency key
            if item.media_type == "video":
                caption = "video tiktok berhasil diproses."
                idemp_key = f"tiktok-{job.inbound_message_id}-video"
            else:
                if item.position == 1:
                    caption = f"foto tiktok berhasil diproses. total: {total_items} foto."
                else:
                    caption = ""
                idemp_key = f"tiktok-{job.inbound_message_id}-photo-{item.position:03d}"

            try:
                response = await self.gateway.send_media(
                    to=job.sender_number,
                    media_type=item.media_type,
                    file_path=item.local_filename,
                    caption=caption,
                    external_reference=job.id,
                    idempotency_key=idemp_key,
                )
                await queue_service.update_item_status(
                    item.id, status="sent", gateway_message_id=response.message_id
                )
                sent_count += 1
                await session.commit()
            except GatewayResponseError as e:
                # 4xx or permanent gateway response
                logger.error(f"[Stage: Sending] GatewayResponseError sending item {item.id} for job {job.id}: status={e.status_code}, message={e.message}")
                await queue_service.update_item_status(
                    item.id, status="failed", error_message=f"Gateway error: {e.message}"
                )
                failed_count += 1
                await session.commit()
            except GatewayError as e:
                # Network or timeout error when sending item
                logger.warning(f"[Stage: Sending] Network error sending item {item.id} for job {job.id}: {e}")
                await queue_service.update_item_status(item.id, status="failed", error_message=str(e))
                failed_count += 1
                await session.commit()

        # Final check
        job.sent_count = sent_count
        job.failed_count = failed_count

        if sent_count == total_items and total_items > 0:
            await queue_service.update_job_status(job.id, "completed")
        elif sent_count > 0:
            # Partial completion for slideshow or some failed
            await queue_service.update_job_status(
                job.id, "completed" if failed_count == 0 else "failed",
                error_code="PARTIAL_FAILURE" if failed_count > 0 else None,
                error_message=f"Terkirim {sent_count}/{total_items} item media." if failed_count > 0 else None,
            )
        else:
            await queue_service.update_job_status(
                job.id, "failed", error_code="SEND_FAILED", error_message="Gagal mengirim semua item media ke gateway."
            )
            await self._send_failure_notification(job.sender_number, job.inbound_message_id)

        await session.commit()


    async def _handle_job_error(self, job: DownloadJob, error_msg: str, session: AsyncSession) -> None:
        queue_service = QueueService(session)
        if job.attempt_count < self.settings.MAX_JOB_RETRIES:
            # Requeue with backoff
            backoff_sec = float(2 ** job.attempt_count * 5)
            logger.info(f"Transient error on job {job.id}. Requeuing with {backoff_sec}s backoff. Error: {error_msg}")
            await asyncio.sleep(backoff_sec)
            await queue_service.update_job_status(
                job.id, "queued", error_code="RETRY_SCHEDULED", error_message=error_msg[:300]
            )
        else:
            logger.warning(f"Job {job.id} failed permanently after {job.attempt_count} attempts. Error: {error_msg}")
            await queue_service.update_job_status(
                job.id, "failed", error_code="MAX_RETRIES_EXCEEDED", error_message=error_msg[:300]
            )
            await self._send_failure_notification(job.sender_number, job.inbound_message_id)
        await session.commit()
