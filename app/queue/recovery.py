import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database.models import DownloadJob, utc_now

logger = logging.getLogger(__name__)


async def recover_incomplete_jobs(session: AsyncSession) -> int:
    """
    Recover jobs that were in progress (extracting, downloading, processing, sending)
    when the application restarted or crashed.
    If retry is still available, transition back to 'queued'. Otherwise mark as 'failed'.
    Returns count of recovered jobs.
    """
    settings = get_settings()
    active_statuses = ("extracting", "downloading", "processing", "sending")

    stmt = select(DownloadJob).where(DownloadJob.status.in_(active_statuses))
    result = await session.execute(stmt)
    incomplete_jobs: list[DownloadJob] = list(result.scalars().all())

    recovered_count = 0
    for job in incomplete_jobs:
        if job.attempt_count < settings.MAX_JOB_RETRIES:
            logger.info(f"Recovering job {job.id} from status {job.status} -> returning to queued.")
            job.status = "queued"
            job.updated_at = utc_now()
            recovered_count += 1
        else:
            logger.warning(f"Job {job.id} stuck in {job.status} and exceeded retries -> marking failed.")
            job.status = "failed"
            job.error_code = "RECOVERY_FAILED"
            job.error_message = "Proses terputus saat restart aplikasi dan batas retry habis."
            job.completed_at = utc_now()
            job.updated_at = utc_now()

    if incomplete_jobs:
        await session.flush()
        logger.info(f"Recovery process evaluated {len(incomplete_jobs)} jobs, requeued {recovered_count}.")

    return recovered_count
