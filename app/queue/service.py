import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import DownloadItem, DownloadJob, utc_now

logger = logging.getLogger(__name__)


class QueueService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def update_job_status(
        self,
        job_id: str,
        new_status: str,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> DownloadJob | None:
        stmt = select(DownloadJob).where(DownloadJob.id == job_id)
        result = await self.session.execute(stmt)
        job = result.scalar_one_or_none()

        if not job:
            return None

        job.status = new_status
        job.updated_at = utc_now()

        if new_status == "extracting" and not job.started_at:
            job.started_at = utc_now()
        elif new_status in ("completed", "failed"):
            job.completed_at = utc_now()

        if error_code is not None:
            job.error_code = error_code
        if error_message is not None:
            job.error_message = error_message

        await self.session.flush()
        return job

    async def update_item_status(
        self,
        item_id: int,
        status: str,
        gateway_message_id: str | None = None,
        error_message: str | None = None,
    ) -> DownloadItem | None:
        stmt = select(DownloadItem).where(DownloadItem.id == item_id)
        result = await self.session.execute(stmt)
        item = result.scalar_one_or_none()

        if not item:
            return None

        item.status = status
        item.updated_at = utc_now()
        if gateway_message_id:
            item.gateway_message_id = gateway_message_id
        if error_message:
            item.error_message = error_message

        await self.session.flush()
        return item
