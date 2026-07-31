from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.admin.allowed_number_service import AllowedNumberService
from app.config import get_settings
from app.database.models import AllowedNumber
from app.database.repositories import AllowedNumberRepository, JobRepository, UnmappedLidRepository
from app.media.cleanup import check_disk_space


class AdminService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.number_repo = AllowedNumberRepository(session)
        self.unmapped_repo = UnmappedLidRepository(session)
        self.job_repo = JobRepository(session)
        self.number_service = AllowedNumberService(session)
        self.settings = get_settings()

    async def get_dashboard_data(self) -> dict[str, Any]:
        stats = await self.job_repo.get_dashboard_stats()
        active_numbers = await self.number_repo.count_active()
        unmapped_count = await self.unmapped_repo.count_unresolved()
        recent_jobs = await self.job_repo.list_recent_jobs(limit=10)

        # Calculate temp disk usage
        temp_dir = Path(self.settings.TEMP_DIR)
        temp_dir.mkdir(parents=True, exist_ok=True)
        used_bytes = 0
        try:
            for f in temp_dir.rglob("*"):
                if f.is_file():
                    used_bytes += f.stat().st_size
        except Exception:
            pass

        # Fetch Gateway Session Status
        try:
            from app.gateway.client import FarrosWAGatewayClient
            gateway = FarrosWAGatewayClient()
            session_info = await gateway.get_default_session()

            raw_status = session_info.get("status", "unknown").lower()
            is_connected = session_info.get("connected", False)

            if is_connected or raw_status == "connected":
                session_status = "connected"
            elif raw_status in ("connecting", "reconnecting"):
                session_status = "connecting"
            elif raw_status in ("disconnected", "logged_out", "stopped"):
                session_status = "disconnected"
            else:
                session_status = "unavailable"
        except Exception:
            session_status = "unavailable"

        return {
            "stats": stats,
            "active_numbers": active_numbers,
            "unmapped_count": unmapped_count,
            "recent_jobs": recent_jobs,
            "temp_disk_used_bytes": used_bytes,
            "disk_free_bytes": check_disk_space(),
            "gateway_session_status": session_status,
        }

    async def add_allowed_number(
        self, name: str, raw_phone: str, raw_lid: str | None = None, notes: str | None = None
    ) -> tuple[AllowedNumber | None, str | None]:
        return await self.number_service.add_number(name=name, raw_phone=raw_phone, raw_lid=raw_lid, notes=notes)

    async def edit_allowed_number(
        self, number_id: int, name: str, raw_phone: str, raw_lid: str | None = None, notes: str | None = None
    ) -> tuple[AllowedNumber | None, str | None]:
        return await self.number_service.update_number(
            number_id=number_id, name=name, raw_phone=raw_phone, raw_lid=raw_lid, notes=notes
        )

    async def toggle_number_status(self, number_id: int) -> AllowedNumber | None:
        return await self.number_service.toggle_active(number_id)

    async def delete_number(self, number_id: int) -> bool:
        return await self.number_service.delete_number(number_id)

    async def retry_failed_job(self, job_id: str) -> tuple[bool, str]:
        job = await self.job_repo.get_by_id(job_id)
        if not job:
            return False, "Job tidak ditemukan."
        if job.status != "failed":
            return False, "Hanya job dengan status failed yang dapat di-retry."

        # Reset failed items back to pending, preserve items already sent
        for item in job.items:
            if item.status == "failed":
                item.status = "pending"
                item.error_message = None

        job.status = "queued"
        job.error_code = None
        job.error_message = None
        if job.attempt_count >= self.settings.MAX_JOB_RETRIES:
            job.attempt_count = 0

        await self.session.commit()
        return True, "Job berhasil dimasukkan kembali ke antrean."
