from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database.models import AllowedNumber
from app.database.repositories import AllowedNumberRepository, JobRepository
from app.media.cleanup import check_disk_space
from app.security.urls import normalize_phone_number


class AdminService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.number_repo = AllowedNumberRepository(session)
        self.job_repo = JobRepository(session)
        self.settings = get_settings()

    async def get_dashboard_data(self) -> dict[str, Any]:
        stats = await self.job_repo.get_dashboard_stats()
        active_numbers = await self.number_repo.count_active()
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

        return {
            "stats": stats,
            "active_numbers": active_numbers,
            "recent_jobs": recent_jobs,
            "temp_disk_used_bytes": used_bytes,
            "disk_free_bytes": check_disk_space(),
        }

    async def add_allowed_number(self, name: str, raw_phone: str, notes: str | None = None) -> tuple[AllowedNumber | None, str | None]:
        norm_phone = normalize_phone_number(raw_phone)
        if not norm_phone:
            return None, "Format nomor telepon tidak valid. Gunakan format 628xxx (10-15 digit)."

        existing = await self.number_repo.get_by_phone(norm_phone)
        if existing:
            return None, f"Nomor telepon {norm_phone} sudah terdaftar dalam whitelist."

        number = await self.number_repo.create_number(name=name.strip(), phone_number=norm_phone, notes=notes)
        await self.session.commit()
        return number, None

    async def edit_allowed_number(self, number_id: int, name: str, raw_phone: str, notes: str | None = None) -> tuple[AllowedNumber | None, str | None]:
        norm_phone = normalize_phone_number(raw_phone)
        if not norm_phone:
            return None, "Format nomor telepon tidak valid."

        existing = await self.number_repo.get_by_phone(norm_phone)
        if existing and existing.id != number_id:
            return None, f"Nomor telepon {norm_phone} sudah digunakan oleh entri lain."

        number = await self.number_repo.update_number(number_id, name.strip(), norm_phone, notes)
        await self.session.commit()
        return number, None

    async def toggle_number_status(self, number_id: int) -> AllowedNumber | None:
        num = await self.number_repo.toggle_active(number_id)
        await self.session.commit()
        return num

    async def delete_number(self, number_id: int) -> bool:
        res = await self.number_repo.delete_number(number_id)
        await self.session.commit()
        return res

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
        # Allow retry by ensuring attempt count allows worker to pick up
        if job.attempt_count >= self.settings.MAX_JOB_RETRIES:
            job.attempt_count = 0

        await self.session.commit()
        return True, "Job berhasil dimasukkan kembali ke antrean."
