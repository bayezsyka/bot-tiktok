from collections.abc import Sequence

from sqlalchemy import desc, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database.models import (
    Admin,
    AllowedNumber,
    DownloadItem,
    DownloadJob,
    UnmappedLid,
    WebhookEvent,
    utc_now,
)


class AdminRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_username(self, username: str) -> Admin | None:
        stmt = select(Admin).where(Admin.username == username)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, admin_id: int) -> Admin | None:
        stmt = select(Admin).where(Admin.id == admin_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_admin(self, username: str, password_hash: str, is_active: bool = True) -> Admin:
        admin = Admin(username=username, password_hash=password_hash, is_active=is_active)
        self.session.add(admin)
        await self.session.flush()
        return admin

    async def update_last_login(self, admin_id: int) -> None:
        stmt = update(Admin).where(Admin.id == admin_id).values(last_login_at=utc_now())
        await self.session.execute(stmt)
        await self.session.flush()


class AllowedNumberRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_phone(self, phone_number: str) -> AllowedNumber | None:
        stmt = select(AllowedNumber).where(AllowedNumber.phone_number == phone_number)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_lid(self, lid_number: str) -> AllowedNumber | None:
        if not lid_number:
            return None
        stmt = select(AllowedNumber).where(AllowedNumber.lid_number == lid_number)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, number_id: int) -> AllowedNumber | None:
        stmt = select(AllowedNumber).where(AllowedNumber.id == number_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_numbers(self, active_only: bool = False) -> Sequence[AllowedNumber]:
        stmt = select(AllowedNumber).order_by(AllowedNumber.name)
        if active_only:
            stmt = stmt.where(AllowedNumber.is_active.is_(True))
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def list_paginated(
        self, search: str | None = None, offset: int = 0, limit: int = 20
    ) -> tuple[Sequence[AllowedNumber], int]:
        stmt = select(AllowedNumber)
        count_stmt = select(func.count()).select_from(AllowedNumber)

        if search:
            q = f"%{search.strip()}%"
            cond = (
                AllowedNumber.name.ilike(q)
                | AllowedNumber.phone_number.contains(search.strip())
                | AllowedNumber.lid_number.contains(search.strip())
            )
            stmt = stmt.where(cond)
            count_stmt = count_stmt.where(cond)

        stmt = stmt.order_by(AllowedNumber.name).offset(offset).limit(limit)
        items_res = await self.session.execute(stmt)
        count_res = await self.session.execute(count_stmt)
        return items_res.scalars().all(), int(count_res.scalar() or 0)

    async def count_active(self) -> int:
        stmt = select(func.count()).select_from(AllowedNumber).where(AllowedNumber.is_active.is_(True))
        result = await self.session.execute(stmt)
        return int(result.scalar() or 0)

    async def create_number(
        self,
        name: str,
        phone_number: str,
        lid_number: str | None = None,
        notes: str | None = None,
        is_active: bool = True,
    ) -> AllowedNumber:
        num = AllowedNumber(
            name=name.strip(),
            phone_number=phone_number.strip(),
            lid_number=lid_number.strip() if lid_number else None,
            notes=notes.strip() if notes else None,
            is_active=is_active,
        )
        self.session.add(num)
        await self.session.flush()
        return num

    async def update_number(
        self,
        number_id: int,
        name: str,
        phone_number: str,
        lid_number: str | None = None,
        notes: str | None = None,
    ) -> AllowedNumber | None:
        num = await self.get_by_id(number_id)
        if num:
            num.name = name.strip()
            num.phone_number = phone_number.strip()
            num.lid_number = lid_number.strip() if lid_number else None
            num.notes = notes.strip() if notes else None
            num.updated_at = utc_now()
            await self.session.flush()
        return num

    async def assign_lid(self, number_id: int, lid_number: str) -> AllowedNumber | None:
        num = await self.get_by_id(number_id)
        if num:
            num.lid_number = lid_number.strip()
            num.updated_at = utc_now()
            await self.session.flush()
        return num

    async def remove_lid(self, number_id: int) -> AllowedNumber | None:
        num = await self.get_by_id(number_id)
        if num:
            num.lid_number = None
            num.updated_at = utc_now()
            await self.session.flush()
        return num

    async def toggle_active(self, number_id: int) -> AllowedNumber | None:
        num = await self.get_by_id(number_id)
        if num:
            num.is_active = not num.is_active
            num.updated_at = utc_now()
            await self.session.flush()
        return num

    async def delete_number(self, number_id: int) -> bool:
        num = await self.get_by_id(number_id)
        if num:
            await self.session.delete(num)
            await self.session.flush()
            return True
        return False

    async def increment_job_stats(self, phone_number: str) -> None:
        stmt = (
            update(AllowedNumber)
            .where(AllowedNumber.phone_number == phone_number)
            .values(total_jobs=AllowedNumber.total_jobs + 1, last_used_at=utc_now())
        )
        await self.session.execute(stmt)
        await self.session.flush()


class UnmappedLidRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_lid(self, lid_number: str) -> UnmappedLid | None:
        stmt = select(UnmappedLid).where(UnmappedLid.lid_number == lid_number)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, unmapped_id: int) -> UnmappedLid | None:
        stmt = select(UnmappedLid).where(UnmappedLid.id == unmapped_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert_unmapped(
        self, lid_number: str, inbound_message_id: str | None = None, message_preview: str | None = None
    ) -> UnmappedLid:
        existing = await self.get_by_lid(lid_number)
        preview_clean = message_preview[:150] if message_preview else None
        now = utc_now()

        if existing:
            existing.occurrence_count += 1
            existing.last_seen_at = now
            if inbound_message_id:
                existing.last_inbound_message_id = inbound_message_id
            if preview_clean:
                existing.last_message_preview = preview_clean
            existing.updated_at = now
            await self.session.flush()
            return existing

        record = UnmappedLid(
            lid_number=lid_number,
            last_inbound_message_id=inbound_message_id,
            last_message_preview=preview_clean,
            occurrence_count=1,
            first_seen_at=now,
            last_seen_at=now,
            resolved_at=None,
        )
        self.session.add(record)
        await self.session.flush()
        return record

    async def list_paginated(
        self, search: str | None = None, offset: int = 0, limit: int = 20
    ) -> tuple[Sequence[UnmappedLid], int]:
        stmt = select(UnmappedLid)
        count_stmt = select(func.count()).select_from(UnmappedLid)

        if search:
            q = f"%{search.strip()}%"
            cond = UnmappedLid.lid_number.contains(search.strip()) | UnmappedLid.last_message_preview.ilike(q)
            stmt = stmt.where(cond)
            count_stmt = count_stmt.where(cond)

        stmt = stmt.order_by(desc(UnmappedLid.last_seen_at)).offset(offset).limit(limit)
        items_res = await self.session.execute(stmt)
        count_res = await self.session.execute(count_stmt)
        return items_res.scalars().all(), int(count_res.scalar() or 0)

    async def count_unresolved(self) -> int:
        stmt = select(func.count()).select_from(UnmappedLid).where(UnmappedLid.resolved_at.is_(None))
        res = await self.session.execute(stmt)
        return int(res.scalar() or 0)

    async def resolve(self, unmapped_id: int) -> UnmappedLid | None:
        record = await self.get_by_id(unmapped_id)
        if record:
            record.resolved_at = utc_now()
            record.updated_at = utc_now()
            await self.session.flush()
        return record

    async def delete_history(self, unmapped_id: int) -> bool:
        record = await self.get_by_id(unmapped_id)
        if record:
            await self.session.delete(record)
            await self.session.flush()
            return True
        return False


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, job_id: str) -> DownloadJob | None:
        stmt = (
            select(DownloadJob)
            .options(selectinload(DownloadJob.items))
            .execution_options(populate_existing=True)
            .where(DownloadJob.id == job_id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_inbound_message_id(self, inbound_id: str) -> DownloadJob | None:
        stmt = (
            select(DownloadJob)
            .options(selectinload(DownloadJob.items))
            .execution_options(populate_existing=True)
            .where(DownloadJob.inbound_message_id == inbound_id)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_active_job_for_number(self, sender_number: str) -> DownloadJob | None:
        active_statuses = ("queued", "extracting", "downloading", "processing", "sending")
        stmt = select(DownloadJob).where(
            DownloadJob.sender_number == sender_number,
            DownloadJob.status.in_(active_statuses)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_awaiting_choice_job_for_number(self, sender_number: str) -> DownloadJob | None:
        stmt = (
            select(DownloadJob)
            .options(selectinload(DownloadJob.items))
            .execution_options(populate_existing=True)
            .where(
                DownloadJob.sender_number == sender_number,
                DownloadJob.status == "awaiting_choice"
            )
            .order_by(desc(DownloadJob.created_at))
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_job(
        self,
        inbound_message_id: str,
        webhook_event_id: str,
        sender_number: str,
        original_url: str,
        canonical_url: str | None = None,
        platform: str = "tiktok",
        status: str = "queued",
        selected_mode: str | None = None,
        music_url: str | None = None,
    ) -> DownloadJob:
        job = DownloadJob(
            inbound_message_id=inbound_message_id,
            webhook_event_id=webhook_event_id,
            sender_number=sender_number,
            platform=platform,
            original_url=original_url,
            canonical_url=canonical_url,
            status=status,
            selected_mode=selected_mode,
            music_url=music_url,
            queued_at=utc_now(),
        )
        self.session.add(job)
        await self.session.flush()
        await self.session.refresh(job, ["items"])
        return job

    async def get_next_queued_job(self) -> DownloadJob | None:
        stmt = (
            select(DownloadJob)
            .options(selectinload(DownloadJob.items))
            .execution_options(populate_existing=True)
            .where(DownloadJob.status == "queued")
            .order_by(DownloadJob.queued_at.asc(), DownloadJob.created_at.asc())
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_incomplete_jobs(self) -> Sequence[DownloadJob]:
        active_statuses = ("extracting", "downloading", "processing", "sending")
        stmt = (
            select(DownloadJob)
            .options(selectinload(DownloadJob.items))
            .execution_options(populate_existing=True)
            .where(DownloadJob.status.in_(active_statuses))
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_dashboard_stats(self) -> dict[str, int]:
        total_stmt = select(func.count()).select_from(DownloadJob)
        completed_stmt = select(func.count()).select_from(DownloadJob).where(DownloadJob.status == "completed")
        failed_stmt = select(func.count()).select_from(DownloadJob).where(DownloadJob.status == "failed")
        active_stmt = select(func.count()).select_from(DownloadJob).where(
            DownloadJob.status.in_(("queued", "extracting", "downloading", "processing", "sending"))
        )
        tiktok_stmt = select(func.count()).select_from(DownloadJob).where(DownloadJob.platform == "tiktok")
        ig_stmt = select(func.count()).select_from(DownloadJob).where(DownloadJob.platform == "instagram")
        unmapped_stmt = select(func.count()).select_from(UnmappedLid).where(UnmappedLid.resolved_at.is_(None))

        total_res = await self.session.execute(total_stmt)
        comp_res = await self.session.execute(completed_stmt)
        fail_res = await self.session.execute(failed_stmt)
        act_res = await self.session.execute(active_stmt)
        tiktok_res = await self.session.execute(tiktok_stmt)
        ig_res = await self.session.execute(ig_stmt)
        unmapped_res = await self.session.execute(unmapped_stmt)

        return {
            "total_jobs": int(total_res.scalar() or 0),
            "completed_jobs": int(comp_res.scalar() or 0),
            "failed_jobs": int(fail_res.scalar() or 0),
            "active_jobs": int(act_res.scalar() or 0),
            "tiktok_jobs": int(tiktok_res.scalar() or 0),
            "instagram_jobs": int(ig_res.scalar() or 0),
            "unmapped_lids_count": int(unmapped_res.scalar() or 0),
        }

    async def list_recent_jobs(self, limit: int = 10) -> Sequence[DownloadJob]:
        stmt = select(DownloadJob).options(selectinload(DownloadJob.items)).order_by(desc(DownloadJob.created_at)).limit(limit)
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def list_jobs_paginated(
        self,
        phone_number: str | None = None,
        status: str | None = None,
        content_type: str | None = None,
        platform: str | None = None,
        search: str | None = None,
        offset: int = 0,
        limit: int = 20,
    ) -> tuple[Sequence[DownloadJob], int]:
        stmt = select(DownloadJob).options(selectinload(DownloadJob.items))
        count_stmt = select(func.count()).select_from(DownloadJob)

        conditions = []
        if phone_number:
            conditions.append(DownloadJob.sender_number.contains(phone_number))
        if status and status != "all":
            conditions.append(DownloadJob.status == status)
        if content_type and content_type != "all":
            conditions.append(DownloadJob.content_type == content_type)
        if platform and platform != "all":
            conditions.append(DownloadJob.platform == platform)
        if search:
            conditions.append(
                (DownloadJob.original_url.contains(search)) | (DownloadJob.sender_number.contains(search))
            )

        for cond in conditions:
            stmt = stmt.where(cond)
            count_stmt = count_stmt.where(cond)

        stmt = stmt.order_by(desc(DownloadJob.created_at)).offset(offset).limit(limit)

        items_res = await self.session.execute(stmt)
        count_res = await self.session.execute(count_stmt)

        return items_res.scalars().all(), int(count_res.scalar() or 0)

    async def get_items_for_job(self, job_id: str) -> Sequence[DownloadItem]:
        stmt = select(DownloadItem).where(DownloadItem.job_id == job_id).order_by(DownloadItem.position)
        result = await self.session.execute(stmt)
        return result.scalars().all()


class WebhookEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_event_id(self, event_id: str) -> WebhookEvent | None:
        stmt = select(WebhookEvent).where(WebhookEvent.event_id == event_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create_event(self, event_id: str, event_type: str, payload_hash: str) -> WebhookEvent:
        event = WebhookEvent(event_id=event_id, event_type=event_type, payload_hash=payload_hash)
        self.session.add(event)
        await self.session.flush()
        return event
