from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.database.models import AllowedNumber, UnmappedLid
from app.database.repositories import AllowedNumberRepository, UnmappedLidRepository
from app.security.urls import normalize_phone_number, parse_lid_mapping


class AllowedNumberService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.number_repo = AllowedNumberRepository(session)
        self.unmapped_repo = UnmappedLidRepository(session)

    @staticmethod
    def validate_lid(raw_lid: str | None) -> tuple[str | None, str | None]:
        if not raw_lid or not str(raw_lid).strip():
            return None, None
        clean_lid = str(raw_lid).strip()
        if not clean_lid.isdigit():
            return None, "LID hanya boleh berisi angka (digit)."
        return clean_lid, None

    async def add_number(
        self, name: str, raw_phone: str, raw_lid: str | None = None, notes: str | None = None, is_active: bool = True
    ) -> tuple[AllowedNumber | None, str | None]:
        if not name or not name.strip():
            return None, "Nama pengguna wajib diisi."

        norm_phone = normalize_phone_number(raw_phone)
        if not norm_phone:
            return None, "Format nomor WhatsApp tidak valid. Gunakan format 628... (10-15 digit)."

        clean_lid, lid_err = self.validate_lid(raw_lid)
        if lid_err:
            return None, lid_err

        existing_phone = await self.number_repo.get_by_phone(norm_phone)
        if existing_phone:
            return None, f"Nomor WhatsApp {norm_phone} sudah terdaftar dalam whitelist."

        if clean_lid:
            existing_lid = await self.number_repo.get_by_lid(clean_lid)
            if existing_lid:
                return None, f"LID {clean_lid} sudah dipasangkan ke nomor {existing_lid.phone_number} ({existing_lid.name})."

        number = await self.number_repo.create_number(
            name=name, phone_number=norm_phone, lid_number=clean_lid, notes=notes, is_active=is_active
        )
        await self.session.commit()
        return number, None

    async def update_number(
        self, number_id: int, name: str, raw_phone: str, raw_lid: str | None = None, notes: str | None = None
    ) -> tuple[AllowedNumber | None, str | None]:
        if not name or not name.strip():
            return None, "Nama pengguna wajib diisi."

        norm_phone = normalize_phone_number(raw_phone)
        if not norm_phone:
            return None, "Format nomor WhatsApp tidak valid."

        clean_lid, lid_err = self.validate_lid(raw_lid)
        if lid_err:
            return None, lid_err

        existing_num = await self.number_repo.get_by_id(number_id)
        if not existing_num:
            return None, "Nomor tidak ditemukan."

        existing_phone = await self.number_repo.get_by_phone(norm_phone)
        if existing_phone and existing_phone.id != number_id:
            return None, f"Nomor WhatsApp {norm_phone} sudah digunakan oleh entri lain."

        if clean_lid:
            existing_lid = await self.number_repo.get_by_lid(clean_lid)
            if existing_lid and existing_lid.id != number_id:
                return None, f"LID {clean_lid} sudah digunakan oleh nomor {existing_lid.phone_number}."

        updated = await self.number_repo.update_number(
            number_id=number_id, name=name, phone_number=norm_phone, lid_number=clean_lid, notes=notes
        )
        await self.session.commit()
        return updated, None

    async def assign_lid(self, number_id: int, raw_lid: str) -> tuple[AllowedNumber | None, str | None]:
        clean_lid, lid_err = self.validate_lid(raw_lid)
        if lid_err or not clean_lid:
            return None, lid_err or "LID wajib diisi."

        num = await self.number_repo.get_by_id(number_id)
        if not num:
            return None, "Nomor tidak ditemukan."

        existing_lid = await self.number_repo.get_by_lid(clean_lid)
        if existing_lid and existing_lid.id != number_id:
            return None, f"LID {clean_lid} sudah dipasangkan ke nomor {existing_lid.phone_number}."

        res = await self.number_repo.assign_lid(number_id, clean_lid)
        await self.session.commit()
        return res, None

    async def remove_lid(self, number_id: int) -> tuple[AllowedNumber | None, str | None]:
        num = await self.number_repo.get_by_id(number_id)
        if not num:
            return None, "Nomor tidak ditemukan."

        res = await self.number_repo.remove_lid(number_id)
        await self.session.commit()
        return res, None

    async def toggle_active(self, number_id: int) -> AllowedNumber | None:
        res = await self.number_repo.toggle_active(number_id)
        await self.session.commit()
        return res

    async def delete_number(self, number_id: int) -> bool:
        res = await self.number_repo.delete_number(number_id)
        await self.session.commit()
        return res

    async def resolve_unmapped_to_existing(
        self, unmapped_id: int, number_id: int
    ) -> tuple[UnmappedLid | None, str | None]:
        unmapped = await self.unmapped_repo.get_by_id(unmapped_id)
        if not unmapped:
            return None, "Record unmapped LID tidak ditemukan."

        num = await self.number_repo.get_by_id(number_id)
        if not num:
            return None, "Nomor WhatsApp sasaran tidak ditemukan."

        if num.lid_number and num.lid_number != unmapped.lid_number:
            return None, f"Nomor {num.phone_number} sudah memiliki LID lain ({num.lid_number}). Hapus LID lama terlebih dahulu."

        existing_lid_owner = await self.number_repo.get_by_lid(unmapped.lid_number)
        if existing_lid_owner and existing_lid_owner.id != number_id:
            return None, f"LID {unmapped.lid_number} sudah terpasang pada nomor lain ({existing_lid_owner.phone_number})."

        await self.number_repo.assign_lid(number_id, unmapped.lid_number)
        resolved = await self.unmapped_repo.resolve(unmapped_id)
        await self.session.commit()
        return resolved, None

    async def resolve_unmapped_to_new(
        self, unmapped_id: int, name: str, raw_phone: str, notes: str | None = None
    ) -> tuple[AllowedNumber | None, str | None]:
        unmapped = await self.unmapped_repo.get_by_id(unmapped_id)
        if not unmapped:
            return None, "Record unmapped LID tidak ditemukan."

        number, err = await self.add_number(
            name=name, raw_phone=raw_phone, raw_lid=unmapped.lid_number, notes=notes, is_active=True
        )
        if err:
            return None, err

        await self.unmapped_repo.resolve(unmapped_id)
        await self.session.commit()
        return number, None

    async def preview_env_import(self) -> list[dict[str, Any]]:
        raw_mapping = parse_lid_mapping()
        results = []

        for lid, phone in raw_mapping.items():
            num = await self.number_repo.get_by_phone(phone)
            lid_owner = await self.number_repo.get_by_lid(lid)

            status = "ready"
            reason = "Siap diimpor"

            if not num:
                status = "not_found"
                reason = f"Nomor {phone} belum ada di whitelist"
            elif num.lid_number == lid:
                status = "imported"
                reason = "Sudah terimpor sebelumnya"
            elif num.lid_number and num.lid_number != lid:
                status = "conflict"
                reason = f"Nomor {phone} sudah memiliki LID lain ({num.lid_number})"
            elif lid_owner and lid_owner.id != num.id:
                status = "conflict"
                reason = f"LID {lid} sudah terpasang di nomor {lid_owner.phone_number}"

            results.append(
                {
                    "lid_number": lid,
                    "phone_number": phone,
                    "name": num.name if num else "-",
                    "status": status,
                    "reason": reason,
                }
            )

        return results

    async def execute_env_import(self) -> tuple[int, int, list[str]]:
        preview = await self.preview_env_import()
        imported_count = 0
        skipped_count = 0
        conflicts = []

        for item in preview:
            status = item["status"]
            if status == "ready":
                num = await self.number_repo.get_by_phone(item["phone_number"])
                if num:
                    await self.number_repo.assign_lid(num.id, item["lid_number"])
                    imported_count += 1
            elif status in ("imported", "not_found"):
                skipped_count += 1
            elif status == "conflict":
                conflicts.append(f"LID {item['lid_number']} -> {item['phone_number']}: {item['reason']}")
                skipped_count += 1

        await self.session.commit()
        return imported_count, skipped_count, conflicts
