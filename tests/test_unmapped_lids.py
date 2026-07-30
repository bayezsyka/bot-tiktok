import pytest
from app.admin.allowed_number_service import AllowedNumberService
from app.database.repositories import AllowedNumberRepository, UnmappedLidRepository
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_unmapped_lid_upsert_and_resolution(test_db: AsyncSession) -> None:
    unmapped_repo = UnmappedLidRepository(test_db)
    service = AllowedNumberService(test_db)

    # 1. Upsert new unmapped LID
    rec1 = await unmapped_repo.upsert_unmapped(
        lid_number="88877766655544",
        inbound_message_id="msg-101",
        message_preview="A" * 200,  # 200 chars long text
    )
    await test_db.commit()

    assert rec1.lid_number == "88877766655544"
    assert rec1.occurrence_count == 1
    assert len(rec1.last_message_preview or "") == 150
    assert rec1.last_inbound_message_id == "msg-101"
    assert rec1.resolved_at is None

    # 2. Second message from same LID increments count
    rec2 = await unmapped_repo.upsert_unmapped(
        lid_number="88877766655544",
        inbound_message_id="msg-102",
        message_preview="Short text",
    )
    await test_db.commit()
    assert rec2.id == rec1.id
    assert rec2.occurrence_count == 2
    assert rec2.last_inbound_message_id == "msg-102"
    assert rec2.last_message_preview == "Short text"

    # 3. Create an allowed number to resolve to
    num_repo = AllowedNumberRepository(test_db)
    target_num = await num_repo.create_number(
        name="Target User", phone_number="6281122334455"
    )
    await test_db.commit()

    # 4. Resolve unmapped LID to existing allowed number
    resolved, err = await service.resolve_unmapped_to_existing(rec2.id, target_num.id)
    assert err is None
    assert resolved is not None
    assert resolved.resolved_at is not None

    # Verify number has the assigned LID
    num_after = await num_repo.get_by_id(target_num.id)
    assert num_after is not None
    assert num_after.lid_number == "88877766655544"

    # 5. Test create new number from unmapped LID
    rec_new = await unmapped_repo.upsert_unmapped(
        lid_number="12398745600011",
        inbound_message_id="msg-201",
        message_preview="Hello bot",
    )
    await test_db.commit()

    new_num, err_new = await service.resolve_unmapped_to_new(
        unmapped_id=rec_new.id,
        name="New User From Unmapped",
        raw_phone="6289900112233",
        notes="Created from unmapped flow",
    )
    assert err_new is None
    assert new_num is not None
    assert new_num.phone_number == "6289900112233"
    assert new_num.lid_number == "12398745600011"

    rec_new_after = await unmapped_repo.get_by_id(rec_new.id)
    assert rec_new_after is not None
    assert rec_new_after.resolved_at is not None

    # 6. Delete history
    del_ok = await unmapped_repo.delete_history(rec_new.id)
    await test_db.commit()
    assert del_ok is True
    assert await unmapped_repo.get_by_id(rec_new.id) is None
