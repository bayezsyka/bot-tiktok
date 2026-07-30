import pytest
from app.admin.allowed_number_service import AllowedNumberService
from app.database.repositories import AllowedNumberRepository
from app.security.urls import resolve_lid_to_phone
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_allowed_number_crud_and_lid(test_db: AsyncSession) -> None:
    service = AllowedNumberService(test_db)


    # 1. Create with LID
    num1, err1 = await service.add_number(
        name="User One", raw_phone="6281234567890", raw_lid="12345678901234", notes="Primary user"
    )
    assert err1 is None
    assert num1 is not None
    assert num1.phone_number == "6281234567890"
    assert num1.lid_number == "12345678901234"

    # 2. Create without LID
    num2, err2 = await service.add_number(name="User Two", raw_phone="089876543210")
    assert err2 is None
    assert num2 is not None
    assert num2.phone_number == "6289876543210"
    assert num2.lid_number is None

    # 3. Duplicate Phone rejected
    _, err_dup_phone = await service.add_number(name="User Dup Phone", raw_phone="6281234567890")
    assert err_dup_phone is not None
    assert "sudah terdaftar" in err_dup_phone

    # 4. Duplicate LID rejected
    _, err_dup_lid = await service.add_number(
        name="User Dup LID", raw_phone="6281111222233", raw_lid="12345678901234"
    )
    assert err_dup_lid is not None
    assert "sudah dipasangkan" in err_dup_lid

    # 5. Update Name & Phone & LID
    updated, err_upd = await service.update_number(
        number_id=num2.id, name="User Two Updated", raw_phone="6289876543210", raw_lid="99988877766655"
    )
    assert err_upd is None
    assert updated is not None
    assert updated.name == "User Two Updated"
    assert updated.lid_number == "99988877766655"

    # 6. Remove LID
    removed, err_rm = await service.remove_lid(num2.id)
    assert err_rm is None
    assert removed is not None
    assert removed.lid_number is None

    # 7. Toggle active status
    toggled = await service.toggle_active(num1.id)
    assert toggled is not None
    assert toggled.is_active is False
    toggled_again = await service.toggle_active(num1.id)
    assert toggled_again is not None
    assert toggled_again.is_active is True

    # 8. Lookups
    repo = AllowedNumberRepository(test_db)

    found_phone = await repo.get_by_phone("6281234567890")
    assert found_phone is not None
    assert found_phone.id == num1.id

    found_lid = await repo.get_by_lid("12345678901234")
    assert found_lid is not None
    assert found_lid.id == num1.id

    # 9. Delete number
    del_res = await service.delete_number(num2.id)
    assert del_res is True
    assert await repo.get_by_id(num2.id) is None


@pytest.mark.asyncio
async def test_lid_priority_and_fallback(test_db: AsyncSession) -> None:
    repo = AllowedNumberRepository(test_db)
    # DB has mapping LID 111222333444 -> 6281234567890
    await repo.create_number(
        name="DB User", phone_number="6281234567890", lid_number="111222333444", is_active=True
    )
    await test_db.commit()


    # DB mapping lookup
    db_match = await repo.get_by_lid("111222333444")
    assert db_match is not None
    assert db_match.phone_number == "6281234567890"

    # Fallback to env mapping string if missing in DB
    env_str = "555666777888:628999888777,111222333444:628000000000"
    resolved_env_only = resolve_lid_to_phone("555666777888", mapping_str=env_str)
    assert resolved_env_only == "628999888777"

    # DB takes priority over ENV (when checked in router)
    assert db_match.phone_number == "6281234567890"
