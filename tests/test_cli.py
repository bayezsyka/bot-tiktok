import argparse

import pytest
from app.database.connection import AsyncSessionLocal
from app.database.repositories import AdminRepository, AllowedNumberRepository
from cli import add_number_cmd, create_admin_cmd, remove_number_cmd


@pytest.mark.asyncio
async def test_cli_create_admin() -> None:
    args = argparse.Namespace(username="testcliadmin", password="SecretPassword123", email="admin@test.com")
    await create_admin_cmd(args)

    async with AsyncSessionLocal() as session:
        repo = AdminRepository(session)
        admin = await repo.get_by_username("testcliadmin")
        assert admin is not None
        assert admin.email == "admin@test.com"


@pytest.mark.asyncio
async def test_cli_add_and_remove_number() -> None:
    args_add = argparse.Namespace(phone="081999888777", name="CLI Test Number", notes="Added via test")
    await add_number_cmd(args_add)

    async with AsyncSessionLocal() as session:
        repo = AllowedNumberRepository(session)
        num = await repo.get_by_phone("6281999888777")
        assert num is not None
        assert num.name == "CLI Test Number"

    args_rm = argparse.Namespace(phone="6281999888777")
    await remove_number_cmd(args_rm)

    async with AsyncSessionLocal() as session:
        repo = AllowedNumberRepository(session)
        num_after = await repo.get_by_phone("6281999888777")
        assert num_after is None
