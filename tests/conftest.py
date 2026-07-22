# ruff: noqa: E402
import asyncio
import os
import tempfile
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Set environment before importing app modules
os.environ["APP_ENV"] = "testing"
os.environ["FARROS_WA_WEBHOOK_SECRET"] = "test-webhook-secret-123456"
os.environ["FARROS_WA_API_KEY"] = "test-gateway-api-key-123456"

# Create a shared temporary database file for the test session
_fd, _test_db_path = tempfile.mkstemp(suffix=".sqlite")
os.close(_fd)
os.environ["DATABASE_PATH"] = _test_db_path

from app.database.connection import get_db, get_session_maker
from app.database.migrations import run_migrations
from app.database.models import Admin, AllowedNumber, DownloadItem, DownloadJob, WebhookEvent
from app.main import app
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
    # Cleanup temp db after session loop closes
    if os.path.exists(_test_db_path):
        try:
            os.unlink(_test_db_path)
        except OSError:
            pass


@pytest_asyncio.fixture
async def test_db() -> AsyncGenerator[AsyncSession, None]:
    await run_migrations()
    session_maker = get_session_maker()
    async with session_maker() as session:
        for model in [DownloadItem, DownloadJob, WebhookEvent, AllowedNumber, Admin]:
            await session.execute(delete(model))
        await session.commit()
        yield session




@pytest_asyncio.fixture
async def client(test_db: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield test_db

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()
