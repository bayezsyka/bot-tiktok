from unittest.mock import AsyncMock, patch

import pytest
from app.database.models import Admin
from app.dependencies import get_current_admin, get_db
from app.main import app
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_admin_ui_allowed_numbers_and_unmapped_lids(test_db) -> None:
    # 1. Create admin user
    admin = Admin(id=1, username="admin_test", password_hash="dummy_hash", is_active=True)
    test_db.add(admin)
    await test_db.commit()

    async def mock_get_admin():
        return admin

    async def mock_get_db():
        yield test_db

    app.dependency_overrides[get_current_admin] = mock_get_admin
    app.dependency_overrides[get_db] = mock_get_db

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            with patch("app.dependencies.get_csrf_token", return_value="test-csrf-token"), \
                 patch("app.dependencies.validate_csrf_request", new_callable=AsyncMock), \
                 patch("app.security.csrf.validate_csrf_request", new_callable=AsyncMock):

                # GET /admin/allowed-numbers
                resp = await client.get("/admin/allowed-numbers")
                assert resp.status_code == 200
                assert "Manajemen Nomor WhatsApp Diizinkan" in resp.text

                # POST /admin/allowed-numbers (Add number with LID)
                resp_add = await client.post(
                    "/admin/allowed-numbers",
                    data={
                        "name": "Test User UI",
                        "phone_number": "6281234567899",
                        "lid_number": "11223344556677",
                        "notes": "Added from UI",
                        "_csrf_token": "test-csrf-token",
                    },
                    follow_redirects=True,
                )
                assert resp_add.status_code == 200
                assert "6281234567899" in resp_add.text
                assert "11223344556677" in resp_add.text

                # GET /admin/unmapped-lids
                resp_unmapped = await client.get("/admin/unmapped-lids")
                assert resp_unmapped.status_code == 200
                assert "LID WhatsApp Belum Dipetakan" in resp_unmapped.text

                # GET /admin/history with platform filter
                resp_history = await client.get("/admin/history?platform=instagram")
                assert resp_history.status_code == 200
                assert "Semua Platform" in resp_history.text or "Instagram" in resp_history.text
    finally:
        app.dependency_overrides.clear()
