from unittest.mock import patch

import pytest
from app.database.models import Admin
from app.database.repositories import JobRepository
from app.dependencies import get_current_admin, get_db
from app.main import app
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_static_assets_routes() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # GET /static/style.css
        resp_css = await client.get("/static/style.css")
        assert resp_css.status_code == 200
        assert "text/css" in resp_css.headers.get("content-type", "").lower()
        assert len(resp_css.text.strip()) > 0

        # GET /static/main.js
        resp_js = await client.get("/static/main.js")
        assert resp_js.status_code == 200
        assert "javascript" in resp_js.headers.get("content-type", "").lower()
        assert len(resp_js.text.strip()) > 0


@pytest.mark.asyncio
async def test_all_admin_templates_rendering(test_db) -> None:
    # 1. Create admin user
    admin = Admin(id=1, username="admin_test", password_hash="dummy_hash", is_active=True)
    test_db.add(admin)

    # 2. Create dummy job for history detail test
    job_repo = JobRepository(test_db)
    job = await job_repo.create_job(
        inbound_message_id="msg-tmpl-test-01",
        webhook_event_id="evt-tmpl-test-01",
        sender_number="6281234567890",
        original_url="https://www.instagram.com/reel/C1234567890/",
        canonical_url="https://www.instagram.com/reel/C1234567890/",
        platform="instagram",
    )
    await test_db.commit()

    async def mock_get_admin():
        return admin

    async def mock_get_db():
        yield test_db

    app.dependency_overrides[get_current_admin] = mock_get_admin
    app.dependency_overrides[get_db] = mock_get_db

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            with patch("app.dependencies.get_csrf_token", return_value="test-csrf-token"):
                # 1. Login Page
                resp_login = await client.get("/admin/login")
                assert resp_login.status_code == 200
                assert "/static/style.css" in resp_login.text
                assert "/static/main.js" in resp_login.text

                # 2. Dashboard Page
                resp_dash = await client.get("/admin")
                assert resp_dash.status_code == 200
                assert "/static/style.css" in resp_dash.text
                assert "/static/main.js" in resp_dash.text
                assert "Ikhtisar Sistem" in resp_dash.text

                # 3. Allowed Numbers Page
                resp_num = await client.get("/admin/allowed-numbers")
                assert resp_num.status_code == 200
                assert "Manajemen Nomor WhatsApp Diizinkan" in resp_num.text

                # 4. Unmapped LIDs Page
                resp_unmapped = await client.get("/admin/unmapped-lids")
                assert resp_unmapped.status_code == 200
                assert "LID WhatsApp Belum Dipetakan" in resp_unmapped.text

                # 5. History Page
                resp_hist = await client.get("/admin/history")
                assert resp_hist.status_code == 200
                assert "Riwayat Pemrosesan" in resp_hist.text

                # 6. History Detail Page
                resp_detail = await client.get(f"/admin/history/{job.id}")
                assert resp_detail.status_code == 200
                assert f"Detail Pekerjaan #{job.id[:8]}" in resp_detail.text

                # 7. Settings Page
                resp_sett = await client.get("/admin/settings")
                assert resp_sett.status_code == 200
                assert "Pengaturan Sistem" in resp_sett.text
    finally:
        app.dependency_overrides.clear()
