from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, Form, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.service import rotate_session, verify_password
from app.database.connection import get_db
from app.database.repositories import AdminRepository
from app.dependencies import get_csrf_token, require_csrf
from app.security.rate_limit import check_login_rate_limit

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/auth/sso/redirect")
@router.get("/sso/redirect")
async def sso_redirect() -> RedirectResponse:
    client_id = "tiktok_bot"
    redirect_uri = "https://tiktok-bot.sangkolo.my.id/auth/sso/callback"
    return RedirectResponse(
        url=f"https://dashboard.sangkolo.com/sso/authorize?client_id={client_id}&redirect_uri={redirect_uri}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/auth/sso/callback")
async def sso_callback(
    request: Request,
    ticket: str | None = None,
    db: AsyncSession = Depends(get_db),
) -> Response:
    if not ticket:
        return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://dashboard.sangkolo.com/api/sso/verify",
                json={
                    "client_id": "tiktok_bot",
                    "client_secret": "sk_ttb_21baef45b4fb6df9b993a201b39b1b94a107859b50c511f4a5122944a95bd905",
                    "ticket": ticket,
                },
                headers={"Accept": "application/json"},
            )
            data = resp.json()
            if not resp.is_success or not (data.get("success") or data.get("valid")):
                return RedirectResponse(url="/admin/login?error=sso_invalid", status_code=status.HTTP_303_SEE_OTHER)

            repo = AdminRepository(db)
            admin = await repo.get_by_username("bayezsyka")
            if not admin:
                admin = await repo.get_by_id(1)
            if not admin:
                return RedirectResponse(url="/admin/login?error=no_admin", status_code=status.HTTP_303_SEE_OTHER)

            rotate_session(request.session, admin.id)
            await repo.update_last_login(admin.id)
            await db.commit()

            return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
    except Exception:
        return RedirectResponse(url="/admin/login?error=sso_exception", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> Response:
    if request.session.get("admin_id"):
        return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "csrf_token": get_csrf_token(request), "error": None},
    )


@router.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    csrf_token: str | None = Form(None, alias="_csrf_token"),
    db: AsyncSession = Depends(get_db),
) -> Response:
    # Rate limiting check
    client_ip = request.client.host if request.client else "unknown"
    check_login_rate_limit(client_ip)

    # CSRF check
    await require_csrf(request)

    repo = AdminRepository(db)
    admin = await repo.get_by_username(username.strip())

    if not admin or not admin.is_active or not verify_password(admin.password_hash, password):
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "request": request,
                "csrf_token": get_csrf_token(request),
                "error": "Username atau password salah.",
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    # Rotate session
    rotate_session(request.session, admin.id)
    await repo.update_last_login(admin.id)
    await db.commit()

    return RedirectResponse(url="/admin", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/logout")
async def logout(request: Request, csrf_token: str | None = Form(None, alias="_csrf_token")) -> RedirectResponse:
    await require_csrf(request)
    request.session.clear()
    return RedirectResponse(url="/admin/login", status_code=status.HTTP_303_SEE_OTHER)
