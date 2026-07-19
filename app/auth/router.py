from pathlib import Path

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
