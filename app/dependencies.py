import uuid

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.connection import get_db
from app.database.models import Admin
from app.database.repositories import AdminRepository
from app.security.csrf import generate_csrf_token, validate_csrf_request


async def get_current_admin(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> Admin:
    admin_id = request.session.get("admin_id")
    if not admin_id:
        if request.headers.get("HX-Request") == "true":
            # Return 200 with HX-Redirect header for HTMX
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required",
                headers={"HX-Redirect": "/admin/login"},
            )
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Authentication required",
            headers={"Location": "/admin/login"},
        )

    repo = AdminRepository(db)
    admin = await repo.get_by_id(int(admin_id))
    if not admin or not admin.is_active:
        request.session.clear()
        if request.headers.get("HX-Request") == "true":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Admin account inactive or removed",
                headers={"HX-Redirect": "/admin/login"},
            )
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            detail="Admin account inactive or removed",
            headers={"Location": "/admin/login"},
        )

    return admin


def get_session_identifier(request: Request) -> str:
    """Ensure a unique session identifier exists in session for CSRF and tracking."""
    sid = request.session.get("session_id")
    if not sid:
        sid = str(uuid.uuid4())
        request.session["session_id"] = sid
    return str(sid)


def get_csrf_token(request: Request) -> str:
    sid = request.session.get("session_id") or request.session.get("admin_id")
    if not sid:
        sid = get_session_identifier(request)
    return generate_csrf_token(str(sid))


async def require_csrf(request: Request) -> None:
    await validate_csrf_request(request)
