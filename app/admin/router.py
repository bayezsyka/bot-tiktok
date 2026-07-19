import math
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.admin.service import AdminService
from app.config import get_settings
from app.database.connection import get_db
from app.database.models import Admin
from app.dependencies import get_csrf_token, get_current_admin, require_csrf

router = APIRouter(dependencies=[Depends(get_current_admin)])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def dashboard_page(
    request: Request,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    service = AdminService(db)
    data = await service.get_dashboard_data()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "admin": admin,
            "csrf_token": get_csrf_token(request),
            "stats": data["stats"],
            "active_numbers": data["active_numbers"],
            "recent_jobs": data["recent_jobs"],
            "temp_disk_used_bytes": data["temp_disk_used_bytes"],
            "disk_free_bytes": data["disk_free_bytes"],
        },
    )


@router.get("/numbers", response_class=HTMLResponse)
async def numbers_page(
    request: Request,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    service = AdminService(db)
    numbers = await service.number_repo.list_numbers()
    return templates.TemplateResponse(
        request,
        "numbers.html",
        {
            "request": request,
            "admin": admin,
            "csrf_token": get_csrf_token(request),
            "numbers": numbers,
            "error": None,
            "success": None,
        },
    )


@router.post("/numbers", response_class=HTMLResponse)
async def add_number(
    request: Request,
    name: str = Form(...),
    phone_number: str = Form(...),
    notes: str | None = Form(None),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AdminService(db)
    num, err = await service.add_allowed_number(name, phone_number, notes)

    numbers = await service.number_repo.list_numbers()
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(
            request,
            "numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "error": err,
                "success": "Nomor berhasil ditambahkan." if not err else None,
            },
        )
    return RedirectResponse(url="/admin/numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/numbers/{number_id}/toggle", response_class=HTMLResponse)
async def toggle_number(
    request: Request,
    number_id: int,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AdminService(db)
    await service.toggle_number_status(number_id)

    numbers = await service.number_repo.list_numbers()
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(
            request,
            "numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "error": None,
                "success": "Status nomor berhasil diperbarui.",
            },
        )
    return RedirectResponse(url="/admin/numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/numbers/{number_id}/delete", response_class=HTMLResponse)
async def delete_number(
    request: Request,
    number_id: int,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AdminService(db)
    await service.delete_number(number_id)

    numbers = await service.number_repo.list_numbers()
    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(
            request,
            "numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "error": None,
                "success": "Nomor berhasil dihapus.",
            },
        )
    return RedirectResponse(url="/admin/numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/history", response_class=HTMLResponse)
async def history_page(
    request: Request,
    phone_number: str | None = Query(None),
    status_val: str | None = Query("all", alias="status"),
    content_type: str | None = Query("all"),
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    service = AdminService(db)
    limit = 15
    offset = (page - 1) * limit
    jobs, total_count = await service.job_repo.list_jobs_paginated(
        phone_number=phone_number,
        status=status_val,
        content_type=content_type,
        search=search,
        offset=offset,
        limit=limit,
    )
    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1

    return templates.TemplateResponse(
        request,
        "history.html",
        {
            "request": request,
            "admin": admin,
            "csrf_token": get_csrf_token(request),
            "jobs": jobs,
            "total_count": total_count,
            "page": page,
            "total_pages": total_pages,
            "filter_phone": phone_number or "",
            "filter_status": status_val or "all",
            "filter_type": content_type or "all",
            "filter_search": search or "",
        },
    )


@router.get("/history/{job_id}", response_class=HTMLResponse)
async def history_detail_page(
    request: Request,
    job_id: str,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    service = AdminService(db)
    job = await service.job_repo.get_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")

    return templates.TemplateResponse(
        request,
        "history_detail.html",
        {
            "request": request,
            "admin": admin,
            "csrf_token": get_csrf_token(request),
            "job": job,
            "items": job.items,
            "message": None,
        },
    )


@router.post("/history/{job_id}/retry", response_class=HTMLResponse)
async def retry_job(
    request: Request,
    job_id: str,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AdminService(db)
    success, msg = await service.retry_failed_job(job_id)

    job = await service.job_repo.get_by_id(job_id)
    if not job:
        return RedirectResponse(url="/admin/history", status_code=status.HTTP_303_SEE_OTHER)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(
            request,
            "history_detail.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "job": job,
                "items": job.items,
                "message": msg,
            },
        )
    return RedirectResponse(url=f"/admin/history/{job_id}", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(
    request: Request,
    admin: Admin = Depends(get_current_admin),
) -> HTMLResponse:
    settings = get_settings()
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "request": request,
            "admin": admin,
            "csrf_token": get_csrf_token(request),
            "settings": settings,
        },
    )
