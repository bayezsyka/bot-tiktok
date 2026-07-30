import math
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.admin.allowed_number_service import AllowedNumberService
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
            "unmapped_count": data["unmapped_count"],
            "recent_jobs": data["recent_jobs"],
            "temp_disk_used_bytes": data["temp_disk_used_bytes"],
            "disk_free_bytes": data["disk_free_bytes"],
        },
    )


# Standard route: /admin/allowed-numbers and legacy alias /admin/numbers
@router.get("/allowed-numbers", response_class=HTMLResponse)
@router.get("/numbers", response_class=HTMLResponse)
async def allowed_numbers_page(
    request: Request,
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    service = AllowedNumberService(db)
    limit = 15
    offset = (page - 1) * limit
    numbers, total_count = await service.number_repo.list_paginated(search=search, offset=offset, limit=limit)
    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
    env_preview = await service.preview_env_import()

    return templates.TemplateResponse(
        request,
        "allowed_numbers.html",
        {
            "request": request,
            "admin": admin,
            "csrf_token": get_csrf_token(request),
            "numbers": numbers,
            "total_count": total_count,
            "page": page,
            "total_pages": total_pages,
            "search": search or "",
            "env_preview": env_preview,
            "error": None,
            "success": None,
        },
    )


@router.post("/allowed-numbers", response_class=HTMLResponse)
@router.post("/numbers", response_class=HTMLResponse)
async def add_allowed_number(
    request: Request,
    name: str = Form(...),
    phone_number: str = Form(...),
    lid_number: str | None = Form(None),
    notes: str | None = Form(None),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    num, err = await service.add_number(name=name, raw_phone=phone_number, raw_lid=lid_number, notes=notes)

    numbers, total_count = await service.number_repo.list_paginated(offset=0, limit=15)
    env_preview = await service.preview_env_import()

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(
            request,
            "allowed_numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "total_count": total_count,
                "page": 1,
                "total_pages": math.ceil(total_count / 15) if total_count > 0 else 1,
                "search": "",
                "env_preview": env_preview,
                "error": err,
                "success": "Nomor berhasil ditambahkan." if not err else None,
            },
        )
    return RedirectResponse(url="/admin/allowed-numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/allowed-numbers/{number_id}/edit", response_class=HTMLResponse)
async def edit_allowed_number(
    request: Request,
    number_id: int,
    name: str = Form(...),
    phone_number: str = Form(...),
    lid_number: str | None = Form(None),
    notes: str | None = Form(None),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    num, err = await service.update_number(
        number_id=number_id, name=name, raw_phone=phone_number, raw_lid=lid_number, notes=notes
    )

    if request.headers.get("HX-Request") == "true":
        numbers, total_count = await service.number_repo.list_paginated(offset=0, limit=15)
        env_preview = await service.preview_env_import()
        return templates.TemplateResponse(
            request,
            "allowed_numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "total_count": total_count,
                "page": 1,
                "total_pages": math.ceil(total_count / 15) if total_count > 0 else 1,
                "search": "",
                "env_preview": env_preview,
                "error": err,
                "success": "Nomor berhasil diperbarui." if not err else None,
            },
        )
    return RedirectResponse(url="/admin/allowed-numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/allowed-numbers/{number_id}/toggle", response_class=HTMLResponse)
@router.post("/numbers/{number_id}/toggle", response_class=HTMLResponse)
async def toggle_number(
    request: Request,
    number_id: int,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    await service.toggle_active(number_id)

    if request.headers.get("HX-Request") == "true":
        numbers, total_count = await service.number_repo.list_paginated(offset=0, limit=15)
        env_preview = await service.preview_env_import()
        return templates.TemplateResponse(
            request,
            "allowed_numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "total_count": total_count,
                "page": 1,
                "total_pages": math.ceil(total_count / 15) if total_count > 0 else 1,
                "search": "",
                "env_preview": env_preview,
                "error": None,
                "success": "Status nomor berhasil diperbarui.",
            },
        )
    return RedirectResponse(url="/admin/allowed-numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/allowed-numbers/{number_id}/delete", response_class=HTMLResponse)
@router.post("/numbers/{number_id}/delete", response_class=HTMLResponse)
async def delete_number(
    request: Request,
    number_id: int,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    await service.delete_number(number_id)

    if request.headers.get("HX-Request") == "true":
        numbers, total_count = await service.number_repo.list_paginated(offset=0, limit=15)
        env_preview = await service.preview_env_import()
        return templates.TemplateResponse(
            request,
            "allowed_numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "total_count": total_count,
                "page": 1,
                "total_pages": math.ceil(total_count / 15) if total_count > 0 else 1,
                "search": "",
                "env_preview": env_preview,
                "error": None,
                "success": "Nomor berhasil dihapus.",
            },
        )
    return RedirectResponse(url="/admin/allowed-numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/allowed-numbers/{number_id}/assign-lid", response_class=HTMLResponse)
async def assign_lid_route(
    request: Request,
    number_id: int,
    lid_number: str = Form(...),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    num, err = await service.assign_lid(number_id, lid_number)

    if request.headers.get("HX-Request") == "true":
        numbers, total_count = await service.number_repo.list_paginated(offset=0, limit=15)
        env_preview = await service.preview_env_import()
        return templates.TemplateResponse(
            request,
            "allowed_numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "total_count": total_count,
                "page": 1,
                "total_pages": math.ceil(total_count / 15) if total_count > 0 else 1,
                "search": "",
                "env_preview": env_preview,
                "error": err,
                "success": "LID berhasil dipasangkan." if not err else None,
            },
        )
    return RedirectResponse(url="/admin/allowed-numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/allowed-numbers/{number_id}/remove-lid", response_class=HTMLResponse)
async def remove_lid_route(
    request: Request,
    number_id: int,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    num, err = await service.remove_lid(number_id)

    if request.headers.get("HX-Request") == "true":
        numbers, total_count = await service.number_repo.list_paginated(offset=0, limit=15)
        env_preview = await service.preview_env_import()
        return templates.TemplateResponse(
            request,
            "allowed_numbers.html",
            {
                "request": request,
                "admin": admin,
                "csrf_token": get_csrf_token(request),
                "numbers": numbers,
                "total_count": total_count,
                "page": 1,
                "total_pages": math.ceil(total_count / 15) if total_count > 0 else 1,
                "search": "",
                "env_preview": env_preview,
                "error": err,
                "success": "Mapping LID berhasil dihapus." if not err else None,
            },
        )
    return RedirectResponse(url="/admin/allowed-numbers", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/allowed-numbers/import-env", response_class=HTMLResponse)
async def import_env_mapping_route(
    request: Request,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    imported, skipped, conflicts = await service.execute_env_import()

    msg = f"Berhasil mengimpor {imported} mapping LID. ({skipped} dilewati)."
    if conflicts:
        msg += f" Konflik: {', '.join(conflicts)}"

    return RedirectResponse(url="/admin/allowed-numbers", status_code=status.HTTP_303_SEE_OTHER)


# Unmapped LIDs routes
@router.get("/unmapped-lids", response_class=HTMLResponse)
async def unmapped_lids_page(
    request: Request,
    search: str | None = Query(None),
    page: int = Query(1, ge=1),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> HTMLResponse:
    service = AllowedNumberService(db)
    limit = 15
    offset = (page - 1) * limit
    unmapped_records, total_count = await service.unmapped_repo.list_paginated(
        search=search, offset=offset, limit=limit
    )
    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
    allowed_numbers = await service.number_repo.list_numbers()

    return templates.TemplateResponse(
        request,
        "unmapped_lids.html",
        {
            "request": request,
            "admin": admin,
            "csrf_token": get_csrf_token(request),
            "unmapped_records": unmapped_records,
            "allowed_numbers": allowed_numbers,
            "total_count": total_count,
            "page": page,
            "total_pages": total_pages,
            "search": search or "",
            "error": None,
            "success": None,
        },
    )


@router.post("/unmapped-lids/{unmapped_id}/assign-existing", response_class=HTMLResponse)
async def assign_unmapped_to_existing(
    request: Request,
    unmapped_id: int,
    allowed_number_id: int = Form(...),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    res, err = await service.resolve_unmapped_to_existing(unmapped_id, allowed_number_id)
    return RedirectResponse(url="/admin/unmapped-lids", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/unmapped-lids/{unmapped_id}/assign-new", response_class=HTMLResponse)
async def assign_unmapped_to_new(
    request: Request,
    unmapped_id: int,
    name: str = Form(...),
    phone_number: str = Form(...),
    notes: str | None = Form(None),
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    res, err = await service.resolve_unmapped_to_new(unmapped_id, name, phone_number, notes)
    return RedirectResponse(url="/admin/unmapped-lids", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/unmapped-lids/{unmapped_id}/resolve", response_class=HTMLResponse)
async def resolve_unmapped_route(
    request: Request,
    unmapped_id: int,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    await service.unmapped_repo.resolve(unmapped_id)
    await db.commit()
    return RedirectResponse(url="/admin/unmapped-lids", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/unmapped-lids/{unmapped_id}/delete", response_class=HTMLResponse)
async def delete_unmapped_route(
    request: Request,
    unmapped_id: int,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
) -> Response:
    await require_csrf(request)
    service = AllowedNumberService(db)
    await service.unmapped_repo.delete_history(unmapped_id)
    await db.commit()
    return RedirectResponse(url="/admin/unmapped-lids", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/history", response_class=HTMLResponse)
async def history_page(
    request: Request,
    phone_number: str | None = Query(None),
    status_val: str | None = Query("all", alias="status"),
    content_type: str | None = Query("all"),
    platform: str | None = Query("all"),
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
        platform=platform,
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
            "filter_platform": platform or "all",
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
