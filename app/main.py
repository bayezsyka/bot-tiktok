import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from starlette.middleware.sessions import SessionMiddleware

import app.lifespan as lifespan_module
from app.admin.router import router as admin_router
from app.auth.router import router as auth_router
from app.config import get_settings
from app.lifespan import lifespan
from app.media.cleanup import check_disk_space
from app.webhooks.router import router as webhooks_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

settings = get_settings()

app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)

app.add_middleware(
    SessionMiddleware,
    secret_key=settings.APP_SECRET,
    https_only=settings.SESSION_COOKIE_SECURE,
    same_site="lax",
    max_age=86400,
)

# Static files
static_path = Path(__file__).parent / "static"
static_path.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_path)), name="static")

# Routers
app.include_router(webhooks_router, prefix="/webhooks", tags=["webhooks"])
app.include_router(auth_router, prefix="/admin", tags=["auth"])
app.include_router(admin_router, prefix="/admin", tags=["admin"])


@app.get("/health", tags=["health"])
async def health_check() -> JSONResponse:
    from app.database.connection import get_engine

    db_status = "connected"
    try:
        engine = get_engine()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception:
        db_status = "error"

    worker_instance = lifespan_module.worker_instance
    worker_status = "running" if worker_instance and getattr(worker_instance, "is_running", False) else "stopped"
    queue_size = 0
    if worker_instance and hasattr(worker_instance, "get_queue_size"):
        queue_size = await worker_instance.get_queue_size()

    disk_free = check_disk_space()

    yt_dlp_status = "available" if shutil.which(settings.YT_DLP_BINARY) else "missing"
    ffmpeg_status = "available" if shutil.which(settings.FFMPEG_BINARY) else "missing"

    return JSONResponse(
        content={
            "status": "ok",
            "database": db_status,
            "worker": worker_status,
            "queue_size": queue_size,
            "disk_free_bytes": disk_free,
            "yt_dlp": yt_dlp_status,
            "ffmpeg": ffmpeg_status,
            "timestamp": datetime.now(UTC).isoformat(),
        }
    )
