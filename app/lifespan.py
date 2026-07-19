import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from app.config import get_settings
from app.database.connection import get_engine, get_session_maker
from app.database.migrations import init_db

logger = logging.getLogger(__name__)

worker_instance: Any = None


async def periodic_cleanup_task() -> None:
    from app.media.cleanup import cleanup_expired_temp_files
    settings = get_settings()
    while True:
        try:
            await asyncio.sleep(1800)  # 30 minutes
            cleanup_expired_temp_files(settings.TEMP_FILE_TTL_MINUTES)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error during periodic cleanup: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global worker_instance
    settings = get_settings()

    # Validate config and create directories
    settings.validate_startup_configuration()

    # Initialize database schema
    engine = get_engine()
    await init_db(engine)

    # Initial cleanup on startup
    from app.media.cleanup import cleanup_expired_temp_files
    cleanup_expired_temp_files(settings.TEMP_FILE_TTL_MINUTES)

    # Recover incomplete jobs after restart
    from app.queue.recovery import recover_incomplete_jobs
    session_maker = get_session_maker()
    async with session_maker() as session:
        await recover_incomplete_jobs(session)
        await session.commit()

    # Start worker task
    from app.queue.worker import QueueWorker
    worker_instance = QueueWorker(session_maker)
    worker_task = asyncio.create_task(worker_instance.run())
    cleanup_task = asyncio.create_task(periodic_cleanup_task())

    try:
        yield
    finally:
        if worker_instance:
            worker_instance.stop()
        worker_task.cancel()
        cleanup_task.cancel()
        try:
            await asyncio.gather(worker_task, cleanup_task, return_exceptions=True)
        except Exception:
            pass
        await engine.dispose()
