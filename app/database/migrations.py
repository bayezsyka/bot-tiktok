import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.database.models import Base

logger = logging.getLogger(__name__)


async def init_db(engine: AsyncEngine) -> None:
    """Initialize database tables using SQLAlchemy metadata and run custom DDL migrations safely."""
    async with engine.begin() as conn:
        # Create missing tables defined in Base metadata
        await conn.run_sync(Base.metadata.create_all)

        # Migration 1: Check and add lid_number column to allowed_numbers
        result = await conn.execute(text("PRAGMA table_info(allowed_numbers);"))
        columns = [row[1] for row in result.fetchall()]
        if "lid_number" not in columns:
            logger.info("Migrating schema: Adding lid_number column to allowed_numbers")
            await conn.execute(text("ALTER TABLE allowed_numbers ADD COLUMN lid_number VARCHAR(50);"))
            await conn.execute(
                text("CREATE UNIQUE INDEX IF NOT EXISTS ix_allowed_numbers_lid_number ON allowed_numbers (lid_number);")
            )

        # Migration 2: Check and add platform column to download_jobs
        result = await conn.execute(text("PRAGMA table_info(download_jobs);"))
        job_columns = [row[1] for row in result.fetchall()]
        if "platform" not in job_columns:
            logger.info("Migrating schema: Adding platform column to download_jobs")
            await conn.execute(text("ALTER TABLE download_jobs ADD COLUMN platform VARCHAR(20) NOT NULL DEFAULT 'tiktok';"))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_download_jobs_platform ON download_jobs (platform);")
            )
            await conn.execute(text("UPDATE download_jobs SET platform = 'tiktok' WHERE platform IS NULL OR platform = '';"))

        # Track migration versions in schema_migrations table
        migrations = [
            "001_add_lid_to_allowed_numbers",
            "002_create_unmapped_lids_table",
            "003_add_platform_to_download_jobs",
        ]
        for version in migrations:
            await conn.execute(
                text(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (:v, datetime('now')) "
                    "ON CONFLICT(version) DO NOTHING;"
                ),
                {"v": version},
            )


async def run_migrations() -> None:
    """Helper to run migrations using the default engine."""
    from app.database.connection import get_engine
    await init_db(get_engine())
