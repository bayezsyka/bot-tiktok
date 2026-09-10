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

        # Migration 3: Add gateway fields to download_jobs and download_items
        result = await conn.execute(text("PRAGMA table_info(download_items);"))
        item_columns = [row[1] for row in result.fetchall()]
        if "gateway_queue_status" not in item_columns:
            logger.info("Migrating schema: Adding gateway status fields to download_items")
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_queue_status VARCHAR(30);"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_delivery_status VARCHAR(30);"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_error_code VARCHAR(50);"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_error_message TEXT;"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_accepted_at DATETIME;"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_sent_at DATETIME;"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_delivered_at DATETIME;"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_read_at DATETIME;"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN gateway_failed_at DATETIME;"))
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN last_gateway_sync_at DATETIME;"))
        if "pending_since_at" not in item_columns:
            logger.info("Migrating schema: Adding pending_since_at column to download_items")
            await conn.execute(text("ALTER TABLE download_items ADD COLUMN pending_since_at DATETIME;"))

        result = await conn.execute(text("PRAGMA table_info(download_jobs);"))
        job_columns = [row[1] for row in result.fetchall()]
        if "gateway_status_summary" not in job_columns:
            logger.info("Migrating schema: Adding gateway status fields to download_jobs")
            await conn.execute(text("ALTER TABLE download_jobs ADD COLUMN gateway_status_summary VARCHAR(50);"))
            await conn.execute(text("ALTER TABLE download_jobs ADD COLUMN last_gateway_sync_at DATETIME;"))
            await conn.execute(text("ALTER TABLE download_jobs ADD COLUMN failure_notification_sent_at DATETIME;"))

        # Migration 6: Add selected_mode and music_url to download_jobs
        result = await conn.execute(text("PRAGMA table_info(download_jobs);"))
        job_cols = [row[1] for row in result.fetchall()]
        if "selected_mode" not in job_cols:
            logger.info("Migrating schema: Adding selected_mode and music_url to download_jobs")
            await conn.execute(text("ALTER TABLE download_jobs ADD COLUMN selected_mode VARCHAR(20);"))
            await conn.execute(text("ALTER TABLE download_jobs ADD COLUMN music_url TEXT;"))

        # Migration 5: Add index for gateway_message_id
        result = await conn.execute(text("PRAGMA index_list(download_items);"))
        indexes = [row[1] for row in result.fetchall()]
        if "ix_download_items_gateway_message_id" not in indexes:
            logger.info("Migrating schema: Adding index ix_download_items_gateway_message_id")
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS ix_download_items_gateway_message_id ON download_items(gateway_message_id);")
            )

        # Track migration versions in schema_migrations table
        migrations = [
            "001_add_lid_to_allowed_numbers",
            "002_create_unmapped_lids_table",
            "003_add_platform_to_download_jobs",
            "004_add_gateway_delivery_fields",
            "005_add_gateway_message_id_index",
            "006_add_pending_since_at",
            "007_add_selected_mode_and_music_url",
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
