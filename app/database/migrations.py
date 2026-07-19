from sqlalchemy.ext.asyncio import AsyncEngine

from app.database.models import Base


async def init_db(engine: AsyncEngine) -> None:
    """Initialize database tables using SQLAlchemy metadata."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def run_migrations() -> None:
    """Helper to run migrations using the default engine."""
    from app.database.connection import get_engine
    await init_db(get_engine())
