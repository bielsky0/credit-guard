"""Database session management for Applicant Service."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool


class Database:
    def __init__(self, url: str) -> None:
        self._engine = create_async_engine(url, poolclass=NullPool)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory

    async def create_all(self) -> None:
        from src.infrastructure.persistence.models import Base
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()


_db: Optional[Database] = None


def get_database() -> Database:
    assert _db is not None, "Database not initialized"
    return _db


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database session."""
    db = get_database()
    async with db.session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_database(url: str) -> None:
    global _db
    _db = Database(url)
    await _db.create_all()


async def close_database() -> None:
    global _db
    if _db:
        await _db.close()
        _db = None