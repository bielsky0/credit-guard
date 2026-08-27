"""Integration test configuration with testcontainers."""

from __future__ import annotations

import pytest
from testcontainers.postgres import PostgresContainer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from src.infrastructure.persistence.models import Base


@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container for the test session."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def database_url(postgres_container):
    """Get database URL from container."""
    return postgres_container.get_connection_url(driver="asyncpg")


@pytest.fixture(scope="session")
async def engine(database_url):
    """Create async engine."""
    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session(engine):
    """Create async session for each test."""
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session