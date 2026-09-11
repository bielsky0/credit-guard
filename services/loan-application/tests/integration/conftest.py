"""Integration test configuration with testcontainers."""

from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from src.infrastructure.persistence.models import Base
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7-alpine") as redis:
        yield redis


@pytest.fixture(scope="session")
def database_url(postgres_container):
    return postgres_container.get_connection_url(driver="asyncpg")


@pytest.fixture(scope="session")
def redis_url(redis_container):
    return redis_container.get_connection_url()


@pytest_asyncio.fixture(scope="session")
async def engine(database_url):
    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session(engine):
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session