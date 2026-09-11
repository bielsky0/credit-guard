"""FastAPI application for Loan Application Service."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress

from crediguard_observability import configure_logging, get_logger
from fastapi import FastAPI

from src.api.routes.loans import router as loans_router

configure_logging("loan-application-service")
logger = get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    import os

    from src.api.dependencies import close_redis, init_redis
    from src.infrastructure.database import close_database, init_database
    from src.infrastructure.kafka.producer import OutboxWorker

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://loan_svc:loan_dev_pw@localhost:5433/loan_db",
    )
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6380/0")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

    await init_database(database_url)
    init_redis(redis_url)
    logger.info("Database and Redis initialized")

    outbox_worker = OutboxWorker(
        bootstrap_servers=kafka_bootstrap,
        database_url=database_url,
    )
    await outbox_worker.start()
    worker_task = asyncio.create_task(outbox_worker.run())
    logger.info("Outbox worker started")

    logger.info("Loan Application Service started")
    yield

    await outbox_worker.stop()
    worker_task.cancel()
    with suppress(asyncio.CancelledError):
        await worker_task
    await close_redis()
    await close_database()
    logger.info("Loan Application Service stopped")


app = FastAPI(
    title="CrediGuard Loan Application Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(loans_router, prefix="/api/v1")


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}