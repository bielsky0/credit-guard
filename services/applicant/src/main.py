"""FastAPI application for Applicant Service."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from crediguard_observability import configure_logging, get_logger
from fastapi import FastAPI
from src.api.routes.auth import router as auth_router
from src.api.routes.me import router as me_router
from src.infrastructure.database import close_database, init_database

# Configure structured logging
configure_logging("applicant-service")
logger = get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    import os

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://applicant_svc:applicant_dev_pw@localhost:5433/applicant_db",
    )
    await init_database(database_url)
    logger.info("Applicant service started", database_url=database_url)
    yield
    await close_database()
    logger.info("Applicant service stopped")


app = FastAPI(
    title="CrediGuard Applicant Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(auth_router, prefix="/api/v1")
app.include_router(me_router, prefix="/api/v1")


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}