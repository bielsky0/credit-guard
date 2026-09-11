"""API Gateway FastAPI application (spec §4.1).

Stateless by design: no database. Validates JWT with the public key, rate-limits
with Redis, and reverse-proxies to internal services over the closed network.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from crediguard_observability import CorrelationIdMiddleware, configure_logging, get_logger
from fastapi import FastAPI

from src.api.proxy_router import close_gateway, init_gateway
from src.api.proxy_router import router as proxy_router
from src.core.config import get_settings
from src.infrastructure.redis import close_redis, get_redis

configure_logging("gateway")
logger = get_logger()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    redis = await get_redis(settings.redis_url)
    init_gateway(settings, redis)
    logger.info(
        "Gateway started",
        applicant_url=settings.applicant_service_url,
        loan_url=settings.loan_service_url,
    )
    yield
    await close_gateway()
    await close_redis()
    logger.info("Gateway stopped")


app = FastAPI(
    title="CrediGuard API Gateway",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(CorrelationIdMiddleware)
app.include_router(proxy_router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}
