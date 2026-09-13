"""API Gateway FastAPI application (spec §4.1).

Stateless by design: no database. Validates JWT with the public key, rate-limits
with Redis, and reverse-proxies to internal services over the closed network.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from crediguard_observability import CorrelationIdMiddleware, configure_logging, get_logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.proxy_router import close_gateway, init_gateway
from src.api.proxy_router import router as proxy_router
from src.api.sse import init_sse
from src.api.sse import router as sse_router
from src.core.config import get_settings
from src.infrastructure.redis import close_redis, get_redis
from src.services.token import TokenValidator

configure_logging("gateway")
logger = get_logger()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    redis = await get_redis(settings.redis_url)
    init_gateway(settings, redis)
    init_sse(settings, TokenValidator(settings.jwt_public_key), redis)
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

# CORS with credentials: browsers on SSE (EventSource withCredentials) and
# credentialed fetch need an explicit origin (never "*") to receive cookies.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Correlation-ID", "Idempotency-Key"],
    expose_headers=["X-Correlation-ID"],
)

app.add_middleware(CorrelationIdMiddleware)
# SSE before the catch-all proxy so /loans/{id}/events is handled locally.
app.include_router(sse_router)
app.include_router(proxy_router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready", tags=["health"])
async def ready() -> dict[str, str]:
    return {"status": "ready"}
