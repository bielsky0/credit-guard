from __future__ import annotations

from typing import Optional
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.idempotency import IdempotencyStore
from src.application.use_cases.create_loan_application import CreateLoanApplicationUseCase
from src.application.use_cases.get_loan_application import GetLoanApplicationUseCase
from src.infrastructure.database import get_session
from src.infrastructure.persistence.repository import (
    SQLAlchemyLoanApplicationRepository,
    SQLAlchemyOutboxRepository,
)
from src.infrastructure.redis.idempotency import RedisIdempotencyStore

_redis_client: Optional[aioredis.Redis] = None
_idempotency_store: Optional[IdempotencyStore] = None


def get_redis_client() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        raise RuntimeError("Redis not initialized")
    return _redis_client


def get_idempotency_store() -> IdempotencyStore:
    global _idempotency_store
    if _idempotency_store is None:
        _idempotency_store = RedisIdempotencyStore(get_redis_client())
    return _idempotency_store


def init_redis(redis_url: str) -> None:
    global _redis_client
    _redis_client = aioredis.from_url(redis_url, decode_responses=True)


async def close_redis() -> None:
    global _redis_client, _idempotency_store
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
        _idempotency_store = None


def get_loan_application_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyLoanApplicationRepository:
    return SQLAlchemyLoanApplicationRepository(session)


def get_outbox_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyOutboxRepository:
    return SQLAlchemyOutboxRepository(session)


def get_create_loan_application_use_case(
    loan_repo: SQLAlchemyLoanApplicationRepository = Depends(get_loan_application_repo),
    outbox_repo: SQLAlchemyOutboxRepository = Depends(get_outbox_repo),
    idempotency_store: IdempotencyStore = Depends(get_idempotency_store),
) -> CreateLoanApplicationUseCase:
    return CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency_store)


def get_get_loan_application_use_case(
    loan_repo: SQLAlchemyLoanApplicationRepository = Depends(get_loan_application_repo),
) -> GetLoanApplicationUseCase:
    return GetLoanApplicationUseCase(loan_repo)


async def get_current_applicant_id(
    x_user_id: Optional[str] = Header(None, alias="X-User-ID"),
) -> UUID:
    if x_user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-User-ID header",
        )
    try:
        return UUID(x_user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid X-User-ID header",
        )