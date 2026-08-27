"""FastAPI dependencies for Applicant Service."""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.application.ports.token_service import TokenService
from src.application.use_cases.get_me import GetMeUseCase
from src.application.use_cases.login import LoginUseCase
from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.use_cases.register import RegisterUseCase
from src.infrastructure.database import get_session
from src.infrastructure.persistence.repository import (
    SQLAlchemyApplicantRepository,
    SQLAlchemyRefreshTokenRepository,
)
from src.infrastructure.security.argon2_hasher import Argon2Hasher
from src.infrastructure.security.jwt_service import JWTService

# Global instances (initialized in main.py)
_token_service: Optional[TokenService] = None
_argon2_hasher: Optional[Argon2Hasher] = None


def get_token_service() -> TokenService:
    global _token_service
    if _token_service is None:
        _token_service = JWTService()
    return _token_service


def get_password_hasher() -> Argon2Hasher:
    global _argon2_hasher
    if _argon2_hasher is None:
        _argon2_hasher = Argon2Hasher()
    return _argon2_hasher


def get_applicant_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyApplicantRepository:
    return SQLAlchemyApplicantRepository(session)


def get_refresh_token_repo(
    session: AsyncSession = Depends(get_session),
) -> SQLAlchemyRefreshTokenRepository:
    return SQLAlchemyRefreshTokenRepository(session)


def get_register_use_case(
    applicant_repo: SQLAlchemyApplicantRepository = Depends(get_applicant_repo),
    refresh_token_repo: SQLAlchemyRefreshTokenRepository = Depends(get_refresh_token_repo),
    password_hasher: Argon2Hasher = Depends(get_password_hasher),
    token_service: TokenService = Depends(get_token_service),
) -> RegisterUseCase:
    return RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)


def get_login_use_case(
    applicant_repo: SQLAlchemyApplicantRepository = Depends(get_applicant_repo),
    refresh_token_repo: SQLAlchemyRefreshTokenRepository = Depends(get_refresh_token_repo),
    password_hasher: Argon2Hasher = Depends(get_password_hasher),
    token_service: TokenService = Depends(get_token_service),
) -> LoginUseCase:
    return LoginUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)


def get_refresh_token_use_case(
    applicant_repo: SQLAlchemyApplicantRepository = Depends(get_applicant_repo),
    refresh_token_repo: SQLAlchemyRefreshTokenRepository = Depends(get_refresh_token_repo),
    password_hasher: Argon2Hasher = Depends(get_password_hasher),
    token_service: TokenService = Depends(get_token_service),
) -> RefreshTokenUseCase:
    return RefreshTokenUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)


def get_get_me_use_case(
    applicant_repo: SQLAlchemyApplicantRepository = Depends(get_applicant_repo),
) -> GetMeUseCase:
    return GetMeUseCase(applicant_repo)


async def get_current_applicant_id(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    token_service: TokenService = Depends(get_token_service),
) -> UUID:
    """Extract and validate applicant ID from Bearer token."""
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid Authorization header",
        )

    token = authorization[7:]  # Remove "Bearer "
    try:
        applicant_id, _ = token_service.decode_access_token(token)
        return applicant_id
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {e}",
        )