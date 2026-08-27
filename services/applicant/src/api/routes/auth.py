"""Auth routes for Applicant Service."""

from __future__ import annotations

from fastapi import APIRouter, Depends, status
from fastapi.routing import APIRouter as APIRouterType
from src.api.dependencies import (
    get_login_use_case,
    get_refresh_token_use_case,
    get_register_use_case,
)
from src.application.dto import LoginRequest, RefreshTokenRequest, RegisterRequest, TokenResponse
from src.application.use_cases.login import LoginUseCase
from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.use_cases.register import RegisterUseCase

router: APIRouterType = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new applicant",
)
async def register(
    request: RegisterRequest,
    use_case: RegisterUseCase = Depends(get_register_use_case),
) -> TokenResponse:
    """Register a new applicant and return access + refresh tokens."""
    return await use_case.execute(request)


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Login and get access + refresh tokens",
)
async def login(
    request: LoginRequest,
    use_case: LoginUseCase = Depends(get_login_use_case),
) -> TokenResponse:
    """Login with email and password, return access + refresh tokens."""
    return await use_case.execute(request)


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Refresh access token using refresh token",
)
async def refresh_token(
    request: RefreshTokenRequest,
    use_case: RefreshTokenUseCase = Depends(get_refresh_token_use_case),
) -> TokenResponse:
    """Rotate refresh token and return new access + refresh tokens."""
    return await use_case.execute(request)