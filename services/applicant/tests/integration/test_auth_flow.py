"""Integration tests for auth flow."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.main import app
from src.infrastructure.persistence.repository import (
    SQLAlchemyApplicantRepository,
    SQLAlchemyRefreshTokenRepository,
)
from src.infrastructure.security.argon2_hasher import Argon2Hasher
from src.infrastructure.security.jwt_service import JWTService
from src.application.use_cases.register import RegisterUseCase
from src.application.use_cases.login import LoginUseCase
from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.use_cases.get_me import GetMeUseCase
from src.application.dto import RegisterRequest, LoginRequest, RefreshTokenRequest


@pytest.mark.asyncio
async def test_full_auth_flow(session: AsyncSession):
    """Test complete auth flow: register -> login -> refresh -> me."""
    # Setup dependencies
    applicant_repo = SQLAlchemyApplicantRepository(session)
    refresh_token_repo = SQLAlchemyRefreshTokenRepository(session)
    password_hasher = Argon2Hasher()
    token_service = JWTService(
        private_key_path="/app/keys/private_key.pem",
        public_key_path="/app/keys/public_key.pem",
    )

    register_use_case = RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)
    login_use_case = LoginUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)
    refresh_use_case = RefreshTokenUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)
    get_me_use_case = GetMeUseCase(applicant_repo)

    # 1. Register
    register_request = RegisterRequest(
        email="integration@example.com",
        password="password123",
        first_name="Integration",
        last_name="Test",
    )
    register_result = await register_use_case.execute(register_request)

    assert register_result.access_token
    assert register_result.refresh_token
    assert register_result.token_type == "bearer"
    assert register_result.expires_in > 0

    # 2. Login
    login_request = LoginRequest(email="integration@example.com", password="password123")
    login_result = await login_use_case.execute(login_request)

    assert login_result.access_token
    assert login_result.refresh_token
    assert login_result.access_token != register_result.access_token  # New token

    # 3. Get me with access token
    # We need to decode the token to get applicant_id
    from jose import jwt
    payload = jwt.decode(
        login_result.access_token,
        token_service._public_key,
        algorithms=["RS256"],
    )
    applicant_id = payload["sub"]

    me_result = await get_me_use_case.execute(applicant_id)
    assert me_result.email == "integration@example.com"
    assert me_result.first_name == "Integration"
    assert me_result.last_name == "Test"

    # 4. Refresh token
    refresh_request = RefreshTokenRequest(refresh_token=login_result.refresh_token)
    refresh_result = await refresh_use_case.execute(refresh_request)

    assert refresh_result.access_token
    assert refresh_result.refresh_token
    assert refresh_result.access_token != login_result.access_token  # New access token
    assert refresh_result.refresh_token != login_result.refresh_token  # Rotated refresh token

    # 5. Get me with new access token
    payload = jwt.decode(
        refresh_result.access_token,
        token_service._public_key,
        algorithms=["RS256"],
    )
    applicant_id = payload["sub"]

    me_result_2 = await get_me_use_case.execute(applicant_id)
    assert me_result_2.email == "integration@example.com"


@pytest.mark.asyncio
async def test_duplicate_registration_fails(session: AsyncSession):
    """Test that duplicate email registration fails."""
    applicant_repo = SQLAlchemyApplicantRepository(session)
    refresh_token_repo = SQLAlchemyRefreshTokenRepository(session)
    password_hasher = Argon2Hasher()
    token_service = JWTService(
        private_key_path="/app/keys/private_key.pem",
        public_key_path="/app/keys/public_key.pem",
    )

    register_use_case = RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)

    request = RegisterRequest(
        email="duplicate@example.com",
        password="password123",
        first_name="First",
        last_name="User",
    )

    # First registration succeeds
    await register_use_case.execute(request)

    # Second registration fails
    with pytest.raises(Exception) as exc_info:
        await register_use_case.execute(request)

    assert "already registered" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_wrong_password_fails(session: AsyncSession):
    """Test that login with wrong password fails."""
    applicant_repo = SQLAlchemyApplicantRepository(session)
    refresh_token_repo = SQLAlchemyRefreshTokenRepository(session)
    password_hasher = Argon2Hasher()
    token_service = JWTService(
        private_key_path="/app/keys/private_key.pem",
        public_key_path="/app/keys/public_key.pem",
    )

    register_use_case = RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)
    login_use_case = LoginUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)

    # Register first
    await register_use_case.execute(RegisterRequest(
        email="wrongpass@example.com",
        password="correct_password",
        first_name="Wrong",
        last_name="Password",
    ))

    # Try login with wrong password
    with pytest.raises(Exception) as exc_info:
        await login_use_case.execute(LoginRequest(
            email="wrongpass@example.com",
            password="wrong_password",
        ))

    assert "invalid" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_refresh_token_rotation(session: AsyncSession):
    """Test that refresh tokens are rotated (old one revoked)."""
    applicant_repo = SQLAlchemyApplicantRepository(session)
    refresh_token_repo = SQLAlchemyRefreshTokenRepository(session)
    password_hasher = Argon2Hasher()
    token_service = JWTService(
        private_key_path="/app/keys/private_key.pem",
        public_key_path="/app/keys/public_key.pem",
    )

    register_use_case = RegisterUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)
    login_use_case = LoginUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)
    refresh_use_case = RefreshTokenUseCase(applicant_repo, refresh_token_repo, password_hasher, token_service)

    await register_use_case.execute(RegisterRequest(
        email="rotation@example.com",
        password="password123",
        first_name="Token",
        last_name="Rotation",
    ))

    login_result = await login_use_case.execute(LoginRequest(
        email="rotation@example.com",
        password="password123",
    ))

    # First refresh
    refresh_result_1 = await refresh_use_case.execute(RefreshTokenRequest(
        refresh_token=login_result.refresh_token,
    ))

    # Second refresh with same old token should fail (revoked)
    with pytest.raises(Exception) as exc_info:
        await refresh_use_case.execute(RefreshTokenRequest(
            refresh_token=login_result.refresh_token,
        ))

    assert "revoked" in str(exc_info.value).lower() or "invalid" in str(exc_info.value).lower()

    # But new refresh token works
    refresh_result_2 = await refresh_use_case.execute(RefreshTokenRequest(
        refresh_token=refresh_result_1.refresh_token,
    ))

    assert refresh_result_2.access_token
    assert refresh_result_2.refresh_token