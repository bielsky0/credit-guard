"""Unit tests for LoginUseCase."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.application.use_cases.login import LoginUseCase
from src.application.dto import LoginRequest, TokenResponse
from src.domain.entities import Applicant
from src.domain.exceptions import InvalidCredentials


class TestLoginUseCase:
    @pytest.fixture
    def mock_applicant_repo(self) -> AsyncMock:
        repo = AsyncMock()
        return repo

    @pytest.fixture
    def mock_refresh_token_repo(self) -> AsyncMock:
        repo = AsyncMock()
        repo.save = AsyncMock()
        return repo

    @pytest.fixture
    def mock_password_hasher(self) -> MagicMock:
        hasher = MagicMock()
        hasher.verify = MagicMock(return_value=True)
        hasher.hash = MagicMock(return_value="hashed_refresh_token")
        return hasher

    @pytest.fixture
    def mock_token_service(self) -> MagicMock:
        service = MagicMock()
        service.create_access_token = MagicMock(return_value="access_token_123")
        service.create_refresh_token = MagicMock(return_value="refresh_token_123")
        service.get_access_token_expiry = MagicMock()
        service.get_refresh_token_expiry = MagicMock()
        return service

    @pytest.fixture
    def applicant(self) -> Applicant:
        return Applicant(
            id=uuid4(),
            email="test@example.com",
            password_hash="hashed_password",
            first_name="John",
            last_name="Doe",
        )

    @pytest.fixture
    def use_case(
        self,
        mock_applicant_repo: AsyncMock,
        mock_refresh_token_repo: AsyncMock,
        mock_password_hasher: MagicMock,
        mock_token_service: MagicMock,
    ) -> LoginUseCase:
        return LoginUseCase(
            mock_applicant_repo,
            mock_refresh_token_repo,
            mock_password_hasher,
            mock_token_service,
        )

    @pytest.mark.asyncio
    async def test_login_success(
        self,
        use_case: LoginUseCase,
        mock_applicant_repo: AsyncMock,
        mock_refresh_token_repo: AsyncMock,
        mock_password_hasher: MagicMock,
        mock_token_service: MagicMock,
        applicant: Applicant,
    ) -> None:
        mock_applicant_repo.get_by_email = AsyncMock(return_value=applicant)

        request = LoginRequest(email="test@example.com", password="password123")

        result = await use_case.execute(request)

        assert isinstance(result, TokenResponse)
        assert result.access_token == "access_token_123"
        assert result.refresh_token == "refresh_token_123"

        mock_applicant_repo.get_by_email.assert_called_once_with("test@example.com")
        mock_password_hasher.verify.assert_called_once_with("password123", "hashed_password")
        mock_refresh_token_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_login_user_not_found(
        self,
        use_case: LoginUseCase,
        mock_applicant_repo: AsyncMock,
    ) -> None:
        mock_applicant_repo.get_by_email = AsyncMock(return_value=None)

        request = LoginRequest(email="nonexistent@example.com", password="password123")

        with pytest.raises(InvalidCredentials):
            await use_case.execute(request)

    @pytest.mark.asyncio
    async def test_login_invalid_password(
        self,
        use_case: LoginUseCase,
        mock_applicant_repo: AsyncMock,
        mock_password_hasher: MagicMock,
        applicant: Applicant,
    ) -> None:
        mock_applicant_repo.get_by_email = AsyncMock(return_value=applicant)
        mock_password_hasher.verify = MagicMock(return_value=False)

        request = LoginRequest(email="test@example.com", password="wrong_password")

        with pytest.raises(InvalidCredentials):
            await use_case.execute(request)