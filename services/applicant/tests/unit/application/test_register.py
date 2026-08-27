"""Unit tests for RegisterUseCase."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.application.use_cases.register import RegisterUseCase
from src.application.dto import RegisterRequest, TokenResponse
from src.domain.entities import Applicant
from src.domain.exceptions import EmailAlreadyRegistered


class TestRegisterUseCase:
    @pytest.fixture
    def mock_applicant_repo(self) -> AsyncMock:
        repo = AsyncMock()
        repo.exists_by_email = AsyncMock(return_value=False)
        repo.save = AsyncMock()
        return repo

    @pytest.fixture
    def mock_refresh_token_repo(self) -> AsyncMock:
        repo = AsyncMock()
        repo.save = AsyncMock()
        return repo

    @pytest.fixture
    def mock_password_hasher(self) -> MagicMock:
        hasher = MagicMock()
        hasher.hash = MagicMock(return_value="hashed_password")
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
    def use_case(
        self,
        mock_applicant_repo: AsyncMock,
        mock_refresh_token_repo: AsyncMock,
        mock_password_hasher: MagicMock,
        mock_token_service: MagicMock,
    ) -> RegisterUseCase:
        return RegisterUseCase(
            mock_applicant_repo,
            mock_refresh_token_repo,
            mock_password_hasher,
            mock_token_service,
        )

    @pytest.mark.asyncio
    async def test_register_success(
        self,
        use_case: RegisterUseCase,
        mock_applicant_repo: AsyncMock,
        mock_refresh_token_repo: AsyncMock,
        mock_password_hasher: MagicMock,
        mock_token_service: MagicMock,
    ) -> None:
        request = RegisterRequest(
            email="test@example.com",
            password="password123",
            first_name="John",
            last_name="Doe",
        )

        result = await use_case.execute(request)

        assert isinstance(result, TokenResponse)
        assert result.access_token == "access_token_123"
        assert result.refresh_token == "refresh_token_123"
        assert result.token_type == "bearer"

        mock_applicant_repo.exists_by_email.assert_called_once_with("test@example.com")
        # hash is called twice: once for password, once for refresh token
        assert mock_password_hasher.hash.call_count == 2
        mock_password_hasher.hash.assert_any_call("password123")
        mock_applicant_repo.save.assert_called_once()
        mock_token_service.create_access_token.assert_called_once()
        mock_token_service.create_refresh_token.assert_called_once()
        mock_refresh_token_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_register_email_already_exists(
        self,
        use_case: RegisterUseCase,
        mock_applicant_repo: AsyncMock,
    ) -> None:
        mock_applicant_repo.exists_by_email = AsyncMock(return_value=True)

        request = RegisterRequest(
            email="existing@example.com",
            password="password123",
            first_name="John",
            last_name="Doe",
        )

        with pytest.raises(EmailAlreadyRegistered) as exc_info:
            await use_case.execute(request)

        assert exc_info.value.email == "existing@example.com"
        mock_applicant_repo.save.assert_not_called()