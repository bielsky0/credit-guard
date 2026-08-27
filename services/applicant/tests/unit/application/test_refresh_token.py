"""Unit tests for RefreshTokenUseCase."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.application.use_cases.refresh_token import RefreshTokenUseCase
from src.application.dto import RefreshTokenRequest, TokenResponse
from src.domain.entities import Applicant, RefreshToken
from src.domain.exceptions import InvalidRefreshToken, RefreshTokenRevoked


class TestRefreshTokenUseCase:
    @pytest.fixture
    def mock_applicant_repo(self) -> AsyncMock:
        repo = AsyncMock()
        return repo

    @pytest.fixture
    def mock_refresh_token_repo(self) -> AsyncMock:
        repo = AsyncMock()
        return repo

    @pytest.fixture
    def mock_password_hasher(self) -> MagicMock:
        hasher = MagicMock()
        hasher.verify = MagicMock(return_value=True)
        hasher.hash = MagicMock(return_value="new_hashed_refresh_token")
        return hasher

    @pytest.fixture
    def mock_token_service(self) -> MagicMock:
        service = MagicMock()
        service.create_access_token = MagicMock(return_value="new_access_token_123")
        service.create_refresh_token = MagicMock(return_value="new_refresh_token_123")
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
    def valid_refresh_token(self, applicant: Applicant) -> RefreshToken:
        return RefreshToken(
            applicant_id=applicant.id,
            token_hash="hashed_refresh_token",
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )

    @pytest.fixture
    def use_case(
        self,
        mock_applicant_repo: AsyncMock,
        mock_refresh_token_repo: AsyncMock,
        mock_password_hasher: MagicMock,
        mock_token_service: MagicMock,
    ) -> RefreshTokenUseCase:
        return RefreshTokenUseCase(
            mock_applicant_repo,
            mock_refresh_token_repo,
            mock_password_hasher,
            mock_token_service,
        )

    @pytest.mark.asyncio
    async def test_refresh_success(
        self,
        use_case: RefreshTokenUseCase,
        mock_applicant_repo: AsyncMock,
        mock_refresh_token_repo: AsyncMock,
        mock_password_hasher: MagicMock,
        mock_token_service: MagicMock,
        applicant: Applicant,
        valid_refresh_token: RefreshToken,
    ) -> None:
        mock_refresh_token_repo.get_all_valid = AsyncMock(return_value=[valid_refresh_token])
        mock_applicant_repo.get_by_id = AsyncMock(return_value=applicant)

        request = RefreshTokenRequest(refresh_token="valid_refresh_token")

        result = await use_case.execute(request)

        assert isinstance(result, TokenResponse)
        assert result.access_token == "new_access_token_123"
        assert result.refresh_token == "new_refresh_token_123"

        mock_refresh_token_repo.get_all_valid.assert_called_once()
        mock_password_hasher.verify.assert_called_once_with("valid_refresh_token", "hashed_refresh_token")
        assert valid_refresh_token.revoked_at is not None
        mock_applicant_repo.get_by_id.assert_called_once_with(applicant.id)
        mock_refresh_token_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_token_not_found(
        self,
        use_case: RefreshTokenUseCase,
        mock_refresh_token_repo: AsyncMock,
    ) -> None:
        mock_refresh_token_repo.get_all_valid = AsyncMock(return_value=[])

        request = RefreshTokenRequest(refresh_token="invalid_token")

        with pytest.raises(InvalidRefreshToken):
            await use_case.execute(request)

    @pytest.mark.asyncio
    async def test_refresh_token_expired(
        self,
        use_case: RefreshTokenUseCase,
        mock_refresh_token_repo: AsyncMock,
    ) -> None:
        expired_token = RefreshToken(
            applicant_id=uuid4(),
            token_hash="hashed_token",
            expires_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
        mock_refresh_token_repo.get_all_valid = AsyncMock(return_value=[expired_token])

        request = RefreshTokenRequest(refresh_token="expired_token")

        with pytest.raises(RefreshTokenRevoked):
            await use_case.execute(request)

    @pytest.mark.asyncio
    async def test_refresh_token_already_revoked(
        self,
        use_case: RefreshTokenUseCase,
        mock_refresh_token_repo: AsyncMock,
    ) -> None:
        revoked_token = RefreshToken(
            applicant_id=uuid4(),
            token_hash="hashed_token",
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )
        revoked_token.revoke()
        mock_refresh_token_repo.get_all_valid = AsyncMock(return_value=[revoked_token])

        request = RefreshTokenRequest(refresh_token="revoked_token")

        with pytest.raises(RefreshTokenRevoked):
            await use_case.execute(request)

    @pytest.mark.asyncio
    async def test_refresh_applicant_not_found(
        self,
        use_case: RefreshTokenUseCase,
        mock_applicant_repo: AsyncMock,
        mock_refresh_token_repo: AsyncMock,
        valid_refresh_token: RefreshToken,
    ) -> None:
        mock_refresh_token_repo.get_all_valid = AsyncMock(return_value=[valid_refresh_token])
        mock_applicant_repo.get_by_id = AsyncMock(return_value=None)

        request = RefreshTokenRequest(refresh_token="valid_token")

        with pytest.raises(InvalidRefreshToken):
            await use_case.execute(request)