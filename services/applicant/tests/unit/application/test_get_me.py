"""Unit tests for GetMeUseCase."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from src.application.use_cases.get_me import GetMeUseCase
from src.application.dto import ApplicantResponse
from src.domain.entities import Applicant
from src.domain.exceptions import ApplicantNotFound


class TestGetMeUseCase:
    @pytest.fixture
    def mock_applicant_repo(self) -> AsyncMock:
        return AsyncMock()

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
    def use_case(self, mock_applicant_repo: AsyncMock) -> GetMeUseCase:
        return GetMeUseCase(mock_applicant_repo)

    @pytest.mark.asyncio
    async def test_get_me_success(
        self,
        use_case: GetMeUseCase,
        mock_applicant_repo: AsyncMock,
        applicant: Applicant,
    ) -> None:
        mock_applicant_repo.get_by_id = AsyncMock(return_value=applicant)

        result = await use_case.execute(applicant.id)

        assert isinstance(result, ApplicantResponse)
        assert result.id == applicant.id
        assert result.email == applicant.email
        assert result.first_name == applicant.first_name
        assert result.last_name == applicant.last_name
        assert result.created_at == applicant.created_at

        mock_applicant_repo.get_by_id.assert_called_once_with(applicant.id)

    @pytest.mark.asyncio
    async def test_get_me_not_found(
        self,
        use_case: GetMeUseCase,
        mock_applicant_repo: AsyncMock,
    ) -> None:
        applicant_id = uuid4()
        mock_applicant_repo.get_by_id = AsyncMock(return_value=None)

        with pytest.raises(ApplicantNotFound) as exc_info:
            await use_case.execute(applicant_id)

        assert exc_info.value.applicant_id == str(applicant_id)