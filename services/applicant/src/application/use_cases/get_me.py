"""GetMe use case for Applicant Service."""

from __future__ import annotations

from uuid import UUID

from src.application.dto import ApplicantResponse
from src.application.ports.repository import ApplicantRepository
from src.domain.exceptions import ApplicantNotFound


class GetMeUseCase:
    """Use case for getting current applicant profile."""

    def __init__(self, applicant_repo: ApplicantRepository) -> None:
        self._applicant_repo = applicant_repo

    async def execute(self, applicant_id: UUID) -> ApplicantResponse:
        """Execute get me."""
        applicant = await self._applicant_repo.get_by_id(applicant_id)
        if applicant is None:
            raise ApplicantNotFound(str(applicant_id))

        return ApplicantResponse(
            id=applicant.id,
            email=applicant.email,
            first_name=applicant.first_name,
            last_name=applicant.last_name,
            created_at=applicant.created_at,
        )