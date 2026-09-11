from __future__ import annotations

from uuid import UUID

from src.application.dto import LoanApplicationResponse
from src.application.ports.repository import LoanApplicationRepository
from src.domain.exceptions import LoanApplicationNotFound


class GetLoanApplicationUseCase:
    def __init__(self, loan_repo: LoanApplicationRepository) -> None:
        self._loan_repo = loan_repo

    async def execute(
        self, loan_id: UUID, applicant_id: UUID
    ) -> LoanApplicationResponse:
        loan = await self._loan_repo.get_by_id(loan_id)
        if loan is None:
            raise LoanApplicationNotFound(loan_id)
        if loan.applicant_id != applicant_id:
            raise LoanApplicationNotFound(loan_id)
        return LoanApplicationResponse.model_validate(loan)