from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from src.application.use_cases.get_loan_application import GetLoanApplicationUseCase
from src.domain.entities import LoanApplication
from src.domain.exceptions import LoanApplicationNotFound


class FakeLoanRepo:
    def __init__(self) -> None:
        self.loans: dict[str, LoanApplication] = {}

    async def get_by_id(self, loan_id: Any) -> LoanApplication | None:
        return self.loans.get(str(loan_id))


@pytest.mark.asyncio
async def test_get_loan_success() -> None:
    repo = FakeLoanRepo()
    loan = LoanApplication(
        applicant_id=uuid4(),
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )
    repo.loans[str(loan.id)] = loan

    use_case = GetLoanApplicationUseCase(repo)  # type: ignore[arg-type]
    result = await use_case.execute(loan.id, loan.applicant_id)

    assert result.id == loan.id
    assert result.applicant_id == loan.applicant_id


@pytest.mark.asyncio
async def test_get_loan_not_found() -> None:
    repo = FakeLoanRepo()
    use_case = GetLoanApplicationUseCase(repo)  # type: ignore[arg-type]

    with pytest.raises(LoanApplicationNotFound):
        await use_case.execute(uuid4(), uuid4())


@pytest.mark.asyncio
async def test_get_loan_wrong_applicant() -> None:
    repo = FakeLoanRepo()
    loan = LoanApplication(
        applicant_id=uuid4(),
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )
    repo.loans[str(loan.id)] = loan

    use_case = GetLoanApplicationUseCase(repo)  # type: ignore[arg-type]

    with pytest.raises(LoanApplicationNotFound):
        await use_case.execute(loan.id, uuid4())