from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Header

from src.api.dependencies import (
    get_create_loan_application_use_case,
    get_current_applicant_id,
    get_get_loan_application_use_case,
)
from src.application.dto import CreateLoanApplicationRequest, LoanApplicationResponse
from src.application.use_cases.create_loan_application import CreateLoanApplicationUseCase
from src.application.use_cases.get_loan_application import GetLoanApplicationUseCase

router: APIRouter = APIRouter(prefix="/loans", tags=["loans"])


@router.post(
    "",
    response_model=LoanApplicationResponse,
    status_code=202,
    summary="Submit a new loan application",
)
async def create_loan_application(
    request: CreateLoanApplicationRequest,
    applicant_id: UUID = Depends(get_current_applicant_id),
    use_case: CreateLoanApplicationUseCase = Depends(get_create_loan_application_use_case),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> LoanApplicationResponse:
    return await use_case.execute(applicant_id, request, idempotency_key)


@router.get(
    "/{loan_id}",
    response_model=LoanApplicationResponse,
    summary="Get a loan application by ID",
)
async def get_loan_application(
    loan_id: UUID,
    applicant_id: UUID = Depends(get_current_applicant_id),
    use_case: GetLoanApplicationUseCase = Depends(get_get_loan_application_use_case),
) -> LoanApplicationResponse:
    return await use_case.execute(loan_id, applicant_id)