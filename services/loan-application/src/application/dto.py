from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field

from src.domain.value_objects import LoanStatus


class CreateLoanApplicationRequest(BaseModel):
    amount: Decimal = Field(gt=0, le=200_000, description="Loan amount in PLN")
    term_months: int = Field(ge=3, le=60, description="Loan term in months")
    monthly_income: Decimal = Field(gt=0, description="Monthly income in PLN")
    applicant_age: int = Field(ge=18, le=75, description="Applicant age")


class LoanApplicationResponse(BaseModel):
    id: UUID
    applicant_id: UUID
    amount: Decimal
    term_months: int
    monthly_income: Decimal
    applicant_age: int
    status: LoanStatus
    decision_reasons: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class LoanApplicationListResponse(BaseModel):
    applications: list[LoanApplicationResponse]