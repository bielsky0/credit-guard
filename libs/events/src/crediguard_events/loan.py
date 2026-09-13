"""Loan-application Kafka payload schemas (spec §5.1-5.2).

Single source of truth for event contracts. Producers (loan-application
outbox worker) and consumers (notification, document, underwriting, ...)
import from here instead of duplicating ad-hoc dicts.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field

LOAN_APPLICATION_SUBMITTED_V1 = "loan.application.submitted.v1"
LOAN_STATUS_CHANGED_V1 = "loan.status.changed.v1"


class LoanApplicationSubmittedV1(BaseModel):
    loan_id: UUID
    applicant_id: UUID
    amount: str
    term_months: int
    monthly_income: str
    applicant_age: int


class LoanStatusChangedV1(BaseModel):
    loan_id: UUID
    applicant_id: UUID
    old_status: str
    new_status: str
    decision_reasons: list[str] | None = Field(default=None)
