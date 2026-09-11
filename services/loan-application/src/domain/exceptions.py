from __future__ import annotations

from uuid import UUID


class LoanApplicationDomainError(Exception):
    """Base exception for Loan Application domain errors."""


class LoanApplicationNotFound(LoanApplicationDomainError):
    def __init__(self, loan_id: UUID) -> None:
        self.loan_id = loan_id
        super().__init__(f"Loan application not found: {loan_id}")


class InvalidStatusTransition(LoanApplicationDomainError):
    def __init__(self, from_status: str, to_status: str) -> None:
        self.from_status = from_status
        self.to_status = to_status
        super().__init__(
            f"Invalid status transition: {from_status} -> {to_status}"
        )


class DuplicateActiveApplication(LoanApplicationDomainError):
    def __init__(self, applicant_id: UUID) -> None:
        self.applicant_id = applicant_id
        super().__init__(
            f"Applicant {applicant_id} already has an active loan application"
        )


class IdempotencyKeyAlreadyUsed(LoanApplicationDomainError):
    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__(f"Idempotency key already used: {idempotency_key}")