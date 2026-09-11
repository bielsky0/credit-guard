from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from src.domain.exceptions import InvalidStatusTransition
from src.domain.value_objects import VALID_TRANSITIONS, LoanStatus


@dataclass
class LoanApplication:
    applicant_id: UUID
    amount: Decimal
    term_months: int
    monthly_income: Decimal
    applicant_age: int
    id: UUID = field(default_factory=uuid4)
    status: LoanStatus = LoanStatus.DRAFT
    decision_reasons: list[str] = field(default_factory=list)
    idempotency_key: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def _transition(self, new_status: LoanStatus) -> None:
        allowed = VALID_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise InvalidStatusTransition(self.status.value, new_status.value)
        self.status = new_status
        self.updated_at = datetime.now(UTC)

    def submit(self) -> None:
        self._transition(LoanStatus.SUBMITTED)

    def start_doc_verification(self) -> None:
        self._transition(LoanStatus.DOC_VERIFICATION)

    def reject_doc(self) -> None:
        self._transition(LoanStatus.DOC_REJECTED)

    def start_underwriting(self) -> None:
        self._transition(LoanStatus.UNDERWRITING)

    def approve(self) -> None:
        self._transition(LoanStatus.APPROVED)

    def reject(self, reasons: list[str] | None = None) -> None:
        self._transition(LoanStatus.REJECTED)
        if reasons:
            self.decision_reasons = reasons

    def start_disbursing(self) -> None:
        self._transition(LoanStatus.DISBURSING)

    def complete_disbursement(self) -> None:
        self._transition(LoanStatus.DISBURSED)

    def fail_disbursement(self) -> None:
        self._transition(LoanStatus.DISBURSEMENT_FAILED)

    @property
    def is_active(self) -> bool:
        return self.status not in {
            LoanStatus.REJECTED,
            LoanStatus.DISBURSED,
            LoanStatus.DOC_REJECTED,
            LoanStatus.DISBURSEMENT_FAILED,
        }

    @property
    def monthly_payment(self) -> Decimal:
        if self.term_months == 0:
            return Decimal("0")
        monthly_rate = Decimal("0.01")  # 12% annual / 12 months
        if monthly_rate == 0:
            return self.amount / self.term_months
        factor = (1 + monthly_rate) ** self.term_months
        return self.amount * (monthly_rate * factor) / (factor - 1)


@dataclass
class OutboxEvent:
    aggregate_id: UUID
    event_type: str
    payload: dict[str, Any]
    id: UUID = field(default_factory=uuid4)
    status: str = "PENDING"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    sent_at: datetime | None = None


@dataclass
class ProcessedEvent:
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    id: UUID = field(default_factory=uuid4)
    processed_at: datetime = field(default_factory=lambda: datetime.now(UTC))