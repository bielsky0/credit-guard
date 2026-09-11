from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
from src.domain.entities import LoanApplication, OutboxEvent, ProcessedEvent
from src.domain.exceptions import InvalidStatusTransition
from src.domain.value_objects import LoanStatus


class TestLoanApplicationStateMachine:
    def _make_loan(self, status: LoanStatus = LoanStatus.DRAFT) -> LoanApplication:
        loan = LoanApplication(
            applicant_id=uuid4(),
            amount=Decimal("10000"),
            term_months=12,
            monthly_income=Decimal("5000"),
            applicant_age=30,
        )
        loan.status = status
        return loan

    def test_draft_to_submitted(self) -> None:
        loan = self._make_loan(LoanStatus.DRAFT)
        loan.submit()
        assert loan.status == LoanStatus.SUBMITTED

    def test_submitted_to_doc_verification(self) -> None:
        loan = self._make_loan(LoanStatus.SUBMITTED)
        loan.start_doc_verification()
        assert loan.status == LoanStatus.DOC_VERIFICATION

    def test_submitted_to_rejected(self) -> None:
        loan = self._make_loan(LoanStatus.SUBMITTED)
        loan.reject()
        assert loan.status == LoanStatus.REJECTED

    def test_doc_verification_to_underwriting(self) -> None:
        loan = self._make_loan(LoanStatus.DOC_VERIFICATION)
        loan.start_underwriting()
        assert loan.status == LoanStatus.UNDERWRITING

    def test_doc_verification_to_doc_rejected(self) -> None:
        loan = self._make_loan(LoanStatus.DOC_VERIFICATION)
        loan.reject_doc()
        assert loan.status == LoanStatus.DOC_REJECTED

    def test_underwriting_to_approved(self) -> None:
        loan = self._make_loan(LoanStatus.UNDERWRITING)
        loan.approve()
        assert loan.status == LoanStatus.APPROVED

    def test_underwriting_to_rejected_with_reasons(self) -> None:
        loan = self._make_loan(LoanStatus.UNDERWRITING)
        loan.reject(reasons=["Wysoki DTI"])
        assert loan.status == LoanStatus.REJECTED
        assert loan.decision_reasons == ["Wysoki DTI"]

    def test_approved_to_disbursing(self) -> None:
        loan = self._make_loan(LoanStatus.APPROVED)
        loan.start_disbursing()
        assert loan.status == LoanStatus.DISBURSING

    def test_disbursing_to_disbursed(self) -> None:
        loan = self._make_loan(LoanStatus.DISBURSING)
        loan.complete_disbursement()
        assert loan.status == LoanStatus.DISBURSED

    def test_disbursing_to_disbursement_failed(self) -> None:
        loan = self._make_loan(LoanStatus.DISBURSING)
        loan.fail_disbursement()
        assert loan.status == LoanStatus.DISBURSEMENT_FAILED

    def test_invalid_transition_raises(self) -> None:
        loan = self._make_loan(LoanStatus.DRAFT)
        with pytest.raises(InvalidStatusTransition) as exc_info:
            loan.approve()
        assert exc_info.value.from_status == "DRAFT"
        assert exc_info.value.to_status == "APPROVED"

    def test_cannot_transition_from_terminal_status(self) -> None:
        for terminal in [
            LoanStatus.DISBURSED,
            LoanStatus.REJECTED,
            LoanStatus.DOC_REJECTED,
            LoanStatus.DISBURSEMENT_FAILED,
        ]:
            loan = self._make_loan(terminal)
            with pytest.raises(InvalidStatusTransition):
                loan.submit()

    def test_is_active_true_for_draft(self) -> None:
        loan = self._make_loan(LoanStatus.DRAFT)
        assert loan.is_active is True

    def test_is_active_true_for_submitted(self) -> None:
        loan = self._make_loan(LoanStatus.SUBMITTED)
        assert loan.is_active is True

    def test_is_active_false_for_rejected(self) -> None:
        loan = self._make_loan(LoanStatus.REJECTED)
        assert loan.is_active is False

    def test_is_active_false_for_disbursed(self) -> None:
        loan = self._make_loan(LoanStatus.DISBURSED)
        assert loan.is_active is False

    def test_monthly_payment_calculation(self) -> None:
        loan = LoanApplication(
            applicant_id=uuid4(),
            amount=Decimal("12000"),
            term_months=12,
            monthly_income=Decimal("5000"),
            applicant_age=30,
        )
        payment = loan.monthly_payment
        assert payment > Decimal("0")
        assert payment * loan.term_months > loan.amount  # includes interest


class TestOutboxEvent:
    def test_default_status_is_pending(self) -> None:
        event = OutboxEvent(
            aggregate_id=uuid4(),
            event_type="loan.application.submitted.v1",
            payload={"loan_id": "test"},
        )
        assert event.status == "PENDING"
        assert event.sent_at is None


class TestProcessedEvent:
    def test_creation(self) -> None:
        event = ProcessedEvent(
            event_id=uuid4(),
            event_type="loan.application.submitted.v1",
            aggregate_id=uuid4(),
        )
        assert event.processed_at is not None