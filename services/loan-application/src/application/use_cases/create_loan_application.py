from __future__ import annotations

from uuid import UUID

from src.application.dto import CreateLoanApplicationRequest, LoanApplicationResponse
from src.application.ports.idempotency import IdempotencyStore
from src.application.ports.repository import (
    LoanApplicationRepository,
    OutboxRepository,
)
from src.domain.entities import LoanApplication, OutboxEvent
from src.domain.exceptions import DuplicateActiveApplication
from src.domain.value_objects import LoanStatus


class CreateLoanApplicationUseCase:
    def __init__(
        self,
        loan_repo: LoanApplicationRepository,
        outbox_repo: OutboxRepository,
        idempotency_store: IdempotencyStore,
    ) -> None:
        self._loan_repo = loan_repo
        self._outbox_repo = outbox_repo
        self._idempotency_store = idempotency_store

    async def execute(
        self,
        applicant_id: UUID,
        request: CreateLoanApplicationRequest,
        idempotency_key: str | None = None,
    ) -> LoanApplicationResponse:
        if idempotency_key:
            existing_id = await self._idempotency_store.get(idempotency_key)
            if existing_id is not None:
                existing_loan = await self._loan_repo.get_by_id(UUID(existing_id))
                if existing_loan is not None:
                    return LoanApplicationResponse.model_validate(existing_loan)

        if await self._loan_repo.exists_active_for_applicant(applicant_id):
            raise DuplicateActiveApplication(applicant_id)

        loan = LoanApplication(
            applicant_id=applicant_id,
            amount=request.amount,
            term_months=request.term_months,
            monthly_income=request.monthly_income,
            applicant_age=request.applicant_age,
            idempotency_key=idempotency_key,
        )
        loan.submit()

        await self._loan_repo.save(loan)

        event_payload = {
            "loan_id": str(loan.id),
            "applicant_id": str(loan.applicant_id),
            "amount": str(loan.amount),
            "term_months": loan.term_months,
            "monthly_income": str(loan.monthly_income),
            "applicant_age": loan.applicant_age,
        }

        outbox_event = OutboxEvent(
            aggregate_id=loan.id,
            event_type="loan.application.submitted.v1",
            payload=event_payload,
        )
        await self._outbox_repo.save(outbox_event)

        status_event_payload = {
            "loan_id": str(loan.id),
            "applicant_id": str(loan.applicant_id),
            "old_status": LoanStatus.DRAFT.value,
            "new_status": loan.status.value,
            "decision_reasons": None,
        }
        status_outbox_event = OutboxEvent(
            aggregate_id=loan.id,
            event_type="loan.status.changed.v1",
            payload=status_event_payload,
        )
        await self._outbox_repo.save(status_outbox_event)

        if idempotency_key:
            await self._idempotency_store.acquire(
                idempotency_key, str(loan.id)
            )

        return LoanApplicationResponse.model_validate(loan)