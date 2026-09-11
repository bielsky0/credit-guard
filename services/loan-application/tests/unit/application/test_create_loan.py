from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from src.application.dto import CreateLoanApplicationRequest
from src.application.ports.idempotency import IdempotencyStore
from src.application.ports.repository import (
    LoanApplicationRepository,
    OutboxRepository,
)
from src.application.use_cases.create_loan_application import CreateLoanApplicationUseCase
from src.domain.entities import LoanApplication
from src.domain.exceptions import DuplicateActiveApplication
from src.domain.value_objects import LoanStatus


class FakeLoanRepo(LoanApplicationRepository):
    def __init__(self) -> None:
        self.loans: dict[str, LoanApplication] = {}
        self._active_for_applicant = False

    async def save(self, loan: LoanApplication) -> None:
        self.loans[str(loan.id)] = loan

    async def get_by_id(self, loan_id: Any) -> LoanApplication | None:
        return self.loans.get(str(loan_id))

    async def get_by_applicant_id(self, applicant_id: Any) -> list[LoanApplication]:
        return [loan for loan in self.loans.values() if loan.applicant_id == applicant_id]

    async def exists_active_for_applicant(self, applicant_id: Any) -> bool:
        return self._active_for_applicant

    async def get_by_idempotency_key(self, key: str) -> LoanApplication | None:
        return None


class FakeOutboxRepo(OutboxRepository):
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def save(self, event: Any) -> None:
        self.events.append(event)

    async def get_pending(self, limit: int = 10) -> list[Any]:
        return []

    async def mark_sent(self, event_id: Any) -> None:
        pass


class FakeIdempotencyStore(IdempotencyStore):
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    async def acquire(self, key: str, value: str, ttl_seconds: int = 86400) -> bool:
        if key in self.store:
            return False
        self.store[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)


@pytest.mark.asyncio
async def test_create_loan_success() -> None:
    loan_repo = FakeLoanRepo()
    outbox_repo = FakeOutboxRepo()
    idempotency = FakeIdempotencyStore()

    use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)

    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )
    applicant_id = uuid4()

    result = await use_case.execute(applicant_id, request)

    assert result.applicant_id == applicant_id
    assert result.amount == Decimal("10000")
    assert result.status == LoanStatus.SUBMITTED
    assert len(loan_repo.loans) == 1
    assert len(outbox_repo.events) == 2  # submitted + status changed


@pytest.mark.asyncio
async def test_create_loan_duplicate_active_raises() -> None:
    loan_repo = FakeLoanRepo()
    loan_repo._active_for_applicant = True
    outbox_repo = FakeOutboxRepo()
    idempotency = FakeIdempotencyStore()

    use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)

    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )

    with pytest.raises(DuplicateActiveApplication):
        await use_case.execute(uuid4(), request)


@pytest.mark.asyncio
async def test_create_loan_with_idempotency_key() -> None:
    loan_repo = FakeLoanRepo()
    outbox_repo = FakeOutboxRepo()
    idempotency = FakeIdempotencyStore()

    use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)

    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )
    idempotency_key = "test-key-123"

    result = await use_case.execute(uuid4(), request, idempotency_key)

    assert result.status == LoanStatus.SUBMITTED
    stored = await idempotency.get(idempotency_key)
    assert stored is not None


@pytest.mark.asyncio
async def test_outbox_events_have_correct_types() -> None:
    loan_repo = FakeLoanRepo()
    outbox_repo = FakeOutboxRepo()
    idempotency = FakeIdempotencyStore()

    use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)

    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )

    await use_case.execute(uuid4(), request)

    event_types = [e.event_type for e in outbox_repo.events]
    assert "loan.application.submitted.v1" in event_types
    assert "loan.status.changed.v1" in event_types