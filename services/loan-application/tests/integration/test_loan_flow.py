"""Integration tests for loan application flow."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from src.application.dto import CreateLoanApplicationRequest
from src.application.use_cases.create_loan_application import CreateLoanApplicationUseCase
from src.application.use_cases.get_loan_application import GetLoanApplicationUseCase
from src.domain.exceptions import DuplicateActiveApplication, LoanApplicationNotFound
from src.domain.value_objects import LoanStatus
from src.infrastructure.persistence.repository import (
    SQLAlchemyLoanApplicationRepository,
    SQLAlchemyOutboxRepository,
)


class FakeIdempotencyStore:
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
async def test_create_and_get_loan(session: AsyncSession) -> None:
    loan_repo = SQLAlchemyLoanApplicationRepository(session)
    outbox_repo = SQLAlchemyOutboxRepository(session)
    idempotency = FakeIdempotencyStore()

    create_use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)
    get_use_case = GetLoanApplicationUseCase(loan_repo)

    applicant_id = uuid4()
    request = CreateLoanApplicationRequest(
        amount=Decimal("15000"),
        term_months=24,
        monthly_income=Decimal("6000"),
        applicant_age=28,
    )

    result = await create_use_case.execute(applicant_id, request)

    assert result.applicant_id == applicant_id
    assert result.amount == Decimal("15000")
    assert result.term_months == 24
    assert result.status == LoanStatus.SUBMITTED

    fetched = await get_use_case.execute(result.id, applicant_id)
    assert fetched.id == result.id
    assert fetched.status == LoanStatus.SUBMITTED


@pytest.mark.asyncio
async def test_outbox_events_created(session: AsyncSession) -> None:
    loan_repo = SQLAlchemyLoanApplicationRepository(session)
    outbox_repo = SQLAlchemyOutboxRepository(session)
    idempotency = FakeIdempotencyStore()

    create_use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)

    applicant_id = uuid4()
    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )

    result = await create_use_case.execute(applicant_id, request)

    pending_events = await outbox_repo.get_pending(limit=100)
    loan_events = [e for e in pending_events if e.aggregate_id == result.id]
    event_types = {e.event_type for e in loan_events}

    assert "loan.application.submitted.v1" in event_types
    assert "loan.status.changed.v1" in event_types


@pytest.mark.asyncio
async def test_idempotency_key_prevents_duplicate(session: AsyncSession) -> None:
    loan_repo = SQLAlchemyLoanApplicationRepository(session)
    outbox_repo = SQLAlchemyOutboxRepository(session)
    idempotency = FakeIdempotencyStore()

    create_use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)

    applicant_id = uuid4()
    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )
    idempotency_key = "unique-key-001"

    result1 = await create_use_case.execute(applicant_id, request, idempotency_key)

    assert await idempotency.get(idempotency_key) == str(result1.id)

    result2 = await create_use_case.execute(applicant_id, request, idempotency_key)
    assert result2.id == result1.id


@pytest.mark.asyncio
async def test_duplicate_active_application_rejected(session: AsyncSession) -> None:
    loan_repo = SQLAlchemyLoanApplicationRepository(session)
    outbox_repo = SQLAlchemyOutboxRepository(session)
    idempotency = FakeIdempotencyStore()

    create_use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)

    applicant_id = uuid4()
    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )

    await create_use_case.execute(applicant_id, request)

    with pytest.raises(DuplicateActiveApplication):
        await create_use_case.execute(applicant_id, request)


@pytest.mark.asyncio
async def test_get_nonexistent_loan(session: AsyncSession) -> None:
    loan_repo = SQLAlchemyLoanApplicationRepository(session)
    get_use_case = GetLoanApplicationUseCase(loan_repo)

    with pytest.raises(LoanApplicationNotFound):
        await get_use_case.execute(uuid4(), uuid4())


@pytest.mark.asyncio
async def test_get_loan_wrong_applicant(session: AsyncSession) -> None:
    loan_repo = SQLAlchemyLoanApplicationRepository(session)
    outbox_repo = SQLAlchemyOutboxRepository(session)
    idempotency = FakeIdempotencyStore()

    create_use_case = CreateLoanApplicationUseCase(loan_repo, outbox_repo, idempotency)
    get_use_case = GetLoanApplicationUseCase(loan_repo)

    applicant_id = uuid4()
    request = CreateLoanApplicationRequest(
        amount=Decimal("10000"),
        term_months=12,
        monthly_income=Decimal("5000"),
        applicant_age=30,
    )

    result = await create_use_case.execute(applicant_id, request)

    with pytest.raises(LoanApplicationNotFound):
        await get_use_case.execute(result.id, uuid4())