from __future__ import annotations

from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.ports.repository import (
    LoanApplicationRepository,
    OutboxRepository,
    ProcessedEventRepository,
)
from src.domain.entities import LoanApplication, OutboxEvent, ProcessedEvent
from src.domain.value_objects import LoanStatus
from src.infrastructure.persistence.models import (
    LoanApplicationModel,
    OutboxEventModel,
    ProcessedEventModel,
)


class SQLAlchemyLoanApplicationRepository(LoanApplicationRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, loan: LoanApplication) -> None:
        model = LoanApplicationModel(
            id=loan.id,
            applicant_id=loan.applicant_id,
            amount=loan.amount,
            term_months=loan.term_months,
            monthly_income=loan.monthly_income,
            applicant_age=loan.applicant_age,
            status=loan.status.value,
            decision_reasons=loan.decision_reasons,
            idempotency_key=loan.idempotency_key,
            created_at=loan.created_at,
            updated_at=loan.updated_at,
        )
        self._session.add(model)
        await self._session.flush()

    async def get_by_id(self, loan_id: UUID) -> LoanApplication | None:
        stmt = select(LoanApplicationModel).where(LoanApplicationModel.id == loan_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)

    async def get_by_applicant_id(self, applicant_id: UUID) -> list[LoanApplication]:
        stmt = (
            select(LoanApplicationModel)
            .where(LoanApplicationModel.applicant_id == applicant_id)
            .order_by(LoanApplicationModel.created_at.desc())
        )
        result = await self._session.execute(stmt)
        return [self._to_entity(m) for m in result.scalars().all()]

    async def exists_active_for_applicant(self, applicant_id: UUID) -> bool:
        active_statuses = [
            s.value for s in LoanStatus if s not in {
                LoanStatus.REJECTED,
                LoanStatus.DISBURSED,
                LoanStatus.DOC_REJECTED,
                LoanStatus.DISBURSEMENT_FAILED,
            }
        ]
        stmt = select(LoanApplicationModel.id).where(
            LoanApplicationModel.applicant_id == applicant_id,
            LoanApplicationModel.status.in_(active_statuses),
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def get_by_idempotency_key(self, key: str) -> LoanApplication | None:
        stmt = select(LoanApplicationModel).where(
            LoanApplicationModel.idempotency_key == key
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_entity(model)

    def _to_entity(self, model: LoanApplicationModel) -> LoanApplication:
        return LoanApplication(
            id=model.id,
            applicant_id=model.applicant_id,
            amount=model.amount,
            term_months=model.term_months,
            monthly_income=model.monthly_income,
            applicant_age=model.applicant_age,
            status=LoanStatus(model.status),
            decision_reasons=model.decision_reasons or [],
            idempotency_key=model.idempotency_key,
            created_at=model.created_at,
            updated_at=model.updated_at,
        )


class SQLAlchemyOutboxRepository(OutboxRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, event: OutboxEvent) -> None:
        model = OutboxEventModel(
            id=event.id,
            aggregate_id=event.aggregate_id,
            event_type=event.event_type,
            payload=event.payload,
            status=event.status,
            created_at=event.created_at,
            sent_at=event.sent_at,
        )
        self._session.add(model)
        await self._session.flush()

    async def get_pending(self, limit: int = 10) -> list[OutboxEvent]:
        stmt = (
            select(OutboxEventModel)
            .where(OutboxEventModel.status == "PENDING")
            .order_by(OutboxEventModel.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return [self._to_entity(m) for m in result.scalars().all()]

    async def mark_sent(self, event_id: UUID) -> None:
        stmt = (
            update(OutboxEventModel)
            .where(OutboxEventModel.id == event_id)
            .values(status="SENT")
        )
        await self._session.execute(stmt)
        await self._session.flush()

    def _to_entity(self, model: OutboxEventModel) -> OutboxEvent:
        return OutboxEvent(
            id=model.id,
            aggregate_id=model.aggregate_id,
            event_type=model.event_type,
            payload=model.payload,
            status=model.status,
            created_at=model.created_at,
            sent_at=model.sent_at,
        )


class SQLAlchemyProcessedEventRepository(ProcessedEventRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def exists(self, event_id: UUID) -> bool:
        stmt = select(ProcessedEventModel.id).where(
            ProcessedEventModel.event_id == event_id
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def save(self, event: ProcessedEvent) -> None:
        model = ProcessedEventModel(
            id=event.id,
            event_id=event.event_id,
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,
            processed_at=event.processed_at,
        )
        self._session.add(model)
        await self._session.flush()