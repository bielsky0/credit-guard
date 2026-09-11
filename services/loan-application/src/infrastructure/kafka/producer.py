from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from crediguard_events import EventEnvelope
from crediguard_observability import get_logger
from pydantic import BaseModel

from src.infrastructure.persistence.models import OutboxEventModel

logger = get_logger()


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
    decision_reasons: list[str] | None = None


EVENT_SCHEMAS: dict[str, type[BaseModel]] = {
    "loan.application.submitted.v1": LoanApplicationSubmittedV1,
    "loan.status.changed.v1": LoanStatusChangedV1,
}


class OutboxWorker:
    def __init__(
        self,
        bootstrap_servers: str,
        database_url: str,
        poll_interval_ms: int = 500,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._database_url = database_url
        self._poll_interval_ms = poll_interval_ms
        self._producer: AIOKafkaProducer | None = None
        self._running = False

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()
        self._running = True
        logger.info(
            "OutboxWorker started",
            bootstrap_servers=self._bootstrap_servers,
            poll_interval_ms=self._poll_interval_ms,
        )

    async def stop(self) -> None:
        self._running = False
        if self._producer:
            await self._producer.stop()
        logger.info("OutboxWorker stopped")

    async def poll_once(self) -> int:
        from sqlalchemy import select, update
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
        from sqlalchemy.pool import NullPool

        engine = create_async_engine(self._database_url, poolclass=NullPool)
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )

        published_count = 0
        try:
            async with session_factory() as session:
                stmt = (
                    select(OutboxEventModel)
                    .where(OutboxEventModel.status == "PENDING")
                    .order_by(OutboxEventModel.created_at)
                    .limit(10)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                events = result.scalars().all()

                for event_model in events:
                    try:
                        await self._publish_event(event_model)

                        update_stmt = (
                            update(OutboxEventModel)
                            .where(OutboxEventModel.id == event_model.id)
                            .values(
                                status="SENT",
                                sent_at=datetime.now(UTC),
                            )
                        )
                        await session.execute(update_stmt)
                        published_count += 1
                    except Exception:
                        logger.exception(
                            "Failed to publish outbox event",
                            event_id=str(event_model.id),
                            event_type=event_model.event_type,
                        )

                await session.commit()
        finally:
            await engine.dispose()

        return published_count

    async def _publish_event(self, event_model: OutboxEventModel) -> None:
        if self._producer is None:
            raise RuntimeError("OutboxWorker not started")

        schema = EVENT_SCHEMAS.get(event_model.event_type)
        if schema is None:
            logger.warning(
                "Unknown event type, skipping",
                event_type=event_model.event_type,
            )
            return

        payload_data = event_model.payload
        if isinstance(payload_data, dict):
            payload = schema.model_validate(payload_data)
        else:
            payload = schema.model_validate_json(str(payload_data))

        envelope = EventEnvelope(
            event_type=event_model.event_type,
            correlation_id=uuid4(),
            producer="loan-application-service",
            payload=payload,
        )

        topic = event_model.event_type
        key = str(event_model.aggregate_id).encode()

        await self._producer.send_and_wait(
            topic=topic,
            key=key,
            value=envelope.model_dump_json().encode(),
        )

        logger.info(
            "Published outbox event",
            event_id=str(event_model.id),
            event_type=event_model.event_type,
            topic=topic,
        )

    async def run(self) -> None:
        import asyncio

        while self._running:
            try:
                count = await self.poll_once()
                if count > 0:
                    logger.info("OutboxWorker published events", count=count)
            except Exception:
                logger.exception("OutboxWorker poll error")
            await asyncio.sleep(self._poll_interval_ms / 1000)