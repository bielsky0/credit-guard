from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import JSON, Index, Numeric, String, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import DateTime


class Base(DeclarativeBase):
    pass


class LoanApplicationModel(Base):
    __tablename__ = "loan_applications"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    applicant_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), index=True, nullable=False
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    term_months: Mapped[int] = mapped_column(nullable=False)
    monthly_income: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    applicant_age: Mapped[int] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="DRAFT")
    decision_reasons: Mapped[Optional[list[Any]]] = mapped_column(JSON, nullable=True)
    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(255), unique=True, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class OutboxEventModel(Base):
    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    aggregate_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_outbox_pending", "status", postgresql_where=text("status = 'PENDING'")),
    )


class ProcessedEventModel(Base):
    __tablename__ = "processed_events"

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )
    event_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), unique=True, nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    aggregate_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )