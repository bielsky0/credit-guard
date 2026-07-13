"""Shared Kafka event envelope (spec docs/SPECYFIKACJA.md §5.2).

Every event CrediGuard services publish is wrapped in this envelope. Per-event
payload schemas (e.g. `UnderwritingCompletedV1`) are added alongside the
service that owns them, not here — this module only defines the envelope
itself.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

PayloadT = TypeVar("PayloadT", bound=BaseModel)


class EventEnvelope(BaseModel, Generic[PayloadT]):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: UUID
    producer: str
    payload: PayloadT
