"""Pure mapping: Kafka envelope -> Redis channel + JSON payload.

Kept side-effect free so it is unit-testable without Kafka/Redis.
This is the seam Etap 9 will reuse when BaseConsumer (retry/DLQ) lands.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from crediguard_events.envelope import EventEnvelope
from crediguard_events.loan import LOAN_STATUS_CHANGED_V1, LoanStatusChangedV1


def redis_channel_for(applicant_id: UUID | str) -> str:
    return f"loan-status:{applicant_id}"


def parse_status_changed(raw: bytes) -> EventEnvelope[LoanStatusChangedV1]:
    """Parse and validate a raw Kafka record into a typed envelope."""
    data: dict[str, Any] = json.loads(raw.decode("utf-8"))
    payload = LoanStatusChangedV1.model_validate(data["payload"])
    return EventEnvelope[LoanStatusChangedV1](
        event_id=data["event_id"],
        event_type=data.get("event_type", LOAN_STATUS_CHANGED_V1),
        occurred_at=data["occurred_at"],
        correlation_id=data["correlation_id"],
        producer=data.get("producer", "loan-application-service"),
        payload=payload,
    )


def to_redis_message(envelope: EventEnvelope[LoanStatusChangedV1]) -> tuple[str, bytes]:
    """Return (channel, json_bytes) for Redis Pub/Sub."""
    channel = redis_channel_for(envelope.payload.applicant_id)
    message = {
        "event_id": str(envelope.event_id),
        "event_type": envelope.event_type,
        "occurred_at": envelope.occurred_at.isoformat(),
        "correlation_id": str(envelope.correlation_id),
        "loan_id": str(envelope.payload.loan_id),
        "applicant_id": str(envelope.payload.applicant_id),
        "old_status": envelope.payload.old_status,
        "new_status": envelope.payload.new_status,
        "decision_reasons": envelope.payload.decision_reasons,
    }
    return channel, json.dumps(message).encode("utf-8")


def handle_message(raw: bytes) -> tuple[str, bytes]:
    """Full in-process pipeline for one Kafka record (parse -> map).

    Raises on invalid payload — caller decides commit/retry semantics.
    """
    return to_redis_message(parse_status_changed(raw))
