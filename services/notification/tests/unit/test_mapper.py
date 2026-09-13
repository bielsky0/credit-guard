"""Unit tests for the notification mapper (pure, no I/O)."""

from __future__ import annotations

import json
from uuid import uuid4

from src.mapper import handle_message, parse_status_changed, redis_channel_for, to_redis_message


def _envelope_bytes() -> bytes:
    loan_id = uuid4()
    applicant_id = uuid4()
    event_id = uuid4()
    correlation_id = uuid4()
    return json.dumps(
        {
            "event_id": str(event_id),
            "event_type": "loan.status.changed.v1",
            "occurred_at": "2026-07-13T12:00:00Z",
            "correlation_id": str(correlation_id),
            "producer": "loan-application-service",
            "payload": {
                "loan_id": str(loan_id),
                "applicant_id": str(applicant_id),
                "old_status": "DRAFT",
                "new_status": "SUBMITTED",
                "decision_reasons": None,
            },
        }
    ).encode()


def test_channel_is_per_applicant() -> None:
    assert redis_channel_for("abc") == "loan-status:abc"


def test_parse_and_map_roundtrip() -> None:
    raw = _envelope_bytes()
    envelope = parse_status_changed(raw)
    assert envelope.payload.new_status == "SUBMITTED"
    channel, message = to_redis_message(envelope)
    assert channel == f"loan-status:{envelope.payload.applicant_id}"
    decoded = json.loads(message.decode())
    assert decoded["loan_id"] == str(envelope.payload.loan_id)
    assert decoded["correlation_id"] == str(envelope.correlation_id)


def test_handle_message_returns_channel_and_bytes() -> None:
    channel, message = handle_message(_envelope_bytes())
    assert channel.startswith("loan-status:")
    assert isinstance(message, bytes)
