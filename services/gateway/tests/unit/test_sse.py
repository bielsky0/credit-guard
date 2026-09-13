"""Unit tests for SSE framing (spec §5.6)."""

from __future__ import annotations

import json

from src.api.sse import format_sse, heartbeat_frame, status_frame


def test_format_sse_frame() -> None:
    frame = format_sse(event="status", data={"a": 1}, event_id="e1")
    assert frame == 'id: e1\nevent: status\ndata: {"a": 1}\n\n'


def test_heartbeat_frame() -> None:
    assert heartbeat_frame() == ": ping\n\n"


def test_status_frame_progress() -> None:
    frame = status_frame(loan_id="L", status="UNDERWRITING", event_id="e2")
    assert "event: status" in frame
    payload = json.loads(frame.split("data: ", 1)[1])
    assert payload["step"] == 3
    assert payload["total_steps"] == 5
    assert payload["loan_id"] == "L"


def test_rejected_is_decision_with_reasons() -> None:
    frame = status_frame(loan_id="L", status="REJECTED", event_id="e3", reasons=["Wysokie DTI"])
    assert "event: decision" in frame
    payload = json.loads(frame.split("data: ", 1)[1])
    assert payload["reasons"] == ["Wysokie DTI"]
    assert payload["status"] == "REJECTED"


def test_approved_is_decision() -> None:
    frame = status_frame(loan_id="L", status="APPROVED", event_id="e4")
    assert "event: decision" in frame
