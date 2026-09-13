"""Server-Sent Events for loan status (spec §5.6).

Prod-like auth: HttpOnly `cg_access` cookie (Bearer header accepted as
fallback for non-browser clients). Cross-user access returns 404 so loan
existence is not leaked. First frame is always the current state from the
loan service, so late subscribers never hang.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from redis.asyncio import Redis

from src.core.config import Settings
from src.services.token import InvalidTokenError, TokenValidator

router = APIRouter()

_validator: TokenValidator | None = None
_settings: Settings | None = None
_redis: Redis | None = None
_http_client: httpx.AsyncClient | None = None

HEARTBEAT_SECONDS = 15
TOTAL_STEPS = 5

_STATUS_STEP: dict[str, tuple[int, str]] = {
    "SUBMITTED": (1, "Wniosek przyjęty..."),
    "DOC_VERIFICATION": (2, "Weryfikacja dokumentu..."),
    "UNDERWRITING": (3, "Analiza ryzyka kredytowego..."),
    "APPROVED": (4, "Wniosek zaakceptowany"),
    "DISBURSING": (5, "Wypłata środków..."),
    "DISBURSED": (5, "Wypłacono"),
    "DOC_REJECTED": (2, "Dokument zweryfikowany negatywnie"),
    "REJECTED": (3, "Wniosek odrzucony"),
    "DISBURSEMENT_FAILED": (5, "Błąd wypłaty"),
}

_TERMINAL_STATUSES = frozenset({"DISBURSED", "DOC_REJECTED", "REJECTED", "DISBURSEMENT_FAILED"})


def init_sse(
    settings: Settings,
    validator: TokenValidator,
    redis: Redis,
    http_client: httpx.AsyncClient | None = None,
) -> None:
    global _validator, _settings, _redis, _http_client
    _settings = settings
    _validator = validator
    _redis = redis
    _http_client = http_client


def format_sse(*, event: str, data: dict[str, Any], event_id: str) -> str:
    """Render one SSE frame (id + event + data lines)."""
    return f"id: {event_id}\nevent: {event}\ndata: {json.dumps(data)}\n\n"


def heartbeat_frame() -> str:
    return ": ping\n\n"


def status_frame(
    *,
    loan_id: str,
    status: str,
    event_id: str,
    reasons: list[str] | None = None,
    occurred_at: str | None = None,
) -> str:
    step, message = _STATUS_STEP.get(status, (1, "Przetwarzanie..."))
    event = "decision" if status in _TERMINAL_STATUSES or status == "APPROVED" else "status"
    data: dict[str, Any] = {
        "loan_id": loan_id,
        "status": status,
        "step": step,
        "total_steps": TOTAL_STEPS,
        "message": message,
        "occurred_at": occurred_at or datetime.now(UTC).isoformat(),
    }
    if reasons:
        data["reasons"] = reasons
    return format_sse(event=event, data=data, event_id=event_id)


def _check_origin(request: Request) -> bool:
    """CSRF guard: cookies are sent automatically, so require our own origin.

    SameSite=Lax blocks most cross-site sends; this check closes the
    remaining gap (top-level GET navigations can't read the stream anyway).
    """
    if _settings is None:
        return False
    origin = request.headers.get("origin") or request.headers.get("referer")
    if origin is None:
        return True  # non-browser client (curl) — auth still enforced
    return origin.startswith(_settings.frontend_url)


async def _fetch_loan(
    loan_id: str, applicant_id: str, correlation_id: str
) -> dict[str, Any] | None:
    """Ownership-checked read from the loan service. None => 404 to caller."""
    assert _settings is not None
    url = f"{_settings.loan_service_url.rstrip('/')}/api/v1/loans/{loan_id}"
    if _http_client is not None:
        resp = await _http_client.get(
            url,
            headers={"X-User-ID": applicant_id, "X-Correlation-ID": correlation_id},
        )
        if resp.status_code == 200:
            return resp.json()  # type: ignore[no-any-return]
        return None
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            url,
            headers={"X-User-ID": applicant_id, "X-Correlation-ID": correlation_id},
        )
        if resp.status_code == 200:
            return resp.json()  # type: ignore[no-any-return]
        return None


async def event_stream(
    *,
    loan_id: str,
    applicant_id: str,
    last_event_id: str | None,
    correlation_id: str,
) -> AsyncIterator[str]:
    assert _redis is not None
    # 1. Initial state from REST so late subscribers never hang.
    loan = await _fetch_loan(loan_id, applicant_id, correlation_id)
    if loan is None:
        return
    status = str(loan.get("status", "SUBMITTED"))
    reasons = loan.get("decision_reasons") or None
    if last_event_id is None:
        yield status_frame(
            loan_id=loan_id,
            status=status,
            event_id=f"initial-{uuid4()}",
            reasons=reasons,
        )
    if status in _TERMINAL_STATUSES and last_event_id is None:
        return

    # 2. Live updates from Redis Pub/Sub (notification bridges Kafka here).
    channel = f"loan-status:{applicant_id}"
    pubsub = _redis.pubsub()
    await pubsub.subscribe(channel)
    seen_resume_id = last_event_id is None
    try:
        while True:
            if await _disconnect_hint():
                break
            msg = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=HEARTBEAT_SECONDS
            )
            if msg is None:
                yield heartbeat_frame()
                continue
            try:
                payload = json.loads(msg["data"])
            except (ValueError, TypeError):
                continue
            if str(payload.get("loan_id")) != loan_id:
                continue  # channel is per-user; filter to this loan
            event_id = str(payload.get("event_id", uuid4()))
            if not seen_resume_id:
                if event_id == last_event_id:
                    seen_resume_id = True
                continue
            yield status_frame(
                loan_id=loan_id,
                status=str(payload.get("new_status", status)),
                event_id=event_id,
                reasons=payload.get("decision_reasons"),
                occurred_at=payload.get("occurred_at"),
            )
            if str(payload.get("new_status")) in _TERMINAL_STATUSES:
                break
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()  # type: ignore[no-untyped-call]


async def _disconnect_hint() -> bool:
    """Cooperative-cancel hook; real disconnects surface as CancelledError."""
    await asyncio.sleep(0)
    return False


@router.get("/api/v1/loans/{loan_id}/events", response_model=None)
async def loan_events(loan_id: str, request: Request) -> StreamingResponse | JSONResponse:
    if _validator is None or _settings is None or _redis is None:
        return JSONResponse({"detail": "Gateway not ready"}, status_code=503)
    if not _check_origin(request):
        return JSONResponse({"detail": "Forbidden"}, status_code=403)

    # Cookie-first (prod browsers), Bearer fallback (curl / service clients).
    applicant_id: str | None = None
    try:
        applicant_id = str(
            _validator.validate_cookie(request.cookies.get(_settings.access_cookie_name))
        )
    except InvalidTokenError:
        try:
            applicant_id = str(_validator.validate(request.headers.get("authorization")))
        except InvalidTokenError:
            return JSONResponse(
                {"detail": "Unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
    assert applicant_id is not None

    correlation_id = request.headers.get("x-correlation-id") or str(uuid4())
    loan = await _fetch_loan(loan_id, applicant_id, correlation_id)
    if loan is None:
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    last_event_id = request.headers.get("last-event-id")
    return StreamingResponse(
        event_stream(
            loan_id=loan_id,
            applicant_id=applicant_id,
            last_event_id=last_event_id,
            correlation_id=correlation_id,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Correlation-ID": correlation_id,
        },
    )
