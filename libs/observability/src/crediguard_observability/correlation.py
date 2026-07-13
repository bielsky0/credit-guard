"""Correlation-ID propagation (spec §9.1): read `X-Correlation-ID` from the
incoming request (or generate one), bind it into structlog's contextvars for
the duration of the request, and echo it back on the response.

Plain ASGI middleware — no framework dependency, so it works the same way in
FastAPI (gateway, applicant, ...) or any other ASGI app.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any
from uuid import uuid4

import structlog

# Minimal local ASGI type aliases so this package has zero framework
# dependencies (works the same under FastAPI/Starlette or any other ASGI app).
Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

HEADER_NAME = b"x-correlation-id"


class CorrelationIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        correlation_id = headers.get(HEADER_NAME, b"").decode() or str(uuid4())

        async def send_with_correlation_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                message.setdefault("headers", [])
                message["headers"].append((HEADER_NAME, correlation_id.encode()))
            await send(message)

        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
        try:
            await self.app(scope, receive, send_with_correlation_id)
        finally:
            structlog.contextvars.unbind_contextvars("correlation_id")
