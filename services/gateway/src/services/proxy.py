"""Reverse proxy to internal services (spec §4.1, §8).

Security rules for header rewriting:
- Incoming requests from outside may carry a spoofed `X-User-ID` / `X-User-Roles`
  header. The gateway *always* removes these from the client request before
  forwarding (protection against spoofing, §8).
- After JWT validation, the gateway injects the trusted `X-User-ID` (and a
  default role set). Internal services trust these headers because they are only
  reachable on the closed internal network.
- The original `Authorization` header is stripped before forwarding.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any

import httpx

# Headers set/controlled exclusively by the gateway — removed from any client
# request before forwarding.
_STRIPPED_HEADERS = frozenset({"x-user-id", "x-user-roles", "authorization", "x-correlation-id"})

# Headers the gateway never lets through from the client (hop-by-hop / security).
_BLOCKED_HEADERS = frozenset(
    {"host", "content-length", "connection", "transfer-encoding", "upgrade"}
)

_DEFAULT_ROLES = "applicant"


class ProxyService:
    """Forwards requests to internal services over the closed network."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(timeout=30.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    def _rewrite_headers(
        self,
        client_headers: Mapping[str, str],
        *,
        applicant_id: str | None,
        correlation_id: str,
    ) -> list[tuple[bytes, bytes]]:
        """Build the header list sent to the downstream service."""
        out: list[tuple[bytes, bytes]] = []
        for k, v in client_headers.items():
            lk = k.lower()
            if lk in _STRIPPED_HEADERS or lk in _BLOCKED_HEADERS:
                continue
            out.append((k.encode("latin-1", "ignore"), v.encode("latin-1", "ignore")))
        # Inject trusted identity + correlation id.
        if applicant_id is not None:
            out.append((b"X-User-ID", applicant_id.encode()))
            out.append((b"X-User-Roles", _DEFAULT_ROLES.encode()))
        out.append((b"X-Correlation-ID", correlation_id.encode()))
        return out

    async def forward(
        self,
        *,
        method: str,
        path: str,
        query_string: bytes,
        base_url: str,
        client_headers: Mapping[str, str],
        body: AsyncIterator[bytes] | None,
        applicant_id: str | None,
        correlation_id: str,
    ) -> dict[str, Any]:
        """Forward a request and stream the response back.

        Returns a dict with status_code, headers, and an async body iterator.
        """
        headers = self._rewrite_headers(
            client_headers,
            applicant_id=applicant_id,
            correlation_id=correlation_id,
        )
        url = base_url.rstrip("/") + "/" + path.lstrip("/")

        req = self._client.build_request(
            method=method,
            url=url,
            headers=headers,
            params=None,
            content=body,
        )
        # Override query string manually to avoid httpx re-encoding surprises.
        req.url = req.url.copy_with(query=query_string.decode("latin-1", "ignore"))

        resp = await self._client.send(req, stream=True)

        async def body_stream() -> AsyncIterator[bytes]:
            try:
                async for chunk in resp.aiter_bytes():
                    yield chunk
            finally:
                await resp.aclose()

        return {
            "status_code": resp.status_code,
            "response_headers": resp.headers,
            "body_iterator": body_stream(),
        }
