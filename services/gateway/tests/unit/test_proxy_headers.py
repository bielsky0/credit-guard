"""Unit tests for the gateway reverse-proxy header rewriting (spec §8).

The core security invariant: client-supplied identity headers (X-User-ID,
X-User-Roles) and Authorization must never be forwarded; the gateway injects the
trusted identity after JWT validation.
"""

from __future__ import annotations

from src.services.proxy import ProxyService


def _proxied_headers(
    proxy: ProxyService, incoming: dict[str, str], *, applicant_id: str | None
) -> dict[str, str]:
    headers = proxy._rewrite_headers(
        incoming,
        applicant_id=applicant_id,
        correlation_id="corr-123",
    )
    return {k.decode(): v.decode() for k, v in headers}


def test_strips_spoofed_user_headers_and_authorization() -> None:
    proxy = ProxyService()
    incoming = {
        "X-User-ID": "attacker-supplied",
        "X-User-Roles": "admin",
        "Authorization": "Bearer attacker-token",
        "Content-Type": "application/json",
    }
    headers = _proxied_headers(proxy, incoming, applicant_id="real-uuid")

    assert "X-User-ID" in headers
    assert headers["X-User-ID"] == "real-uuid"
    assert "X-User-Roles" in headers
    assert headers["X-User-Roles"] == "applicant"
    assert "Authorization" not in headers


def test_public_routes_forward_without_identity() -> None:
    proxy = ProxyService()
    headers = _proxied_headers(proxy, {"Content-Type": "application/json"}, applicant_id=None)
    assert "X-User-ID" not in headers
    assert "X-User-Roles" not in headers
    assert "X-Correlation-ID" in headers


def test_correlation_id_is_always_forwarded() -> None:
    proxy = ProxyService()
    headers = _proxied_headers(proxy, {}, applicant_id=None)
    assert headers["X-Correlation-ID"] == "corr-123"


def test_hop_by_hop_headers_are_not_forwarded() -> None:
    proxy = ProxyService()
    headers = _proxied_headers(
        proxy,
        {"Connection": "keep-alive", "Transfer-Encoding": "chunked", "Host": "evil.com"},
        applicant_id=None,
    )
    for blocked in ("Connection", "Transfer-Encoding", "Host"):
        assert blocked not in headers
