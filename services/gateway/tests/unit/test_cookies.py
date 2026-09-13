"""Unit tests for HttpOnly cookie auth (Etap 4, prod-like)."""

from __future__ import annotations

import json

from src.core.config import Settings
from src.services.cookies import (
    access_cookie_header,
    build_refresh_body_from_cookie,
    clear_auth_cookies_headers,
    is_cookie_auth_path,
    is_logout_path,
    refresh_cookie_header,
    sanitize_auth_body,
)


def _settings() -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        jwt_public_key_path="/dev/null",
        cookie_secure=False,
    )


def test_cookie_auth_paths() -> None:
    assert is_cookie_auth_path("api/v1/auth/login")
    assert is_cookie_auth_path("api/v1/auth/register")
    assert is_cookie_auth_path("api/v1/auth/refresh")
    assert not is_cookie_auth_path("api/v1/loans")
    assert not is_cookie_auth_path("api/v1/me")


def test_logout_path() -> None:
    assert is_logout_path("api/v1/auth/logout")
    assert not is_logout_path("api/v1/auth/login")


def test_access_cookie_is_httpx_only_lax() -> None:
    header = access_cookie_header("tok123", _settings())
    assert "cg_access=tok123" in header
    assert "HttpOnly" in header
    assert "SameSite=Lax" in header
    assert "Path=/" in header
    assert "Secure" not in header  # localhost HTTP


def test_refresh_cookie_scoped_path() -> None:
    header = refresh_cookie_header("ref123", _settings())
    assert "cg_refresh=ref123" in header
    assert "Path=/api/v1/auth/refresh" in header
    assert "HttpOnly" in header


def test_secure_flag_in_prod() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        jwt_public_key_path="/dev/null",
        cookie_secure=True,
    )
    assert "Secure" in access_cookie_header("t", settings)


def test_sanitize_strips_raw_jwts() -> None:
    body = json.dumps(
        {"access_token": "A", "refresh_token": "R", "token_type": "bearer", "expires_in": 900}
    ).encode()
    safe, access, refresh = sanitize_auth_body(body)
    assert access == "A"
    assert refresh == "R"
    assert "access_token" not in safe
    assert "refresh_token" not in safe
    assert safe["expires_in"] == 900


def test_sanitize_rejects_garbage() -> None:
    safe, access, refresh = sanitize_auth_body(b"not-json")
    assert access is None and refresh is None
    assert "detail" in safe


def test_refresh_body_injects_cookie_token() -> None:
    raw = build_refresh_body_from_cookie("REF")
    assert json.loads(raw) == {"refresh_token": "REF"}


def test_logout_clears_both_cookies() -> None:
    headers = clear_auth_cookies_headers(_settings())
    assert len(headers) == 2
    assert any("cg_access=" in h and "Max-Age=0" in h for h in headers)
    assert any("cg_refresh=" in h and "Max-Age=0" in h for h in headers)
