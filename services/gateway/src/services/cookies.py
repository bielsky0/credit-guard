"""HttpOnly cookie handling for auth (spec §4.1, §8 — prod-like).

The gateway owns the edge: upstream Applicant Service returns plain JSON
`TokenResponse`, and the gateway translates it into two HttpOnly cookies.
Raw JWTs never reach the browser JS / response bodies.

- `cg_access`: short-lived access token, Path=/, sent to API + SSE.
- `cg_refresh`: long-lived refresh token, Path=/api/v1/auth/refresh only,
  so it is never sent to SSE or loan endpoints.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi.responses import JSONResponse

from src.core.config import Settings

AUTH_REGISTER_PATH = "api/v1/auth/register"
AUTH_LOGIN_PATH = "api/v1/auth/login"
AUTH_REFRESH_PATH = "api/v1/auth/refresh"
AUTH_LOGOUT_PATH = "api/v1/auth/logout"

_COOKIE_PATHS = (AUTH_REGISTER_PATH, AUTH_LOGIN_PATH, AUTH_REFRESH_PATH)


def is_cookie_auth_path(path: str) -> bool:
    normalized = path.lstrip("/")
    return any(normalized == p or normalized.startswith(p + "/") for p in _COOKIE_PATHS)


def is_logout_path(path: str) -> bool:
    normalized = path.lstrip("/")
    return normalized == AUTH_LOGOUT_PATH or normalized.startswith(AUTH_LOGOUT_PATH + "/")


def _cookie_header(
    name: str,
    value: str,
    *,
    max_age: int,
    path: str,
    secure: bool,
) -> str:
    parts = [f"{name}={value}", f"Max-Age={max_age}", f"Path={path}", "HttpOnly", "SameSite=Lax"]
    if secure:
        parts.append("Secure")
    return "; ".join(parts)


def access_cookie_header(access_token: str, settings: Settings) -> str:
    return _cookie_header(
        settings.access_cookie_name,
        access_token,
        max_age=settings.access_cookie_max_age,
        path="/",
        secure=settings.cookie_secure,
    )


def refresh_cookie_header(refresh_token: str, settings: Settings) -> str:
    return _cookie_header(
        settings.refresh_cookie_name,
        refresh_token,
        max_age=settings.refresh_cookie_max_age,
        path="/api/v1/auth/refresh",
        secure=settings.cookie_secure,
    )


def clear_cookie_header(name: str, *, path: str, secure: bool) -> str:
    parts = [f"{name}=", "Max-Age=0", f"Path={path}", "HttpOnly", "SameSite=Lax"]
    if secure:
        parts.append("Secure")
    return "; ".join(parts)


def clear_auth_cookies_headers(settings: Settings) -> list[str]:
    return [
        clear_cookie_header(settings.access_cookie_name, path="/", secure=settings.cookie_secure),
        clear_cookie_header(
            settings.refresh_cookie_name,
            path="/api/v1/auth/refresh",
            secure=settings.cookie_secure,
        ),
    ]


def sanitize_auth_body(body: bytes) -> tuple[dict[str, Any], str | None, str | None]:
    """Parse upstream TokenResponse, return (safe_body, access, refresh).

    The safe body keeps only non-secret fields so raw JWTs never leak
    to the browser. Returns (error_body, None, None) when unparsable —
    caller forwards upstream status in that case.
    """
    try:
        data = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {"detail": "Invalid auth response"}, None, None
    if not isinstance(data, dict):
        return {"detail": "Invalid auth response"}, None, None
    access = data.get("access_token")
    refresh = data.get("refresh_token")
    if not isinstance(access, str) or not isinstance(refresh, str):
        return {"detail": "Invalid auth response"}, None, None
    safe: dict[str, Any] = {
        "token_type": data.get("token_type", "bearer"),
        "expires_in": data.get("expires_in"),
    }
    return safe, access, refresh


def build_cookie_auth_response(
    *,
    status_code: int,
    safe_body: dict[str, Any],
    access_token: str,
    refresh_token: str,
    settings: Settings,
    correlation_id: str | None = None,
) -> JSONResponse:
    headers: dict[str, str] = {}
    if correlation_id:
        headers["X-Correlation-ID"] = correlation_id
    response = JSONResponse(status_code=status_code, content=safe_body, headers=headers)
    response.headers.append("Set-Cookie", access_cookie_header(access_token, settings))
    response.headers.append("Set-Cookie", refresh_cookie_header(refresh_token, settings))
    return response


def build_logout_response(
    settings: Settings,
    correlation_id: str | None = None,
) -> JSONResponse:
    headers: dict[str, str] = {}
    if correlation_id:
        headers["X-Correlation-ID"] = correlation_id
    response = JSONResponse(status_code=200, content={"detail": "Logged out"}, headers=headers)
    for clearing in clear_auth_cookies_headers(settings):
        response.headers.append("Set-Cookie", clearing)
    return response


def build_refresh_body_from_cookie(refresh_token: str) -> bytes:
    """Upstream applicant expects JSON {"refresh_token": ...}; the browser
    only sends the HttpOnly cookie, so the gateway injects the body."""
    return json.dumps({"refresh_token": refresh_token}).encode("utf-8")
