"""Catch-all reverse proxy endpoint (spec §4.1).

One `/{path:path}` route inspects the request against the route table, applies
JWT auth and rate limiting where required, forwards the request to the matching
internal service, and streams the response back to the client. Internal-service
routing and header rewriting happen inside `ProxyService`.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from redis.asyncio import Redis

from src.api.routes import Route
from src.api.routing import build_routes
from src.core.config import Settings
from src.infrastructure.rate_limit import SlidingWindowRateLimiter
from src.services.cookies import (
    build_cookie_auth_response,
    build_logout_response,
    build_refresh_body_from_cookie,
    is_cookie_auth_path,
    is_logout_path,
    sanitize_auth_body,
)
from src.services.proxy import ProxyService
from src.services.token import InvalidTokenError, TokenValidator

router = APIRouter()

_routes: list[Route] = []
_proxy: ProxyService | None = None
_validator: TokenValidator | None = None
_limiter: SlidingWindowRateLimiter | None = None
_settings: Settings | None = None


def init_gateway(settings: Settings, redis: Redis) -> None:
    """Wire gateway singletons. Called once at startup (see main.py)."""
    global _routes, _proxy, _validator, _limiter, _settings

    _routes = build_routes(settings)
    _proxy = ProxyService()
    _validator = TokenValidator(settings.jwt_public_key)
    _limiter = SlidingWindowRateLimiter(redis)
    _settings = settings


async def close_gateway() -> None:
    global _proxy
    if _proxy is not None:
        await _proxy.aclose()
        _proxy = None


def _match_route(path: str) -> Route | None:
    """Return the most specific (longest-prefix) route matching the request path."""
    # Starlette strips the leading slash from `{path:path}` params.
    normalized = path if path.startswith("/") else f"/{path}"
    candidates = [
        r
        for r in _routes
        if normalized == r.path_prefix or normalized.startswith(r.path_prefix + "/")
    ]
    return max(candidates, key=lambda r: len(r.path_prefix)) if candidates else None


def _rate_key(route: Route, client_ip: str, applicant_id: str | None) -> str | None:
    if route.rate_key_prefix == "auth":
        return f"{route.rate_key_prefix}:{client_ip}"
    if route.rate_key_prefix == "loans":
        if applicant_id is None:
            return None
        return f"{route.rate_key_prefix}:{applicant_id}"
    return None


@router.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    include_in_schema=False,
)
async def proxy_endpoint(path: str, request: Request) -> Response:
    if _proxy is None or _validator is None or _settings is None:
        return JSONResponse({"detail": "Gateway not ready"}, status_code=503)

    client_headers = dict(request.headers.items())
    correlation_id = client_headers.get("x-correlation-id") or str(uuid4())

    # Local logout: clear both cookies, never touches upstream.
    if is_logout_path(path):
        return build_logout_response(_settings, correlation_id)

    route = _match_route(path)
    if route is None or route.base_url == "":
        # Not a routable path (SSE is a separate router, registered first).
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    # Refresh is cookie-only (prod-like): the browser sends only the
    # HttpOnly cg_refresh cookie; the gateway injects the upstream body.
    refresh_token_from_cookie: str | None = None
    if path.lstrip("/") == "api/v1/auth/refresh":
        refresh_token_from_cookie = request.cookies.get(_settings.refresh_cookie_name)
        if not refresh_token_from_cookie:
            return JSONResponse(
                {"detail": "Missing refresh cookie"},
                status_code=401,
                headers={"X-Correlation-ID": correlation_id},
            )

    # JWT auth where required: Bearer header first, cg_access cookie fallback
    # (browsers on SSE / credentialed fetch cannot set Authorization).
    applicant_id: str | None = None
    if route.requires_auth:
        try:
            uid = _validator.validate(client_headers.get("authorization"))
            applicant_id = str(uid)
        except InvalidTokenError:
            try:
                uid = _validator.validate_cookie(request.cookies.get(_settings.access_cookie_name))
                applicant_id = str(uid)
            except InvalidTokenError:
                return JSONResponse(
                    {"detail": "Unauthorized"},
                    status_code=401,
                    headers={
                        "WWW-Authenticate": "Bearer",
                        "X-Correlation-ID": correlation_id,
                    },
                )

    # Rate limiting.
    if route.rate_limit and _limiter is not None:
        client_host = request.client.host if request.client is not None else "unknown"
        rk = _rate_key(route, client_host, applicant_id)
        if rk is None:
            return JSONResponse({"detail": "Rate limit not applicable"}, status_code=400)
        allowed = await _limiter.allowed(rk, route.rate_limit_count, route.rate_window_seconds)
        if not allowed:
            return JSONResponse({"detail": "Rate limit exceeded"}, status_code=429)

    # Forward and stream back.
    body: object = request.stream()
    if refresh_token_from_cookie is not None:
        raw = build_refresh_body_from_cookie(refresh_token_from_cookie)

        async def _single_body() -> AsyncIterator[bytes]:
            yield raw

        body = _single_body()
        client_headers["content-type"] = "application/json"

    result = await _proxy.forward(
        method=request.method,
        path=path,
        query_string=request.scope.get("query_string", b""),
        base_url=route.base_url,
        client_headers=client_headers,
        body=body,  # type: ignore[arg-type]
        applicant_id=applicant_id,
        correlation_id=correlation_id,
    )

    # Cookie translation for auth endpoints: buffer the small JSON
    # TokenResponse, set HttpOnly cookies, strip raw JWTs from the body.
    if is_cookie_auth_path(path):
        chunks = [chunk async for chunk in result["body_iterator"]]
        raw_body = b"".join(chunks)
        if 200 <= result["status_code"] < 300:
            safe_body, access, refresh = sanitize_auth_body(raw_body)
            if access is None or refresh is None:
                return JSONResponse(
                    status_code=502,
                    content={"detail": "Invalid auth response from upstream"},
                    headers={"X-Correlation-ID": correlation_id},
                )
            return build_cookie_auth_response(
                status_code=result["status_code"],
                safe_body=safe_body,
                access_token=access,
                refresh_token=refresh,
                settings=_settings,
                correlation_id=correlation_id,
            )
        return Response(
            content=raw_body,
            status_code=result["status_code"],
            media_type=result["response_headers"].get("content-type", "application/json"),
            headers={"X-Correlation-ID": correlation_id},
        )

    resp_headers = {
        k: v
        for k, v in result["response_headers"].items()
        if k.lower() not in {"transfer-encoding", "connection", "content-length", "set-cookie"}
    }
    if correlation_id:
        resp_headers["X-Correlation-ID"] = correlation_id

    return StreamingResponse(
        result["body_iterator"],
        status_code=result["status_code"],
        headers=resp_headers,
    )
