"""Catch-all reverse proxy endpoint (spec §4.1).

One `/{path:path}` route inspects the request against the route table, applies
JWT auth and rate limiting where required, forwards the request to the matching
internal service, and streams the response back to the client. Internal-service
routing and header rewriting happen inside `ProxyService`.
"""

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from redis.asyncio import Redis

from src.api.routes import Route
from src.api.routing import build_routes
from src.core.config import Settings
from src.infrastructure.rate_limit import SlidingWindowRateLimiter
from src.services.proxy import ProxyService
from src.services.token import InvalidTokenError, TokenValidator

router = APIRouter()

_routes: list[Route] = []
_proxy: ProxyService | None = None
_validator: TokenValidator | None = None
_limiter: SlidingWindowRateLimiter | None = None


def init_gateway(settings: Settings, redis: Redis) -> None:
    """Wire gateway singletons. Called once at startup (see main.py)."""
    global _routes, _proxy, _validator, _limiter

    _routes = build_routes(settings)
    _proxy = ProxyService()
    _validator = TokenValidator(settings.jwt_public_key)
    _limiter = SlidingWindowRateLimiter(redis)


async def close_gateway() -> None:
    global _proxy
    if _proxy is not None:
        await _proxy.aclose()
        _proxy = None


def _match_route(path: str) -> Route | None:
    """Return the most specific (longest-prefix) route matching the request path."""
    candidates = [
        r for r in _routes if path == r.path_prefix or path.startswith(r.path_prefix + "/")
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
    if _proxy is None or _validator is None:
        return JSONResponse({"detail": "Gateway not ready"}, status_code=503)

    route = _match_route(path)
    if route is None or route.base_url == "":
        # Not a routable path (or handled later, e.g. SSE in Etap 4).
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    client_headers = dict(request.headers.items())

    # JWT auth where required.
    applicant_id: str | None = None
    if route.requires_auth:
        try:
            uid = _validator.validate(client_headers.get("authorization"))
            applicant_id = str(uid)
        except InvalidTokenError:
            return JSONResponse(
                {"detail": "Unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
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
    correlation_id = client_headers.get("x-correlation-id") or str(uuid4())
    result = await _proxy.forward(
        method=request.method,
        path=path,
        query_string=request.scope.get("query_string", b""),
        base_url=route.base_url,
        client_headers=client_headers,
        body=request.stream(),
        applicant_id=applicant_id,
        correlation_id=correlation_id,
    )

    resp_headers = {
        k: v
        for k, v in result["response_headers"].items()
        if k.lower() not in {"transfer-encoding", "connection", "content-length"}
    }
    if correlation_id:
        resp_headers["X-Correlation-ID"] = correlation_id

    return StreamingResponse(
        result["body_iterator"],
        status_code=result["status_code"],
        headers=resp_headers,
    )
