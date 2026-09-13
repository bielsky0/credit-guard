"""Unit tests for catch-all route matching (slash-less `{path:path}` params)."""

from __future__ import annotations

from src.api.proxy_router import _match_route, init_gateway
from src.core.config import Settings


def _init() -> None:
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        jwt_public_key_path="/dev/null",
    )

    class FakeRedis:
        pass

    init_gateway(settings, FakeRedis())  # type: ignore[arg-type]


def test_matches_without_leading_slash() -> None:
    _init()
    route = _match_route("api/v1/auth/login")
    assert route is not None
    assert route.path_prefix == "/api/v1/auth/login"


def test_matches_loans_subpath() -> None:
    _init()
    route = _match_route("api/v1/loans/123")
    assert route is not None
    assert route.path_prefix == "/api/v1/loans"


def test_unknown_path_returns_none() -> None:
    _init()
    assert _match_route("api/v1/nope") is None
