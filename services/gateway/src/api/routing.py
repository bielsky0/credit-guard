"""Builds the gateway route table from runtime settings (spec §4.1).

Ordering matters: the catch-all matches the longest prefix. Public routes
(register/login/refresh/webhooks) need no JWT; authenticated routes do. Rate
limits come from the spec §8 (auth: 5/min/IP, loans: 3/10min/user).
"""

from __future__ import annotations

from src.api.routes import Route
from src.core.config import Settings


def build_routes(settings: Settings) -> list[Route]:
    return [
        Route(
            path_prefix="/api/v1/auth/register",
            base_url=settings.applicant_service_url,
            requires_auth=False,
            rate_limit=True,
            public=True,
            rate_key_prefix="auth",
            rate_window_seconds=60,
            rate_limit_count=settings.rate_limit_auth_per_minute,
        ),
        Route(
            path_prefix="/api/v1/auth/login",
            base_url=settings.applicant_service_url,
            requires_auth=False,
            rate_limit=True,
            public=True,
            rate_key_prefix="auth",
            rate_window_seconds=60,
            rate_limit_count=settings.rate_limit_auth_per_minute,
        ),
        Route(
            path_prefix="/api/v1/auth/refresh",
            base_url=settings.applicant_service_url,
            requires_auth=False,
            rate_limit=False,
            public=True,
        ),
        Route(
            path_prefix="/api/v1/me",
            base_url=settings.applicant_service_url,
            requires_auth=True,
            rate_limit=False,
        ),
        Route(
            path_prefix="/api/v1/loans",
            base_url=settings.loan_service_url,
            requires_auth=True,
            rate_limit=True,
            rate_key_prefix="loans",
            rate_window_seconds=600,
            rate_limit_count=settings.rate_limit_loans_per_10_min,
        ),
        Route(
            path_prefix="/api/v1/loans/{id}",
            base_url=settings.loan_service_url,
            requires_auth=True,
            rate_limit=False,
        ),
        Route(
            path_prefix="/api/v1/loans/{id}/documents",
            base_url=settings.document_service_url,
            requires_auth=True,
            rate_limit=False,
        ),
        Route(
            path_prefix="/api/v1/loans/{id}/events",
            base_url="",  # handled locally as SSE (Etap 4)
            requires_auth=True,
            rate_limit=False,
        ),
        Route(
            path_prefix="/api/v1/webhooks/stripe",
            base_url=settings.loan_service_url,
            requires_auth=False,
            rate_limit=False,
            public=True,
        ),
    ]
