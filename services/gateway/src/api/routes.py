"""Route table for the gateway (spec §4.1).

Each entry binds a public path pattern to an internal service URL and declares
whether JWT auth is required and whether rate limiting applies.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    path_prefix: str
    base_url: str
    requires_auth: bool
    rate_limit: bool
    public: bool = False
    rate_key_prefix: str | None = None
    rate_window_seconds: int = 0
    rate_limit_count: int = 0
