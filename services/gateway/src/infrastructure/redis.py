"""Redis client lifecycle for the gateway."""

from __future__ import annotations

from redis.asyncio import Redis

_redis: Redis | None = None


async def get_redis(url: str) -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(url, decode_responses=False)
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
