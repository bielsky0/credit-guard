"""Rate limiting backed by Redis (spec §8).

Sliding window implemented with two Redis sorted-set commands per check:
`ZREMRANGEBYSCORE` to prune expired entries, then `ZCARD` to count windows in
the current interval. This is O(log n) and avoids a Lua script while still being
tolerant of clock skew across clients.
"""

from __future__ import annotations

import time
from typing import Protocol

from redis.asyncio import Redis


class RateLimiter(Protocol):
    async def allowed(self, key: str, limit: int, window_seconds: int) -> bool:
        """Return True if the request passes, False if it exceeds the limit."""
        ...


class SlidingWindowRateLimiter:
    """Redis-backed sliding window rate limiter."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def allowed(self, key: str, limit: int, window_seconds: int) -> bool:
        now = time.time()
        window_start = now - window_seconds
        zset_key = f"ratelimit:{key}"

        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(zset_key, 0, window_start)
            pipe.zcard(zset_key)
            pipe.zadd(zset_key, {str(now): now})
            pipe.expire(zset_key, window_seconds)
            result = await pipe.execute()

        count = int(result[1])
        return count < limit
