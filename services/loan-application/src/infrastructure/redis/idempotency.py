from __future__ import annotations

import redis.asyncio as redis

from src.application.ports.idempotency import IdempotencyStore


class RedisIdempotencyStore(IdempotencyStore):
    def __init__(self, redis_client: redis.Redis) -> None:
        self._redis = redis_client

    async def acquire(self, key: str, value: str, ttl_seconds: int = 86400) -> bool:
        result = await self._redis.set(
            f"idempotency:{key}", value, nx=True, ex=ttl_seconds
        )
        return result is not None

    async def get(self, key: str) -> str | None:
        result = await self._redis.get(f"idempotency:{key}")
        if result is None:
            return None
        if isinstance(result, bytes):
            return result.decode()
        return str(result)