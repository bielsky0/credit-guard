from __future__ import annotations

from abc import ABC, abstractmethod


class IdempotencyStore(ABC):
    @abstractmethod
    async def acquire(self, key: str, value: str, ttl_seconds: int = 86400) -> bool: ...

    @abstractmethod
    async def get(self, key: str) -> str | None: ...