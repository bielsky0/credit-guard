"""Unit tests for the sliding-window rate limiter using a fake Redis."""

from __future__ import annotations

from typing import Any

from src.infrastructure.rate_limit import SlidingWindowRateLimiter


class FakePipe:
    """In-process simulation of a Redis pipeline (transactions disabled, ordered)."""

    def __init__(self, store: dict[str, list[float]]) -> None:
        self._store = store
        self._ops: list[Any] = []

    def zremrangebyscore(self, key: str, min_: float, max_: float) -> FakePipe:
        self._ops.append(("zrem", key, min_, max_))
        return self

    def zcard(self, key: str) -> FakePipe:
        self._ops.append(("zcard", key))
        return self

    def zadd(self, key: str, mapping: dict[str, float]) -> FakePipe:
        self._ops.append(("zadd", key, list(mapping.values())))
        return self

    def expire(self, key: str, seconds: int) -> FakePipe:
        self._ops.append(("expire", key, seconds))
        return self

    async def __aenter__(self) -> FakePipe:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass  # mutations happen inside execute()

    async def execute(self) -> list[float]:
        results: list[float] = []
        for op in self._ops:
            kind = op[0]
            if kind == "zrem":
                key: str = op[1]
                lo: float = float(op[2])
                hi: float = float(op[3])
                before = len(self._store.get(key, []))
                self._store[key] = [v for v in self._store.get(key, []) if not (lo <= v <= hi)]
                results.append(float(before - len(self._store[key])))
            elif kind == "zcard":
                results.append(float(len(self._store.get(op[1], []))))
            elif kind == "zadd":
                key = op[1]
                self._store.setdefault(key, []).extend(op[2])
                results.append(1.0)
            elif kind == "expire":
                results.append(1.0)
        return results


class FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, list[float]] = {}

    def pipeline(self, **_: object) -> FakePipe:
        return FakePipe(self._store)


def _limiter() -> SlidingWindowRateLimiter:
    return SlidingWindowRateLimiter(FakeRedis())  # type: ignore[arg-type]


async def test_allows_up_to_limit() -> None:
    limiter = _limiter()
    results = [await limiter.allowed("loans:u1", 3, 600) for _ in range(3)]
    assert results == [True, True, True]


async def test_exceeding_limit_is_blocked() -> None:
    limiter = _limiter()
    for _ in range(3):
        assert await limiter.allowed("auth:1.2.3.4", 3, 60) is True
    assert await limiter.allowed("auth:1.2.3.4", 3, 60) is False


async def test_different_keys_do_not_interfere() -> None:
    limiter = _limiter()
    assert await limiter.allowed("auth:1.2.3.4", 2, 60) is True
    assert await limiter.allowed("auth:1.2.3.4", 2, 60) is True
    assert await limiter.allowed("auth:5.6.7.8", 2, 60) is True
    assert await limiter.allowed("auth:1.2.3.4", 2, 60) is False
    assert await limiter.allowed("auth:5.6.7.8", 2, 60) is True  # different key still has room
