"""Argon2id password hasher implementation."""

from __future__ import annotations

import argon2
from argon2 import PasswordHasher as Argon2PasswordHasher
from src.application.ports.password_hasher import PasswordHasher as PasswordHasherPort


class Argon2Hasher(PasswordHasherPort):
    """Argon2id implementation of PasswordHasher."""

    def __init__(self) -> None:
        self._hasher = Argon2PasswordHasher(
            time_cost=3,
            memory_cost=65536,
            parallelism=4,
            hash_len=32,
            salt_len=16,
        )

    def hash(self, password: str) -> str:
        return self._hasher.hash(password)

    def verify(self, password: str, password_hash: str) -> bool:
        try:
            self._hasher.verify(password_hash, password)
            return True
        except argon2.exceptions.VerifyMismatchError:
            return False
        except argon2.exceptions.HashingError:
            return False