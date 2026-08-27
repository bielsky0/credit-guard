"""Password hasher port for Applicant Service."""

from __future__ import annotations

from abc import ABC, abstractmethod


class PasswordHasher(ABC):
    """Port for password hashing."""

    @abstractmethod
    def hash(self, password: str) -> str:
        """Hash a password."""

    @abstractmethod
    def verify(self, password: str, password_hash: str) -> bool:
        """Verify a password against its hash."""