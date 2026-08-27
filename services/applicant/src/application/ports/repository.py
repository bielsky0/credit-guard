"""Repository ports for Applicant Service."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from uuid import UUID

from src.domain.entities import Applicant, RefreshToken


class ApplicantRepository(ABC):
    """Port for applicant persistence."""

    @abstractmethod
    async def save(self, applicant: Applicant) -> None:
        """Save applicant."""

    @abstractmethod
    async def get_by_id(self, applicant_id: UUID) -> Optional[Applicant]:
        """Get applicant by ID."""

    @abstractmethod
    async def get_by_email(self, email: str) -> Optional[Applicant]:
        """Get applicant by email."""

    @abstractmethod
    async def exists_by_email(self, email: str) -> bool:
        """Check if applicant with email exists."""


class RefreshTokenRepository(ABC):
    """Port for refresh token persistence."""

    @abstractmethod
    async def save(self, token: RefreshToken) -> None:
        """Save refresh token."""

    @abstractmethod
    async def get_by_hash(self, token_hash: str) -> Optional[RefreshToken]:
        """Get refresh token by hash."""

    @abstractmethod
    async def revoke_all_for_applicant(self, applicant_id: UUID) -> None:
        """Revoke all refresh tokens for applicant (e.g., on password change)."""

    @abstractmethod
    async def update(self, token: RefreshToken) -> None:
        """Update existing refresh token."""

    @abstractmethod
    async def get_all_valid(self, now: datetime) -> list[RefreshToken]:
        """Get all valid (not expired, not revoked) refresh tokens."""