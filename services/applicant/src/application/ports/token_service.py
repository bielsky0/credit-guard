"""Token service port for Applicant Service (RS256 JWT)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID


class TokenService(ABC):
    """Port for JWT token operations (RS256)."""

    @abstractmethod
    def create_access_token(self, applicant_id: UUID, email: str) -> str:
        """Create short-lived access token (15 min)."""

    @abstractmethod
    def create_refresh_token(self, applicant_id: UUID) -> str:
        """Create long-lived refresh token (7 days)."""

    @abstractmethod
    def decode_access_token(self, token: str) -> tuple[UUID, str]:
        """Decode and validate access token. Returns (applicant_id, email)."""

    @abstractmethod
    def get_access_token_expiry(self) -> datetime:
        """Get expiry datetime for access token."""

    @abstractmethod
    def get_refresh_token_expiry(self) -> datetime:
        """Get expiry datetime for refresh token."""