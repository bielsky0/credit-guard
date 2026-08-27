"""Domain entities for Applicant Service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4


@dataclass
class Applicant:
    """Applicant aggregate root."""

    email: str
    password_hash: str
    first_name: str
    last_name: str
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def update_timestamp(self) -> None:
        """Update the updated_at timestamp."""
        self.updated_at = datetime.now(timezone.utc)

    @property
    def full_name(self) -> str:
        """Return full name."""
        return f"{self.first_name} {self.last_name}"


@dataclass
class RefreshToken:
    """Refresh token entity (stored hashed)."""

    applicant_id: UUID
    token_hash: str
    expires_at: datetime
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    revoked_at: Optional[datetime] = None

    def is_valid(self, now: Optional[datetime] = None) -> bool:
        """Check if token is valid (not expired, not revoked)."""
        if now is None:
            now = datetime.now(timezone.utc)
        return self.revoked_at is None and self.expires_at > now

    def revoke(self) -> None:
        """Mark token as revoked."""
        self.revoked_at = datetime.now(timezone.utc)