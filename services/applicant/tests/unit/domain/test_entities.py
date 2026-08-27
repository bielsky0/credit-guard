"""Unit tests for domain entities."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from src.domain.entities import Applicant, RefreshToken


class TestApplicant:
    def test_create_applicant(self) -> None:
        applicant = Applicant(
            email="test@example.com",
            password_hash="hashed_password",
            first_name="John",
            last_name="Doe",
        )

        assert applicant.email == "test@example.com"
        assert applicant.first_name == "John"
        assert applicant.last_name == "Doe"
        assert applicant.full_name == "John Doe"
        assert applicant.id is not None
        assert isinstance(applicant.created_at, datetime)
        assert isinstance(applicant.updated_at, datetime)

    def test_update_timestamp(self) -> None:
        applicant = Applicant(
            email="test@example.com",
            password_hash="hashed_password",
            first_name="John",
            last_name="Doe",
        )
        old_updated = applicant.updated_at

        # Small delay to ensure timestamp changes
        import time
        time.sleep(0.01)

        applicant.update_timestamp()

        assert applicant.updated_at > old_updated


class TestRefreshToken:
    def test_create_refresh_token(self) -> None:
        token = RefreshToken(
            applicant_id=uuid4(),
            token_hash="hashed_token",
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )

        assert token.applicant_id is not None
        assert token.token_hash == "hashed_token"
        assert token.revoked_at is None

    def test_is_valid_when_valid(self) -> None:
        token = RefreshToken(
            applicant_id=uuid4(),
            token_hash="hashed_token",
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )

        assert token.is_valid() is True

    def test_is_valid_when_expired(self) -> None:
        token = RefreshToken(
            applicant_id=uuid4(),
            token_hash="hashed_token",
            expires_at=datetime.now(timezone.utc) - timedelta(days=1),
        )

        assert token.is_valid() is False

    def test_is_valid_when_revoked(self) -> None:
        token = RefreshToken(
            applicant_id=uuid4(),
            token_hash="hashed_token",
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )
        token.revoke()

        assert token.is_valid() is False
        assert token.revoked_at is not None

    def test_revoke(self) -> None:
        token = RefreshToken(
            applicant_id=uuid4(),
            token_hash="hashed_token",
            expires_at=datetime.now(timezone.utc) + timedelta(days=7),
        )

        token.revoke()

        assert token.revoked_at is not None
        assert token.is_valid() is False