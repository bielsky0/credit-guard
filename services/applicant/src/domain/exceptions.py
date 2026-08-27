"""Domain exceptions for Applicant Service."""

from __future__ import annotations


class ApplicantDomainError(Exception):
    """Base exception for applicant domain errors."""


class ApplicantNotFound(ApplicantDomainError):
    """Raised when applicant is not found."""

    def __init__(self, applicant_id: str) -> None:
        self.applicant_id = applicant_id
        super().__init__(f"Applicant not found: {applicant_id}")


class EmailAlreadyRegistered(ApplicantDomainError):
    """Raised when email is already registered."""

    def __init__(self, email: str) -> None:
        self.email = email
        super().__init__(f"Email already registered: {email}")


class InvalidCredentials(ApplicantDomainError):
    """Raised when login credentials are invalid."""

    def __init__(self) -> None:
        super().__init__("Invalid email or password")


class InvalidRefreshToken(ApplicantDomainError):
    """Raised when refresh token is invalid or expired."""

    def __init__(self) -> None:
        super().__init__("Invalid or expired refresh token")


class RefreshTokenRevoked(ApplicantDomainError):
    """Raised when refresh token has been revoked (used already)."""

    def __init__(self) -> None:
        super().__init__("Refresh token has been revoked")