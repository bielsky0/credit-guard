"""DTOs for Applicant Service use cases."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class RegisterRequest(BaseModel):
    """Request to register a new applicant."""

    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)


class LoginRequest(BaseModel):
    """Request to login."""

    email: EmailStr
    password: str


class RefreshTokenRequest(BaseModel):
    """Request to refresh access token."""

    refresh_token: str


class TokenResponse(BaseModel):
    """Response with access and refresh tokens."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds until access token expires


class ApplicantResponse(BaseModel):
    """Applicant profile response."""

    id: UUID
    email: EmailStr
    first_name: str
    last_name: str
    created_at: datetime

    class Config:
        from_attributes = True