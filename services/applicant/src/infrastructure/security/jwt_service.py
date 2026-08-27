"""RS256 JWT token service implementation."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from jose import jwt
from jose.exceptions import JWTError
from src.application.ports.token_service import TokenService


class JWTService(TokenService):
    """RS256 implementation of TokenService."""

    ALGORITHM = "RS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 15
    REFRESH_TOKEN_EXPIRE_DAYS = 7

    def __init__(
        self,
        private_key_path: str | None = None,
        public_key_path: str | None = None,
    ) -> None:
        keys_dir = Path(os.getenv("KEYS_DIR", "/app/keys"))
        private_key_path = private_key_path or str(keys_dir / "private_key.pem")
        public_key_path = public_key_path or str(keys_dir / "public_key.pem")
        self._private_key = Path(private_key_path).read_bytes()
        self._public_key = Path(public_key_path).read_bytes()

    def create_access_token(self, applicant_id: UUID, email: str) -> str:
        now = datetime.now(timezone.utc)
        expire = now + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)
        payload = {
            "sub": str(applicant_id),
            "email": email,
            "type": "access",
            "iat": int(now.timestamp()),
            "exp": int(expire.timestamp()),
        }
        return jwt.encode(payload, self._private_key, algorithm=self.ALGORITHM)

    def create_refresh_token(self, applicant_id: UUID) -> str:
        now = datetime.now(timezone.utc)
        expire = now + timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)
        payload = {
            "sub": str(applicant_id),
            "type": "refresh",
            "iat": int(now.timestamp()),
            "exp": int(expire.timestamp()),
        }
        return jwt.encode(payload, self._private_key, algorithm=self.ALGORITHM)

    def decode_access_token(self, token: str) -> tuple[UUID, str]:
        try:
            payload = jwt.decode(token, self._public_key, algorithms=[self.ALGORITHM])
            if payload.get("type") != "access":
                raise JWTError("Invalid token type")
            applicant_id = UUID(payload["sub"])
            email = payload["email"]
            return applicant_id, email
        except (JWTError, KeyError, ValueError) as e:
            raise JWTError(f"Invalid access token: {e}") from e

    def get_access_token_expiry(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(minutes=self.ACCESS_TOKEN_EXPIRE_MINUTES)

    def get_refresh_token_expiry(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(days=self.REFRESH_TOKEN_EXPIRE_DAYS)