"""JWT validation for the gateway (spec §4.1, §8).

The gateway holds only the *public* key (RS256) and uses it to validate a
Bearer access token issued by the Applicant Service. On success it extracts the
applicant ID and binds it as X-User-ID for downstream services.
"""

from __future__ import annotations

from uuid import UUID

from jose import jwt
from jose.exceptions import JWTError

from src.core.config import Settings

ALGORITHM = "RS256"


class InvalidTokenError(Exception):
    """Raised when the access token is missing or invalid."""


class TokenValidator:
    """Validates RS256 access tokens using the public key."""

    def __init__(self, public_key: bytes) -> None:
        self._public_key = public_key

    def validate_token(self, token: str) -> UUID:
        """Validate a raw JWT string and return the applicant UUID.

        Raises InvalidTokenError for any missing/malformed/expired token.
        """
        if not token:
            raise InvalidTokenError("Missing token")
        try:
            payload = jwt.decode(
                token,
                self._public_key,
                algorithms=[ALGORITHM],
            )
            if payload.get("type") != "access":
                raise InvalidTokenError("Not an access token")
            return UUID(payload["sub"])
        except (JWTError, KeyError, ValueError) as e:
            raise InvalidTokenError(f"Invalid token: {e}") from e

    def validate(self, authorization: str | None) -> UUID:
        """Parse `Authorization: Bearer <token>` and return the applicant UUID.

        Raises InvalidTokenError for any missing/malformed/expired token.
        """
        if authorization is None or not authorization.startswith("Bearer "):
            raise InvalidTokenError("Missing or invalid Authorization header")
        return self.validate_token(authorization[7:])

    def validate_cookie(self, cookie_value: str | None) -> UUID:
        """Validate the access token carried in the HttpOnly cookie."""
        if not cookie_value:
            raise InvalidTokenError("Missing access cookie")
        return self.validate_token(cookie_value)


def build_token_validator(settings: Settings) -> TokenValidator:
    return TokenValidator(settings.jwt_public_key)
