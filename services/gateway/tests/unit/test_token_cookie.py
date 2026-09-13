"""Unit tests for cookie-first JWT validation (Etap 4)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jose import jwt
from src.services.token import InvalidTokenError, TokenValidator


def _validator() -> tuple[TokenValidator, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public_pem = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    return TokenValidator(public_pem.encode()), private_pem


def _token(private_pem: str, sub: str, token_type: str = "access") -> str:
    now = datetime.now(UTC)
    return jwt.encode(
        {
            "sub": sub,
            "type": token_type,
            "iat": now,
            "exp": now + timedelta(minutes=15),
        },
        private_pem,
        algorithm="RS256",
    )


def test_validate_token_raw() -> None:
    validator, private_pem = _validator()
    sub = str(uuid4())
    assert str(validator.validate_token(_token(private_pem, sub))) == sub


def test_validate_cookie_accepts_access_token() -> None:
    validator, private_pem = _validator()
    sub = str(uuid4())
    assert str(validator.validate_cookie(_token(private_pem, sub))) == sub


def test_validate_cookie_rejects_missing() -> None:
    validator, _ = _validator()
    with pytest.raises(InvalidTokenError):
        validator.validate_cookie(None)
    with pytest.raises(InvalidTokenError):
        validator.validate_cookie("")


def test_validate_cookie_rejects_refresh_type() -> None:
    validator, private_pem = _validator()
    with pytest.raises(InvalidTokenError):
        validator.validate_cookie(_token(private_pem, str(uuid4()), token_type="refresh"))


def test_header_still_works() -> None:
    validator, private_pem = _validator()
    sub = str(uuid4())
    assert str(validator.validate(f"Bearer {_token(private_pem, sub)}")) == sub
