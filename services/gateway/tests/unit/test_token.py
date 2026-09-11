"""Unit tests for JWT token validation in the gateway."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from jose import jwt
from src.services.token import InvalidTokenError, TokenValidator

KeyPair = tuple[bytes, bytes]


@pytest.fixture()
def keypair() -> KeyPair:
    from cryptography.hazmat.primitives.asymmetric import rsa

    key: RSAPrivateKey = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem: bytes = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem: bytes = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem


@pytest.fixture()
def validator(keypair: KeyPair) -> TokenValidator:
    _, public_pem = keypair
    return TokenValidator(public_pem)


def _access_token(keypair: KeyPair, sub: str) -> str:
    private_pem, _ = keypair
    payload: dict[str, Any] = {"sub": sub, "email": "a@b.c", "type": "access"}
    return jwt.encode(payload, private_pem, algorithm="RS256")


def test_valid_access_token_returns_applicant_id(
    keypair: KeyPair, validator: TokenValidator
) -> None:
    uid = uuid4()
    token = _access_token(keypair, str(uid))
    assert validator.validate(f"Bearer {token}") == uid


def test_missing_authorization_header_raises(validator: TokenValidator) -> None:
    with pytest.raises(InvalidTokenError):
        validator.validate(None)


def test_non_bearer_header_raises(validator: TokenValidator) -> None:
    with pytest.raises(InvalidTokenError):
        validator.validate("Token abc")


def test_refresh_token_is_rejected(keypair: KeyPair, validator: TokenValidator) -> None:
    private_pem, _ = keypair
    payload: dict[str, Any] = {"sub": str(uuid4()), "type": "refresh"}
    refresh = jwt.encode(payload, private_pem, algorithm="RS256")
    with pytest.raises(InvalidTokenError):
        validator.validate(f"Bearer {refresh}")


def test_garbage_token_raises(validator: TokenValidator) -> None:
    with pytest.raises(InvalidTokenError):
        validator.validate("Bearer not.a.token")
