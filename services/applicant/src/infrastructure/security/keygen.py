#!/usr/bin/env python3
"""Generate RSA key pair for RS256 JWT signing.

Run once to generate keys:
    python -m src.infrastructure.security.keygen

Keys are saved to KEYS_DIR (default: ./keys/):
- private_key.pem (Applicant Service only)
- public_key.pem (shared with Gateway via volume)
"""

from __future__ import annotations

import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# Use /app/keys in Docker, ./keys locally
KEYS_DIR = Path(os.getenv("KEYS_DIR", "./keys"))
PRIVATE_KEY_PATH = KEYS_DIR / "private_key.pem"
PUBLIC_KEY_PATH = KEYS_DIR / "public_key.pem"


def generate_keys() -> None:
    """Generate RSA 2048-bit key pair."""
    KEYS_DIR.mkdir(parents=True, exist_ok=True)

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    public_key = private_key.public_key()
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    PRIVATE_KEY_PATH.write_bytes(private_pem)
    PUBLIC_KEY_PATH.write_bytes(public_pem)

    # Secure permissions
    PRIVATE_KEY_PATH.chmod(0o600)
    PUBLIC_KEY_PATH.chmod(0o644)

    print("Generated keys:")
    print(f"  Private: {PRIVATE_KEY_PATH}")
    print(f"  Public:  {PUBLIC_KEY_PATH}")


if __name__ == "__main__":
    generate_keys()