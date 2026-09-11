"""Gateway configuration loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the gateway service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    gateway_port: int = 8000

    jwt_public_key_path: str = "/app/keys/public_key.pem"

    applicant_service_url: str = "http://applicant:8001"
    loan_service_url: str = "http://loan-application:8002"
    document_service_url: str = "http://document:8003"

    redis_url: str = "redis://localhost:6380/0"

    rate_limit_auth_per_minute: int = 5
    rate_limit_loans_per_10_min: int = 3

    @property
    def jwt_public_key(self) -> bytes:
        return Path(self.jwt_public_key_path).read_bytes()


@lru_cache
def get_settings() -> Settings:
    return Settings()
