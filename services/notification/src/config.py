"""Notification Service configuration (env-only, no secrets in code)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9094"
    kafka_topic: str = "loan.status.changed.v1"
    kafka_group_id: str = "notification-service"
    redis_url: str = "redis://localhost:6380/0"
