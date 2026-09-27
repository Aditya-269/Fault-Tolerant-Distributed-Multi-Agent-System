"""Configuration module for the distributed system.

Loads environment variables from a .env file if available, providing typed and
defaulted connection parameters for RabbitMQ and Redis.
"""

from dataclasses import dataclass, field
import os
from pathlib import Path
from dotenv import load_dotenv

# Locate and load .env file from project root
_ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH)


@dataclass(frozen=True)
class RabbitMQConfig:
    """RabbitMQ connection configuration."""

    host: str = field(default_factory=lambda: os.getenv("RABBITMQ_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_PORT", "5672")))
    user: str = field(default_factory=lambda: os.getenv("RABBITMQ_USER", "guest"))
    password: str = field(default_factory=lambda: os.getenv("RABBITMQ_PASSWORD", "guest"))
    queue_name: str = field(
        default_factory=lambda: os.getenv("RABBITMQ_QUEUE", "tasks_queue")
    )


@dataclass(frozen=True)
class RedisConfig:
    """Redis connection configuration."""

    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    db: int = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))
    password: str | None = field(
        default_factory=lambda: os.getenv("REDIS_PASSWORD") or None
    )


@dataclass(frozen=True)
class Settings:
    """Global system settings."""

    rabbitmq: RabbitMQConfig = field(default_factory=RabbitMQConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)


# Default singleton instance for convenience
settings = Settings()
