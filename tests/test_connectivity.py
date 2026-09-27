"""Connectivity and infrastructure health tests.

Validates that:
1. Python dependencies can be imported.
2. Configuration loads correctly.
3. RabbitMQ broker is reachable and accepts connections.
4. Redis server is reachable and responds to PING.
"""

import sys
from pathlib import Path

# Add project root to sys.path to enable imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pika
import redis
from config import settings


def test_dependency_imports():
    """Verify essential dependencies import without errors."""
    import pika
    import redis
    import dotenv
    import config

    assert config.settings.rabbitmq.host is not None
    assert config.settings.redis.host is not None


def test_redis_connectivity():
    """Verify that Redis is running and reachable."""
    client = redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        socket_connect_timeout=5,
    )
    # Ping Redis; expects True response
    assert client.ping() is True
    client.close()


def test_rabbitmq_connectivity():
    """Verify that RabbitMQ is running and reachable."""
    credentials = pika.PlainCredentials(
        username=settings.rabbitmq.user,
        password=settings.rabbitmq.password,
    )
    parameters = pika.ConnectionParameters(
        host=settings.rabbitmq.host,
        port=settings.rabbitmq.port,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2,
        socket_timeout=5,
    )
    connection = pika.BlockingConnection(parameters)
    try:
        assert connection.is_open is True
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    print("Running infrastructure connectivity checks...")
    test_dependency_imports()
    print("✓ Dependencies imported successfully.")

    test_redis_connectivity()
    print("✓ Redis is reachable.")

    test_rabbitmq_connectivity()
    print("✓ RabbitMQ is reachable.")

    print("\nAll health and connectivity checks passed!")

