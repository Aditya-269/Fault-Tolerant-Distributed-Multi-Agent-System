"""RabbitMQ connection factory."""

from typing import Optional
import pika

from config import RabbitMQConfig, settings


def create_connection(
    config: Optional[RabbitMQConfig] = None,
) -> pika.BlockingConnection:
    """Create and return a new pika BlockingConnection.

    Args:
        config: RabbitMQ connection configuration (defaults to global settings).

    Returns:
        An open pika.BlockingConnection.
    """
    cfg = config or settings.rabbitmq
    credentials = pika.PlainCredentials(username=cfg.user, password=cfg.password)
    parameters = pika.ConnectionParameters(
        host=cfg.host,
        port=cfg.port,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2,
        socket_timeout=5,
    )
    return pika.BlockingConnection(parameters)
