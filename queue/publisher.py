"""Task publisher for enqueuing tasks to RabbitMQ."""

import json
from typing import Any, Optional
import pika

from config import settings
from queue.connection import create_connection


class TaskPublisher:
    """Publishes task messages to a durable RabbitMQ queue with persistent delivery."""

    def __init__(
        self,
        connection: Optional[pika.BlockingConnection] = None,
        queue_name: Optional[str] = None,
    ) -> None:
        """Initialize the TaskPublisher.

        Args:
            connection: Optional existing pika BlockingConnection.
            queue_name: Target queue name (defaults to settings.rabbitmq.queue_name).
        """
        self._owns_connection = connection is None
        self.connection = connection or create_connection()
        self.channel = self.connection.channel()
        self.queue_name = queue_name or settings.rabbitmq.queue_name

        # Ensure queue is durable
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def publish(
        self,
        task_id: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        """Publish a task by its ID with persistent delivery mode.

        Args:
            task_id: Unique task identifier.
            extra: Optional additional metadata to include in the payload.
        """
        payload: dict[str, Any] = {"task_id": task_id}
        if extra:
            payload.update(extra)

        body = json.dumps(payload).encode("utf-8")

        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
            ),
        )

    def close(self) -> None:
        """Close channel and connection cleanly."""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass

        if self._owns_connection:
            try:
                if self.connection and self.connection.is_open:
                    self.connection.close()
            except Exception:
                pass

    def __enter__(self) -> "TaskPublisher":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
