"""Task consumer for dequeuing tasks from RabbitMQ."""

import json
import time
from typing import Callable, Optional
import pika

from config import settings
from queue.connection import create_connection
from queue.message import QueueMessage


class TaskConsumer:
    """Consumes tasks from a durable RabbitMQ queue with prefetch=1 and manual acknowledgements."""

    def __init__(
        self,
        connection: Optional[pika.BlockingConnection] = None,
        queue_name: Optional[str] = None,
        prefetch_count: int = 1,
    ) -> None:
        """Initialize the TaskConsumer.

        Args:
            connection: Optional existing pika BlockingConnection.
            queue_name: Target queue name (defaults to settings.rabbitmq.queue_name).
            prefetch_count: Number of unacknowledged messages allowed per worker (default: 1).
        """
        self._owns_connection = connection is None
        self.connection = connection or create_connection()
        self.channel = self.connection.channel()
        self.queue_name = queue_name or settings.rabbitmq.queue_name
        self.prefetch_count = prefetch_count

        # Ensure queue is durable
        self.channel.queue_declare(queue=self.queue_name, durable=True)

        # Ensure worker only receives prefetch_count unacknowledged tasks
        self.channel.basic_qos(prefetch_count=self.prefetch_count)

    def consume_one(self, timeout: Optional[float] = 5.0) -> Optional[QueueMessage]:
        """Fetch a single message synchronously within an optional timeout.

        Args:
            timeout: Maximum seconds to wait for a message. If None, checks once.

        Returns:
            QueueMessage if a message was retrieved, None otherwise.
        """
        deadline = time.time() + (timeout if timeout is not None else 0.0)

        while True:
            method_frame, properties, body = self.channel.basic_get(
                queue=self.queue_name,
                auto_ack=False,
            )
            if method_frame:
                payload = json.loads(body.decode("utf-8"))
                return QueueMessage(
                    task_id=payload.get("task_id", ""),
                    delivery_tag=method_frame.delivery_tag,
                    channel=self.channel,
                    body=payload,
                )

            if timeout is None or time.time() >= deadline:
                return None

            time.sleep(0.05)

    def start_consuming(
        self,
        callback: Callable[[QueueMessage], None],
    ) -> None:
        """Start a blocking consumption loop invoking callback for each message.

        Args:
            callback: Function taking a QueueMessage instance.
        """

        def _on_message(channel, method, properties, body):
            payload = json.loads(body.decode("utf-8"))
            msg = QueueMessage(
                task_id=payload.get("task_id", ""),
                delivery_tag=method.delivery_tag,
                channel=channel,
                body=payload,
            )
            callback(msg)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=_on_message,
            auto_ack=False,
        )
        self.channel.start_consuming()

    def stop_consuming(self) -> None:
        """Stop the blocking consumption loop."""
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
        except Exception:
            pass

    def purge_queue(self) -> int:
        """Purge all messages from the queue. Returns count of purged messages."""
        return self.channel.queue_purge(queue=self.queue_name).method.message_count

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

    def __enter__(self) -> "TaskConsumer":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
