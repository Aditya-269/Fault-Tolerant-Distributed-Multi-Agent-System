"""Queue message representation abstracting broker acknowledgement mechanisms."""

from dataclasses import dataclass
from typing import Any
import pika.channel


@dataclass
class QueueMessage:
    """Encapsulates a task queue message with explicit manual acknowledgement capabilities.

    Isolates RabbitMQ delivery tags and channel interactions from business logic.
    """

    task_id: str
    delivery_tag: int
    channel: pika.channel.Channel
    body: dict[str, Any]

    def ack(self) -> None:
        """Acknowledge message processing completion to the broker."""
        self.channel.basic_ack(delivery_tag=self.delivery_tag)

    def nack(self, requeue: bool = True) -> None:
        """Reject message, optionally requesting redelivery."""
        self.channel.basic_nack(delivery_tag=self.delivery_tag, requeue=requeue)
