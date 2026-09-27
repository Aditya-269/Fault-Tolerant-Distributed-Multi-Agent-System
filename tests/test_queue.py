"""Tests for RabbitMQ task queue abstraction and lifecycle."""

import sys
from pathlib import Path
import uuid
import pika
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher


@pytest.fixture
def queue_name():
    """Generate an isolated test queue name and delete it after the test."""
    name = f"test_queue_{uuid.uuid4().hex[:8]}"
    yield name

    # Teardown: delete test queue if exists
    try:
        conn = create_connection()
        ch = conn.channel()
        ch.queue_delete(queue=name)
        conn.close()
    except Exception:
        pass


def test_durable_queue_declaration(queue_name: str):
    """Verify that TaskPublisher and TaskConsumer declare a durable queue."""
    with TaskPublisher(queue_name=queue_name) as pub:
        # Declare queue again with passive=True to inspect existing queue attributes
        res = pub.channel.queue_declare(queue=queue_name, passive=True)
        assert res.method.queue == queue_name

    with TaskConsumer(queue_name=queue_name, prefetch_count=1) as sub:
        assert sub.prefetch_count == 1
        res = sub.channel.queue_declare(queue=queue_name, passive=True)
        assert res.method.queue == queue_name


def test_publish_consume_ack_lifecycle(queue_name: str):
    """Integration test verifying: publish task -> consume task -> acknowledge task."""
    test_task_id = f"task-{uuid.uuid4()}"

    with TaskPublisher(queue_name=queue_name) as publisher:
        with TaskConsumer(queue_name=queue_name, prefetch_count=1) as consumer:
            # 1. Publish task
            publisher.publish(task_id=test_task_id, extra={"priority": "high"})

            # 2. Consume task
            message = consumer.consume_one(timeout=5.0)
            assert message is not None
            assert message.task_id == test_task_id
            assert message.body.get("priority") == "high"

            # 3. Acknowledge task
            message.ack()

            # 4. Verify queue is now empty
            subsequent_message = consumer.consume_one(timeout=0.2)
            assert subsequent_message is None


def test_nack_requeue_behavior(queue_name: str):
    """Verify that nack with requeue=True returns message to the queue."""
    test_task_id = f"task-nack-{uuid.uuid4().hex[:6]}"

    with TaskPublisher(queue_name=queue_name) as publisher:
        with TaskConsumer(queue_name=queue_name, prefetch_count=1) as consumer:
            publisher.publish(task_id=test_task_id)

            # Consume and reject with requeue=True
            first_msg = consumer.consume_one(timeout=5.0)
            assert first_msg is not None
            assert first_msg.task_id == test_task_id
            first_msg.nack(requeue=True)

            # Consume again to verify redelivery
            redelivered = consumer.consume_one(timeout=5.0)
            assert redelivered is not None
            assert redelivered.task_id == test_task_id
            redelivered.ack()


def test_consume_timeout_empty_queue(queue_name: str):
    """Verify consumer returns None on empty queue without hanging."""
    with TaskConsumer(queue_name=queue_name, prefetch_count=1) as consumer:
        msg = consumer.consume_one(timeout=0.3)
        assert msg is None


def test_callback_consuming(queue_name: str):
    """Verify start_consuming callback interface and clean stop."""
    test_task_id = f"task-callback-{uuid.uuid4().hex[:6]}"
    received: list[str] = []

    with TaskPublisher(queue_name=queue_name) as publisher:
        publisher.publish(task_id=test_task_id)

    with TaskConsumer(queue_name=queue_name, prefetch_count=1) as consumer:
        def on_msg(msg):
            received.append(msg.task_id)
            msg.ack()
            consumer.stop_consuming()

        consumer.start_consuming(callback=on_msg)

    assert received == [test_task_id]

