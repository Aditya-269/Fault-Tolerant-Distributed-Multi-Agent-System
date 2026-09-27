"""Tests for the Coordinator module."""

import sys
from pathlib import Path
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from coordinator.coordinator import Coordinator
from models.task import TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from state.task_store import TaskStore


@pytest.fixture
def coordinator_env():
    """Set up an isolated test coordinator environment with cleanup."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_coord_{suffix}"
    queue_name = f"test_coord_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    publisher = TaskPublisher(queue_name=queue_name)
    consumer = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    coord = Coordinator(task_store=store, publisher=publisher)

    yield {
        "coordinator": coord,
        "store": store,
        "consumer": consumer,
        "queue_name": queue_name,
    }

    # Teardown
    coord.close()
    consumer.close()

    # Clean up Redis keys
    for key in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(key)

    # Clean up RabbitMQ queue
    try:
        conn = create_connection()
        ch = conn.channel()
        ch.queue_delete(queue=queue_name)
        conn.close()
    except Exception:
        pass


def test_coordinator_generates_task_id_and_stores_in_redis(coordinator_env):
    """Verify coordinator generates unique task_id, initial status is PENDING, and task is stored in Redis."""
    coord: Coordinator = coordinator_env["coordinator"]
    store: TaskStore = coordinator_env["store"]

    payload = {"a": 10, "b": 20}
    task = coord.create_task(task_type="calculate", payload=payload)

    # 1. Verify task_id was generated
    assert task.task_id is not None
    assert isinstance(task.task_id, str)
    assert len(task.task_id) > 0

    # 2. Verify initial status is PENDING
    assert task.status == TaskStatus.PENDING

    # 3. Verify task is stored in Redis
    stored_task = store.get_task(task.task_id)
    assert stored_task is not None
    assert stored_task.task_id == task.task_id
    assert stored_task.task_type == "calculate"
    assert stored_task.payload == payload
    assert stored_task.status == TaskStatus.PENDING


def test_coordinator_publishes_to_rabbitmq_with_correct_task_id(coordinator_env):
    """Verify coordinator publishes task message to RabbitMQ containing the correct task_id."""
    coord: Coordinator = coordinator_env["coordinator"]
    consumer: TaskConsumer = coordinator_env["consumer"]

    task = coord.create_task(task_type="calculate", payload={"a": 10, "b": 20})

    # Consume from RabbitMQ
    msg = consumer.consume_one(timeout=5.0)
    assert msg is not None
    assert msg.task_id == task.task_id
    assert msg.body.get("task_id") == task.task_id

    # Acknowledge to leave queue clean
    msg.ack()


def test_coordinator_does_not_execute_task(coordinator_env):
    """Verify the coordinator strictly creates/dispatches the task without executing it."""
    coord: Coordinator = coordinator_env["coordinator"]

    task = coord.create_task(task_type="calculate", payload={"a": 10, "b": 20})

    # Ensure task has not been executed by the coordinator
    assert task.status == TaskStatus.PENDING
    assert task.result is None
    assert task.error is None
    assert task.agent_id is None


def test_coordinator_multiple_tasks_unique_ids(coordinator_env):
    """Verify coordinator assigns distinct unique IDs across multiple submissions."""
    coord: Coordinator = coordinator_env["coordinator"]
    consumer: TaskConsumer = coordinator_env["consumer"]

    task1 = coord.create_task(task_type="calculate", payload={"a": 1, "b": 2})
    task2 = coord.create_task(task_type="calculate", payload={"a": 3, "b": 4})

    assert task1.task_id != task2.task_id

    # Verify both messages are in RabbitMQ in order
    msg1 = consumer.consume_one(timeout=5.0)
    assert msg1 is not None
    assert msg1.task_id == task1.task_id
    msg1.ack()

    msg2 = consumer.consume_one(timeout=5.0)
    assert msg2 is not None
    assert msg2.task_id == task2.task_id
    msg2.ack()
