"""Unit and integration tests for Redis TaskStore."""

import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.task import Task, TaskStatus
from state.task_store import TaskStore


@pytest.fixture
def task_store():
    """Provide a TaskStore using an isolated test prefix and clean up afterward."""
    store = TaskStore(key_prefix="test_task")
    created_keys: list[str] = []

    # Wrap save_task to track keys for guaranteed teardown
    orig_save = store.save_task

    def tracked_save(task: Task) -> None:
        created_keys.append(store._key(task.task_id))
        orig_save(task)

    store.save_task = tracked_save  # type: ignore[method-assign]

    yield store

    # Teardown: delete all test keys created during the test
    for key in created_keys:
        store.redis.delete(key)


def test_save_and_retrieve_task(task_store: TaskStore):
    """Verify storing a task in Redis and retrieving it accurately."""
    task = Task(task_type="image_resize", payload={"width": 800, "height": 600})
    task_store.save_task(task)

    retrieved = task_store.get_task(task.task_id)
    assert retrieved is not None
    assert retrieved.task_id == task.task_id
    assert retrieved.task_type == "image_resize"
    assert retrieved.payload == {"width": 800, "height": 600}
    assert retrieved.status == TaskStatus.PENDING
    assert retrieved.agent_id is None
    assert retrieved.result is None
    assert retrieved.error is None
    assert retrieved.created_at == task.created_at


def test_task_exists(task_store: TaskStore):
    """Verify task_exists returns True for existing tasks and False otherwise."""
    task = task_store.create_task(task_type="report_generation")

    assert task_store.task_exists(task.task_id) is True
    assert task_store.task_exists("non-existent-uuid-12345") is False


def test_updating_status(task_store: TaskStore):
    """Verify status progression from PENDING to PROCESSING to COMPLETED."""
    task = task_store.create_task(task_type="batch_job")
    initial_updated_at = task.updated_at

    # Transition to PROCESSING
    updated_processing = task_store.update_status(task.task_id, TaskStatus.PROCESSING)
    assert updated_processing is not None
    assert updated_processing.status == TaskStatus.PROCESSING
    assert updated_processing.updated_at >= initial_updated_at

    # Transition to COMPLETED
    updated_completed = task_store.update_status(task.task_id, TaskStatus.COMPLETED)
    assert updated_completed is not None
    assert updated_completed.status == TaskStatus.COMPLETED


def test_updating_agent_id(task_store: TaskStore):
    """Verify agent_id can be assigned and persists in Redis."""
    task = task_store.create_task(task_type="summarize")
    assert task.agent_id is None

    updated = task_store.update_agent_id(task.task_id, "worker-node-1")
    assert updated is not None
    assert updated.agent_id == "worker-node-1"

    # Verify reload from Redis
    reloaded = task_store.get_task(task.task_id)
    assert reloaded is not None
    assert reloaded.agent_id == "worker-node-1"


def test_updating_result(task_store: TaskStore):
    """Verify storing execution result and status transition to COMPLETED."""
    task = task_store.create_task(task_type="calculate_sum", payload={"a": 5, "b": 10})
    task_store.update_status(task.task_id, TaskStatus.PROCESSING)

    result_payload = {"sum": 15}
    completed_task = task_store.store_result(task.task_id, result=result_payload)

    assert completed_task is not None
    assert completed_task.result == result_payload
    assert completed_task.status == TaskStatus.COMPLETED

    # Verify persisted state
    persisted = task_store.get_task(task.task_id)
    assert persisted is not None
    assert persisted.result == result_payload
    assert persisted.status == TaskStatus.COMPLETED


def test_storing_error(task_store: TaskStore):
    """Verify storing error message and status transition to FAILED."""
    task = task_store.create_task(task_type="risky_op")
    task_store.update_status(task.task_id, TaskStatus.PROCESSING)

    failed_task = task_store.store_error(task.task_id, error="Divide by zero error")

    assert failed_task is not None
    assert failed_task.error == "Divide by zero error"
    assert failed_task.status == TaskStatus.FAILED

    # Verify persisted state
    persisted = task_store.get_task(task.task_id)
    assert persisted is not None
    assert persisted.error == "Divide by zero error"
    assert persisted.status == TaskStatus.FAILED


def test_missing_task_handling(task_store: TaskStore):
    """Verify operations on non-existent task IDs return None cleanly."""
    dummy_id = "missing-task-id-9999"

    assert task_store.get_task(dummy_id) is None
    assert task_store.update_status(dummy_id, TaskStatus.PROCESSING) is None
    assert task_store.update_agent_id(dummy_id, "agent-x") is None
    assert task_store.store_result(dummy_id, result={"done": True}) is None
    assert task_store.store_error(dummy_id, error="some failure") is None
    assert task_store.delete_task(dummy_id) is False
