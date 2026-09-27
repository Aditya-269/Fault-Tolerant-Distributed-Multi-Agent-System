"""Unit tests for the Task model and TaskStatus enum."""

import sys
from pathlib import Path
import time
import uuid

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.task import Task, TaskStatus


def test_task_creation_defaults():
    """Verify task creation with default and required fields."""
    task = Task(task_type="data_processing", payload={"input": "test.csv"})

    assert task.task_type == "data_processing"
    assert task.payload == {"input": "test.csv"}
    assert isinstance(task.task_id, str)
    assert len(task.task_id) > 0
    assert task.status == TaskStatus.PENDING
    assert task.agent_id is None
    assert task.result is None
    assert task.error is None
    assert task.created_at is not None
    assert task.updated_at is not None


def test_unique_task_ids():
    """Verify that each task instance receives a unique UUID by default."""
    ids = {Task(task_type="test").task_id for _ in range(100)}
    assert len(ids) == 100

    # Ensure generated task_id is a valid UUID4
    sample_task = Task(task_type="test")
    parsed_uuid = uuid.UUID(sample_task.task_id, version=4)
    assert str(parsed_uuid) == sample_task.task_id


def test_task_serialization_roundtrip():
    """Verify to_dict/from_dict and to_json/from_json fidelity."""
    original = Task(
        task_type="compute",
        payload={"x": 10, "y": 20},
        status=TaskStatus.PROCESSING,
        agent_id="agent-007",
    )

    # Dictionary roundtrip
    as_dict = original.to_dict()
    from_dict_task = Task.from_dict(as_dict)
    assert from_dict_task.task_id == original.task_id
    assert from_dict_task.task_type == original.task_type
    assert from_dict_task.payload == original.payload
    assert from_dict_task.status == TaskStatus.PROCESSING
    assert from_dict_task.agent_id == "agent-007"

    # JSON roundtrip
    json_str = original.to_json()
    from_json_task = Task.from_json(json_str)
    assert from_json_task.task_id == original.task_id
    assert from_json_task.status == TaskStatus.PROCESSING
    assert from_json_task.agent_id == "agent-007"


def test_task_touch_updates_timestamp():
    """Verify touch() updates updated_at without modifying created_at."""
    task = Task(task_type="test")
    initial_created_at = task.created_at
    initial_updated_at = task.updated_at

    # Wait briefly to ensure a timestamp delta
    time.sleep(0.01)
    task.touch()

    assert task.created_at == initial_created_at
    assert task.updated_at != initial_updated_at
