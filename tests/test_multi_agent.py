"""Tests for multi-agent task consumption, distribution, and identification."""

import concurrent.futures
import sys
from pathlib import Path
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from coordinator.coordinator import Coordinator
from models.task import TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from state.task_store import TaskStore


@pytest.fixture
def multi_agent_env():
    """Set up an isolated environment with a shared queue and store for Agent A and Agent B."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_multi_{suffix}"
    queue_name = f"test_multi_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    publisher = TaskPublisher(queue_name=queue_name)
    coordinator = Coordinator(task_store=store, publisher=publisher)

    # Both agents consume from the SAME queue with prefetch=1
    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)

    agent_a = AgentA(task_store=store, consumer=consumer_a)
    agent_b = AgentB(task_store=store, consumer=consumer_b)

    yield {
        "store": store,
        "coordinator": coordinator,
        "agent_a": agent_a,
        "agent_b": agent_b,
        "queue_name": queue_name,
    }

    # Teardown
    coordinator.close()
    agent_a.close()
    agent_b.close()

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


def test_agent_a_can_process_task(multi_agent_env):
    """Verify Agent A receives and processes a task with correct agent_id."""
    coord: Coordinator = multi_agent_env["coordinator"]
    agent_a: AgentA = multi_agent_env["agent_a"]
    store: TaskStore = multi_agent_env["store"]

    task = coord.create_task(task_type="calculate", payload={"a": 5, "b": 15})

    completed = agent_a.process_one(timeout=5.0)
    assert completed is not None
    assert completed.task_id == task.task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.agent_id == "agent_a"
    assert completed.result == 20

    persisted = store.get_task(task.task_id)
    assert persisted is not None
    assert persisted.agent_id == "agent_a"
    assert persisted.result == 20
    assert persisted.status == TaskStatus.COMPLETED


def test_agent_b_can_process_task(multi_agent_env):
    """Verify Agent B receives and processes a task with correct agent_id."""
    coord: Coordinator = multi_agent_env["coordinator"]
    agent_b: AgentB = multi_agent_env["agent_b"]
    store: TaskStore = multi_agent_env["store"]

    task = coord.create_task(task_type="calculate", payload={"a": 50, "b": 25})

    completed = agent_b.process_one(timeout=5.0)
    assert completed is not None
    assert completed.task_id == task.task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.agent_id == "agent_b"
    assert completed.result == 75

    persisted = store.get_task(task.task_id)
    assert persisted is not None
    assert persisted.agent_id == "agent_b"
    assert persisted.result == 75
    assert persisted.status == TaskStatus.COMPLETED


def test_multiple_tasks_distributed_across_workers(multi_agent_env):
    """Verify multiple tasks are distributed between Agent A and Agent B."""
    coord: Coordinator = multi_agent_env["coordinator"]
    agent_a: AgentA = multi_agent_env["agent_a"]
    agent_b: AgentB = multi_agent_env["agent_b"]
    store: TaskStore = multi_agent_env["store"]

    # Submit 4 tasks to the shared queue
    submitted_tasks = [
        coord.create_task(task_type="calculate", payload={"a": i, "b": i * 10})
        for i in range(1, 5)
    ]

    processed_by: dict[str, list[str]] = {"agent_a": [], "agent_b": []}

    # Alternate consumption between the two agents
    for _ in range(2):
        task_a = agent_a.process_one(timeout=5.0)
        assert task_a is not None
        processed_by["agent_a"].append(task_a.task_id)

        task_b = agent_b.process_one(timeout=5.0)
        assert task_b is not None
        processed_by["agent_b"].append(task_b.task_id)

    # 1. Verify both agents processed tasks
    assert len(processed_by["agent_a"]) == 2
    assert len(processed_by["agent_b"]) == 2

    # 2. Verify all 4 submitted tasks were processed
    all_processed_ids = set(processed_by["agent_a"] + processed_by["agent_b"])
    expected_ids = {t.task_id for t in submitted_tasks}
    assert all_processed_ids == expected_ids

    # 3. Verify Redis task states and proper agent_id recording
    for task_id in processed_by["agent_a"]:
        record = store.get_task(task_id)
        assert record is not None
        assert record.status == TaskStatus.COMPLETED
        assert record.agent_id == "agent_a"

    for task_id in processed_by["agent_b"]:
        record = store.get_task(task_id)
        assert record is not None
        assert record.status == TaskStatus.COMPLETED
        assert record.agent_id == "agent_b"


def test_concurrent_worker_distribution(multi_agent_env):
    """Verify concurrent worker threads process tasks without race conditions or collision."""
    coord: Coordinator = multi_agent_env["coordinator"]
    agent_a: AgentA = multi_agent_env["agent_a"]
    agent_b: AgentB = multi_agent_env["agent_b"]
    store: TaskStore = multi_agent_env["store"]

    tasks = [
        coord.create_task(task_type="calculate", payload={"a": i, "b": 100})
        for i in range(10)
    ]

    def run_worker(worker):
        results = []
        for _ in range(5):
            t = worker.process_one(timeout=5.0)
            if t:
                results.append(t)
        return results

    # Run Agent A and Agent B concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(run_worker, agent_a)
        future_b = executor.submit(run_worker, agent_b)
        results_a = future_a.result()
        results_b = future_b.result()

    total_completed = len(results_a) + len(results_b)
    assert total_completed == 10

    # Ensure both workers participated
    assert len(results_a) > 0
    assert len(results_b) > 0

    # Check agent_id assignment
    for t in results_a:
        assert t.agent_id == "agent_a"
        persisted = store.get_task(t.task_id)
        assert persisted.agent_id == "agent_a"
        assert persisted.status == TaskStatus.COMPLETED

    for t in results_b:
        assert t.agent_id == "agent_b"
        persisted = store.get_task(t.task_id)
        assert persisted.agent_id == "agent_b"
        assert persisted.status == TaskStatus.COMPLETED
