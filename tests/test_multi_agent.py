"""Tests for multi-agent task consumption, distribution, and identification."""

import concurrent.futures
from pathlib import Path
import sys
import time
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from agents.failure_detector import FailureDetector
from coordinator.coordinator import Coordinator
from models.task import Task, TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.message import QueueMessage
from queue.publisher import TaskPublisher
from recovery.recovery_manager import RecoveryManager
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def multi_agent_env():
    """Set up an isolated environment with a shared queue and store for Agent A and Agent B."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_multi_{suffix}"
    queue_name = f"test_multi_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease")
    registry = HealthRegistry(redis_client=store.redis, key_prefix=f"{key_prefix}:health")
    detector = FailureDetector(
        redis_client=store.redis,
        health_registry=registry,
        heartbeat_prefix=f"{key_prefix}:hb",
    )
    publisher = TaskPublisher(queue_name=queue_name)
    coordinator = Coordinator(task_store=store, publisher=publisher)
    recovery_manager = RecoveryManager(
        task_store=store,
        task_lease=lease,
        failure_detector=detector,
        health_registry=registry,
        publisher=publisher,
        queue_name=queue_name,
    )

    # Both agents consume from the SAME queue with prefetch=1
    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)

    agent_a = AgentA(
        task_store=store,
        consumer=consumer_a,
        task_lease=lease,
        health_registry=registry,
    )
    agent_b = AgentB(
        task_store=store,
        consumer=consumer_b,
        task_lease=lease,
        health_registry=registry,
    )

    yield {
        "store": store,
        "coordinator": coordinator,
        "agent_a": agent_a,
        "agent_b": agent_b,
        "queue_name": queue_name,
        "lease": lease,
        "registry": registry,
        "detector": detector,
        "recovery_manager": recovery_manager,
        "publisher": publisher,
        "key_prefix": key_prefix,
    }

    # Teardown
    coordinator.close()
    recovery_manager.close()
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


def test_multi_agent_burst_task_distribution(multi_agent_env):
    """Verify that submitting multiple tasks with both agents running results in both agents processing work.

    Does not assert strict round-robin delivery, but verifies both agents participate,
    all tasks reach COMPLETED state, results are correct, and no tasks are lost.
    """
    coord: Coordinator = multi_agent_env["coordinator"]
    agent_a: AgentA = multi_agent_env["agent_a"]
    agent_b: AgentB = multi_agent_env["agent_b"]
    store: TaskStore = multi_agent_env["store"]
    lease: TaskLease = multi_agent_env["lease"]

    task_count = 8
    submitted_tasks = [
        coord.create_task(task_type="calculate", payload={"a": i * 10, "b": 5})
        for i in range(1, task_count + 1)
    ]
    expected_ids = {t.task_id for t in submitted_tasks}

    def run_worker(worker: Worker):
        processed = []
        for _ in range(task_count):
            task = worker.process_one(timeout=2.0)
            if task:
                processed.append(task)
        return processed

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(run_worker, agent_a)
        future_b = executor.submit(run_worker, agent_b)
        results_a = future_a.result()
        results_b = future_b.result()

    total_completed = len(results_a) + len(results_b)
    assert total_completed == task_count

    # Both agents must participate under concurrent test conditions
    assert len(results_a) > 0, "Agent A should process at least one task"
    assert len(results_b) > 0, "Agent B should process at least one task"

    all_processed_ids = {t.task_id for t in results_a} | {t.task_id for t in results_b}
    assert all_processed_ids == expected_ids

    # Verify state, ownership, and lease release in Redis
    for t in results_a:
        assert t.agent_id == "agent_a"
        persisted = store.get_task(t.task_id)
        assert persisted is not None
        assert persisted.status == TaskStatus.COMPLETED
        assert persisted.agent_id == "agent_a"
        assert persisted.result == t.payload["a"] + t.payload["b"]
        assert lease.exists(t.task_id) is False

    for t in results_b:
        assert t.agent_id == "agent_b"
        persisted = store.get_task(t.task_id)
        assert persisted is not None
        assert persisted.status == TaskStatus.COMPLETED
        assert persisted.agent_id == "agent_b"
        assert persisted.result == t.payload["a"] + t.payload["b"]
        assert lease.exists(t.task_id) is False


def test_multi_agent_failure_recovery_agent_a_crash_agent_b_recovers(multi_agent_env):
    """Verify that when Agent A crashes during processing, RecoveryManager requeues and Agent B completes the task."""
    coord: Coordinator = multi_agent_env["coordinator"]
    agent_a: AgentA = multi_agent_env["agent_a"]
    agent_b: AgentB = multi_agent_env["agent_b"]
    store: TaskStore = multi_agent_env["store"]
    lease: TaskLease = multi_agent_env["lease"]
    rm: RecoveryManager = multi_agent_env["recovery_manager"]
    queue_name = multi_agent_env["queue_name"]

    # 1. Create a task via coordinator
    task = coord.create_task(task_type="calculate", payload={"a": 33, "b": 67})
    task_id = task.task_id

    # 2. Agent A claims task with a short lease (1s) to simulate a failure window
    msg_a = agent_a.consumer.consume_one(timeout=3.0)
    assert msg_a is not None
    assert msg_a.task_id == task_id

    assert lease.acquire(task_id, "agent_a", ttl=1) is True
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # Confirm intermediate ownership
    claimed = store.get_task(task_id)
    assert claimed.status == TaskStatus.PROCESSING
    assert claimed.agent_id == "agent_a"
    assert lease.get_owner(task_id) == "agent_a"

    # 3. Agent A simulates crash: stops emitting heartbeats, halts
    time.sleep(1.1)  # Allow 1-second lease TTL to expire
    assert lease.exists(task_id) is False
    assert rm.failure_detector.is_failed("agent_a") is True

    # 4. RecoveryManager detects dead agent + expired lease, marks RECOVERABLE, and requeues
    assert rm.is_task_recoverable(task_id) is True
    recovered = rm.recover_task(task_id)
    assert recovered is not None
    assert recovered.task_id == task_id
    assert recovered.status == TaskStatus.RECOVERABLE
    assert recovered.recovery_attempts == 1

    # 5. Agent B consumes and processes the recovered task
    completed = agent_b.process_one(timeout=5.0)
    assert completed is not None
    assert completed.task_id == task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 100
    assert completed.agent_id == "agent_b"
    assert completed.previous_agent_id == "agent_a"

    # 6. Verify final state in Redis: lease released, no data loss
    persisted = store.get_task(task_id)
    assert persisted is not None
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.agent_id == "agent_b"
    assert persisted.result == 100
    assert persisted.previous_agent_id == "agent_a"
    assert lease.exists(task_id) is False


def test_multi_agent_lease_mutual_exclusion_prevents_concurrent_processing(multi_agent_env):
    """Verify that distributed task leases prevent concurrent processing and race conditions between agents."""
    coord: Coordinator = multi_agent_env["coordinator"]
    agent_a: AgentA = multi_agent_env["agent_a"]
    agent_b: AgentB = multi_agent_env["agent_b"]
    store: TaskStore = multi_agent_env["store"]
    lease: TaskLease = multi_agent_env["lease"]

    task = coord.create_task(task_type="calculate", payload={"a": 12, "b": 18})
    task_id = task.task_id

    # Agent A acquires lease and starts processing
    msg_a = agent_a.consumer.consume_one(timeout=3.0)
    assert msg_a is not None
    assert lease.acquire(task_id, "agent_a", ttl=10) is True
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # Simulate duplicate message arrival at Agent B while Agent A holds active lease
    duplicate_msg = QueueMessage(
        task_id=task_id,
        delivery_tag=999,
        channel=agent_b.consumer.channel,
        body={"task_id": task_id},
    )

    # Agent B attempts to process the message, but detects active lease conflict
    result = agent_b.process_message(duplicate_msg)
    assert result is None, "Agent B must not process task owned by Agent A"

    # Verify task state was not overwritten by Agent B
    current = store.get_task(task_id)
    assert current.status == TaskStatus.PROCESSING
    assert current.agent_id == "agent_a"
    assert lease.get_owner(task_id) == "agent_a"

    # Agent A finishes processing successfully
    result_val = agent_a.executor(task)
    store.store_result(task_id=task_id, result=result_val, status=TaskStatus.COMPLETED)
    lease.release(task_id, "agent_a")
    msg_a.ack()

    # Final verification: task completed cleanly by Agent A, lease released
    final_task = store.get_task(task_id)
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.agent_id == "agent_a"
    assert final_task.result == 30
    assert lease.exists(task_id) is False
