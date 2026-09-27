"""Integration tests verifying distributed task lease integration into the Agent processing pipeline (Week 2, Batch 5).

Verifies:
1. Agent A can acquire a lease and process a task end-to-end.
2. Agent B cannot process a task while Agent A owns its lease.
3. Lease is released immediately after successful task completion.
4. Completed tasks do not retain an active lease in Redis.
5. Failed tasks release their lease cleanly upon error.
6. Multi-task distribution across Agent A and Agent B with mutual lease safety.
"""

from pathlib import Path
import sys
import threading
import time
from unittest.mock import MagicMock
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from coordinator.coordinator import Coordinator
from models.task import Task, TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.message import QueueMessage
from queue.publisher import TaskPublisher
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def lease_integration_env():
    """Create isolated test environment for task lease processing flow."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_lease_int_{suffix}"
    queue_name = f"test_lease_int_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease")
    publisher = TaskPublisher(queue_name=queue_name)
    coordinator = Coordinator(task_store=store, publisher=publisher)

    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)

    agent_a = AgentA(task_store=store, consumer=consumer_a, task_lease=lease)
    agent_b = AgentB(task_store=store, consumer=consumer_b, task_lease=lease)

    yield {
        "store": store,
        "lease": lease,
        "coordinator": coordinator,
        "publisher": publisher,
        "agent_a": agent_a,
        "agent_b": agent_b,
        "consumer_a": consumer_a,
        "consumer_b": consumer_b,
        "queue_name": queue_name,
        "key_prefix": key_prefix,
    }

    # Teardown
    coordinator.close()
    agent_a.close()
    agent_b.close()

    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)

    try:
        conn = create_connection()
        ch = conn.channel()
        ch.queue_delete(queue=queue_name)
        conn.close()
    except Exception:
        pass


def test_agent_a_can_acquire_and_process_task(lease_integration_env):
    """Prove Agent A can acquire lease, transition state, execute, and complete task."""
    coord: Coordinator = lease_integration_env["coordinator"]
    agent_a: AgentA = lease_integration_env["agent_a"]
    store: TaskStore = lease_integration_env["store"]
    lease: TaskLease = lease_integration_env["lease"]

    # 1. Coordinator creates and publishes task
    task = coord.create_task("calculate", {"a": 25, "b": 35})
    task_id = task.task_id

    # 2. Before processing, task is PENDING and unleased
    assert task.status == TaskStatus.PENDING
    assert lease.exists(task_id) is False

    # 3. Agent A consumes and processes the task
    completed = agent_a.process_one(timeout=5.0)

    # 4. Verify completion
    assert completed is not None
    assert completed.task_id == task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 60
    assert completed.agent_id == "agent_a"

    # 5. Verify persisted Redis state
    persisted = store.get_task(task_id)
    assert persisted is not None
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.result == 60
    assert persisted.agent_id == "agent_a"


def test_agent_b_cannot_process_task_while_agent_a_owns_lease(lease_integration_env):
    """Prove Agent B cannot process a task while Agent A actively owns its lease."""
    coord: Coordinator = lease_integration_env["coordinator"]
    agent_a: AgentA = lease_integration_env["agent_a"]
    agent_b: AgentB = lease_integration_env["agent_b"]
    store: TaskStore = lease_integration_env["store"]
    lease: TaskLease = lease_integration_env["lease"]

    task = coord.create_task("calculate", {"a": 100, "b": 200})
    task_id = task.task_id

    agent_a_entered = threading.Event()
    agent_a_can_finish = threading.Event()

    def blocking_executor_a(t: Task) -> int:
        agent_a_entered.set()
        agent_a_can_finish.wait(timeout=5.0)
        return 300

    agent_a.executor = blocking_executor_a

    # Run Agent A in background thread to hold the lease during execution
    def run_agent_a():
        agent_a.process_one(timeout=5.0)

    thread_a = threading.Thread(target=run_agent_a)
    thread_a.start()

    # Wait until Agent A has acquired lease and is executing
    assert agent_a_entered.wait(timeout=5.0) is True
    assert lease.exists(task_id) is True
    assert lease.get_owner(task_id) == "agent_a"

    # Status must be PROCESSING and owned by agent_a
    mid_task = store.get_task(task_id)
    assert mid_task.status == TaskStatus.PROCESSING
    assert mid_task.agent_id == "agent_a"

    # Agent B attempts to process a message for the exact same task
    mock_channel = MagicMock()
    conflicting_msg = QueueMessage(
        task_id=task_id,
        delivery_tag=99,
        channel=mock_channel,
        body={"task_id": task_id},
    )

    result_b = agent_b.process_message(conflicting_msg)

    # Agent B must NOT execute or modify task
    assert result_b is None
    # Message must be safely nacked without requeue
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=99, requeue=False)
    # Task ownership must remain Agent A
    assert lease.get_owner(task_id) == "agent_a"
    mid_task_after = store.get_task(task_id)
    assert mid_task_after.agent_id == "agent_a"
    assert mid_task_after.status == TaskStatus.PROCESSING

    # Release Agent A to complete
    agent_a_can_finish.set()
    thread_a.join(timeout=5.0)

    # Final task must be completed by Agent A
    final_task = store.get_task(task_id)
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.agent_id == "agent_a"
    assert final_task.result == 300


def test_lease_is_released_after_successful_completion(lease_integration_env):
    """Prove the lease exists during execution and is deleted immediately upon completion."""
    coord: Coordinator = lease_integration_env["coordinator"]
    agent_a: AgentA = lease_integration_env["agent_a"]
    lease: TaskLease = lease_integration_env["lease"]

    task = coord.create_task("calculate", {"a": 7, "b": 8})
    task_id = task.task_id

    lease_during_exec = {}

    def inspecting_executor(t: Task) -> int:
        lease_during_exec["exists"] = lease.exists(t.task_id)
        lease_during_exec["owner"] = lease.get_owner(t.task_id)
        return 15

    agent_a.executor = inspecting_executor

    completed = agent_a.process_one(timeout=5.0)

    assert completed is not None
    # Proves lease was held by agent_a during execution
    assert lease_during_exec["exists"] is True
    assert lease_during_exec["owner"] == "agent_a"

    # Proves lease is released after completion
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None


def test_completed_tasks_do_not_retain_active_lease(lease_integration_env):
    """Prove completed tasks across multiple agents do not retain active leases in Redis."""
    coord: Coordinator = lease_integration_env["coordinator"]
    agent_a: AgentA = lease_integration_env["agent_a"]
    agent_b: AgentB = lease_integration_env["agent_b"]
    store: TaskStore = lease_integration_env["store"]
    lease: TaskLease = lease_integration_env["lease"]

    # Dispatch 4 tasks
    tasks = [
        coord.create_task("calculate", {"a": i, "b": i * 10})
        for i in range(1, 5)
    ]
    task_ids = [t.task_id for t in tasks]

    # Process with Agent A and Agent B
    agent_a.process_one(timeout=5.0)
    agent_b.process_one(timeout=5.0)
    agent_a.process_one(timeout=5.0)
    agent_b.process_one(timeout=5.0)

    # Verify all 4 tasks are COMPLETED and none retain an active lease
    for tid in task_ids:
        t = store.get_task(tid)
        assert t is not None
        assert t.status == TaskStatus.COMPLETED
        assert t.agent_id in ("agent_a", "agent_b")
        # Lease must be completely absent from Redis
        assert lease.exists(tid) is False
        assert lease.is_leased(tid) is False
        assert lease.get_owner(tid) is None


def test_failed_task_releases_lease(lease_integration_env):
    """Prove a task that fails during execution still releases its lease cleanly."""
    coord: Coordinator = lease_integration_env["coordinator"]
    agent_a: AgentA = lease_integration_env["agent_a"]
    store: TaskStore = lease_integration_env["store"]
    lease: TaskLease = lease_integration_env["lease"]

    task = coord.create_task("calculate", {"bad": "payload"})
    task_id = task.task_id

    # Agent A processes invalid task (raises ValueError)
    with pytest.raises(ValueError):
        agent_a.process_one(timeout=5.0)

    # Task must be FAILED in Redis
    failed_task = store.get_task(task_id)
    assert failed_task is not None
    assert failed_task.status == TaskStatus.FAILED
    assert failed_task.error is not None

    # Lease must NOT remain active
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None
