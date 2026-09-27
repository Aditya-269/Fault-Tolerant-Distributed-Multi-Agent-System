"""Tests for distributed task leases (Week 2, Batch 4).

Verifies:
1. Agent A successfully acquires a lease.
2. Agent B cannot acquire the same active lease.
3. Lease owner can renew it.
4. Wrong agent cannot renew another agent's lease.
5. Lease can be released by its owner.
6. Lease expires after TTL.
7. Another agent can acquire an expired lease.
8. Worker holds lease during PROCESSING state and releases on COMPLETED.
9. Worker respects active lease held by another agent without overwriting.
"""

from pathlib import Path
import sys
import time
from unittest.mock import MagicMock
import uuid
import pytest
import redis

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.worker import Worker
from config import settings
from models.task import Task, TaskStatus
from queue.message import QueueMessage
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def lease_env():
    """Create isolated Redis client and TaskLease instance for unit testing."""
    suffix = uuid.uuid4().hex[:8]
    prefix = f"test_lease_{suffix}"

    r = redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
    )
    lease = TaskLease(redis_client=r, prefix=prefix, lease_ttl=30)

    yield {
        "lease": lease,
        "redis": r,
        "prefix": prefix,
    }

    # Teardown
    for key in r.keys(f"{prefix}:*"):
        r.delete(key)
    r.close()


def test_1_agent_a_successfully_acquires_lease(lease_env):
    """1. Agent A successfully acquires a lease."""
    lease: TaskLease = lease_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    assert lease.exists(task_id) is False
    assert lease.is_leased(task_id) is False
    assert lease.get_owner(task_id) is None

    # Acquire lease
    acquired = lease.acquire(task_id, "agent_a")
    assert acquired is True

    # Verify state
    assert lease.exists(task_id) is True
    assert lease.is_leased(task_id) is True
    assert lease.get_owner(task_id) == "agent_a"

    # Verify TTL is set
    ttl = lease.get_ttl(task_id)
    assert 0 < ttl <= 30


def test_2_agent_b_cannot_acquire_same_active_lease(lease_env):
    """2. Agent B cannot acquire the same active lease."""
    lease: TaskLease = lease_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Agent A acquires lease first
    assert lease.acquire(task_id, "agent_a") is True
    assert lease.get_owner(task_id) == "agent_a"

    # Agent B attempts to acquire the same active lease
    acquired_b = lease.acquire(task_id, "agent_b")
    assert acquired_b is False

    # Owner must remain Agent A
    assert lease.get_owner(task_id) == "agent_a"


def test_3_lease_owner_can_renew(lease_env):
    """3. Lease owner can renew it."""
    lease: TaskLease = lease_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Acquire with short TTL of 10s
    assert lease.acquire(task_id, "agent_a", ttl=10) is True
    assert lease.get_ttl(task_id) <= 10

    # Agent A renews with 45s TTL
    renewed = lease.renew(task_id, "agent_a", ttl=45)
    assert renewed is True

    # Verify lease is extended
    assert lease.get_owner(task_id) == "agent_a"
    ttl = lease.get_ttl(task_id)
    assert ttl > 10
    assert ttl <= 45


def test_4_wrong_agent_cannot_renew_another_agents_lease(lease_env):
    """4. Wrong agent cannot renew another agent's lease."""
    lease: TaskLease = lease_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Agent A acquires lease
    assert lease.acquire(task_id, "agent_a", ttl=20) is True

    # Agent B attempts to renew Agent A's lease
    renewed_b = lease.renew(task_id, "agent_b", ttl=60)
    assert renewed_b is False

    # Ownership and TTL must not be compromised
    assert lease.get_owner(task_id) == "agent_a"
    assert lease.get_ttl(task_id) <= 20

    # Non-existent lease cannot be renewed
    assert lease.renew("non_existent_task", "agent_a", ttl=60) is False


def test_5_lease_can_be_released_by_owner(lease_env):
    """5. Lease can be released by its owner."""
    lease: TaskLease = lease_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    assert lease.acquire(task_id, "agent_a") is True

    # Wrong agent cannot release the lease
    released_wrong = lease.release(task_id, "agent_b")
    assert released_wrong is False
    assert lease.exists(task_id) is True
    assert lease.get_owner(task_id) == "agent_a"

    # Owner releases the lease
    released_owner = lease.release(task_id, "agent_a")
    assert released_owner is True
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None

    # Subsequent release of already released lease returns False
    assert lease.release(task_id, "agent_a") is False


def test_6_lease_expires_after_ttl(lease_env):
    """6. Lease expires after TTL."""
    lease: TaskLease = lease_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Acquire lease with 1 second TTL
    assert lease.acquire(task_id, "agent_a", ttl=1) is True
    assert lease.exists(task_id) is True
    assert lease.get_owner(task_id) == "agent_a"

    # Wait for Redis TTL expiration
    time.sleep(1.2)

    # Lease must have automatically expired in Redis
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None


def test_7_another_agent_can_acquire_expired_lease(lease_env):
    """7. Another agent can acquire an expired lease."""
    lease: TaskLease = lease_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Agent A acquires lease with 1 second TTL
    assert lease.acquire(task_id, "agent_a", ttl=1) is True
    assert lease.acquire(task_id, "agent_b") is False

    # Wait for expiration
    time.sleep(1.2)
    assert lease.exists(task_id) is False

    # Now that Agent A's lease expired, Agent B can successfully acquire it
    acquired_b = lease.acquire(task_id, "agent_b", ttl=30)
    assert acquired_b is True
    assert lease.get_owner(task_id) == "agent_b"
    assert lease.exists(task_id) is True


def test_8_worker_holds_lease_during_processing_and_releases_on_completion():
    """Verify worker creates lease during PROCESSING state and cleans it up on completion."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_worker_lease_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease")

    # Create a pending task in Redis
    task = store.create_task("calculate", {"a": 10, "b": 20})
    task_id = task.task_id

    lease_observed = {}

    def inspecting_executor(t: Task) -> int:
        # Check that lease exists and is owned by agent_a during execution
        lease_observed["exists"] = lease.exists(t.task_id)
        lease_observed["owner"] = lease.get_owner(t.task_id)
        return 30

    mock_consumer = MagicMock()
    mock_msg = MagicMock(spec=QueueMessage)
    mock_msg.task_id = task_id
    mock_msg.ack = MagicMock()

    worker = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=mock_consumer,
        executor=inspecting_executor,
        enable_heartbeat=False,
        task_lease=lease,
    )

    completed = worker.process_message(mock_msg)

    # 1. During PROCESSING, lease was active
    assert lease_observed["exists"] is True
    assert lease_observed["owner"] == "agent_a"

    # 2. After COMPLETED, task is finished and lease is released
    assert completed is not None
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 30
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None
    mock_msg.ack.assert_called_once()

    # Teardown
    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)


def test_9_worker_respects_active_lease_held_by_another_agent():
    """Verify worker does not process task or overwrite state if lease is held by another agent."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_worker_conflict_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease")

    task = store.create_task("calculate", {"a": 5, "b": 5})
    task_id = task.task_id

    # Simulate Agent B already holding active lease on this task
    assert lease.acquire(task_id, "agent_b") is True

    executor_called = []

    def mock_executor(t: Task) -> int:
        executor_called.append(True)
        return 10

    mock_consumer = MagicMock()
    mock_msg = MagicMock(spec=QueueMessage)
    mock_msg.task_id = task_id
    mock_msg.ack = MagicMock()

    worker = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=mock_consumer,
        executor=mock_executor,
        enable_heartbeat=False,
        task_lease=lease,
    )

    result = worker.process_message(mock_msg)

    # Worker A must skip execution
    assert result is None
    assert len(executor_called) == 0
    # Task status must not have been modified to PROCESSING by Agent A
    current_task = store.get_task(task_id)
    assert current_task.status == TaskStatus.PENDING
    assert current_task.agent_id is None
    # Lease must still belong to Agent B
    assert lease.get_owner(task_id) == "agent_b"
    # Agent A did not ACK message
    mock_msg.ack.assert_not_called()

    # Teardown
    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)
