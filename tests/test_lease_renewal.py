"""Tests for periodic lease renewal during long-running tasks (Week 2, Batch 6).

Verifies:
1. Lease TTL gets refreshed.
2. Lease remains owned during long processing.
3. Renewal stops after completion.
4. Wrong agent cannot renew the lease.
5. Lease eventually expires if renewal stops.
6. Worker maintains lease across long execution and cleans up upon completion.
7. Renewal stops if processing fails.
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
from state.lease_renewer import LeaseRenewer
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def lease_renewal_env():
    """Create isolated Redis client and TaskLease instance for renewal tests."""
    suffix = uuid.uuid4().hex[:8]
    prefix = f"test_renewal_{suffix}"

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
    for k in r.keys(f"{prefix}:*"):
        r.delete(k)
    r.close()


def test_1_lease_ttl_gets_refreshed(lease_renewal_env):
    """1. Lease TTL gets refreshed periodically by LeaseRenewer."""
    lease: TaskLease = lease_renewal_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Acquire lease with 3s TTL
    assert lease.acquire(task_id, "agent_a", ttl=3) is True
    initial_ttl = lease.get_ttl(task_id)
    assert 0 < initial_ttl <= 3

    # Start renewer with 0.4s interval, refreshing to 3s
    renewer = LeaseRenewer(
        lease=lease,
        task_id=task_id,
        agent_id="agent_a",
        interval=0.4,
        ttl=3,
    )
    renewer.start()
    assert renewer.is_running is True

    try:
        # Wait 1.0s (two renewals will occur at ~0.4s and ~0.8s)
        time.sleep(1.0)

        # Without renewal, TTL would have fallen to ~2.0s
        # With renewal, TTL was refreshed back to ~3s
        current_ttl = lease.get_ttl(task_id)
        assert current_ttl > 2.0
        assert lease.get_owner(task_id) == "agent_a"
    finally:
        renewer.stop()

    assert renewer.is_running is False


def test_2_lease_remains_owned_during_long_processing(lease_renewal_env):
    """2. Lease remains owned during task execution exceeding initial TTL."""
    lease: TaskLease = lease_renewal_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Initial lease TTL of 2 seconds
    assert lease.acquire(task_id, "agent_a", ttl=2) is True

    # Renew every 0.5s to 2s
    renewer = LeaseRenewer(
        lease=lease,
        task_id=task_id,
        agent_id="agent_a",
        interval=0.5,
        ttl=2,
    )
    renewer.start()

    try:
        # Simulate 3.2s of processing (exceeds initial 2.0s TTL by 1.2s!)
        time.sleep(3.2)

        # Lease must still be active and owned by Agent A
        assert lease.exists(task_id) is True
        assert lease.get_owner(task_id) == "agent_a"
        assert lease.get_ttl(task_id) > 0
    finally:
        renewer.stop()


def test_3_renewal_stops_after_completion(lease_renewal_env):
    """3. Renewal stops after task completion and does not recreate key."""
    lease: TaskLease = lease_renewal_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    assert lease.acquire(task_id, "agent_a", ttl=2) is True

    renewer = LeaseRenewer(
        lease=lease,
        task_id=task_id,
        agent_id="agent_a",
        interval=0.3,
        ttl=2,
    )
    renewer.start()
    assert renewer.is_running is True

    # Simulate task completion: stop renewer and release lease
    renewer.stop()
    assert renewer.is_running is False
    assert lease.release(task_id, "agent_a") is True
    assert lease.exists(task_id) is False

    # Wait for multiple renewal intervals
    time.sleep(0.8)

    # Key must NOT have been recreated by background thread
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None


def test_4_wrong_agent_cannot_renew_the_lease(lease_renewal_env):
    """4. Wrong agent cannot renew another agent's lease."""
    lease: TaskLease = lease_renewal_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Agent A acquires the lease with 3s TTL
    assert lease.acquire(task_id, "agent_a", ttl=3) is True

    # Agent B attempts to renew Agent A's lease
    renewer_b = LeaseRenewer(
        lease=lease,
        task_id=task_id,
        agent_id="agent_b",
        interval=0.3,
        ttl=15,
    )

    renewed = renewer_b.renew_once()
    assert renewed is False

    # Ownership and TTL must not be compromised
    assert lease.get_owner(task_id) == "agent_a"
    assert lease.get_ttl(task_id) <= 3


def test_5_lease_eventually_expires_if_renewal_stops(lease_renewal_env):
    """5. Lease eventually expires in Redis if renewal stops."""
    lease: TaskLease = lease_renewal_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # Acquire lease with 1.0s TTL
    assert lease.acquire(task_id, "agent_a", ttl=1) is True

    # Start renewer with 0.3s interval
    renewer = LeaseRenewer(
        lease=lease,
        task_id=task_id,
        agent_id="agent_a",
        interval=0.3,
        ttl=1,
    )
    renewer.start()

    # Let it renew once
    time.sleep(0.5)
    assert lease.exists(task_id) is True

    # Stop renewer (simulating worker crash or stopped heartbeat/renewer)
    renewer.stop()

    # Wait for TTL to expire
    time.sleep(1.2)

    # Lease must have automatically expired in Redis
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None


def test_6_worker_long_running_task_renewal_and_completion():
    """6. Worker processes long task exceeding lease TTL via automatic background renewal."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_worker_long_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    # Short 2-second lease TTL
    lease = TaskLease(
        redis_client=store.redis,
        prefix=f"{key_prefix}:lease",
        lease_ttl=2,
    )

    task = store.create_task("calculate", {"a": 50, "b": 50})
    task_id = task.task_id

    # Simulated long-running computation of 3.0s (longer than 2s lease TTL)
    def long_running_executor(t: Task) -> int:
        time.sleep(3.0)
        return 100

    mock_consumer = MagicMock()
    mock_msg = MagicMock(spec=QueueMessage)
    mock_msg.task_id = task_id
    mock_msg.ack = MagicMock()

    worker = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=mock_consumer,
        executor=long_running_executor,
        enable_heartbeat=False,
        task_lease=lease,
        lease_renewal_interval=0.5,
    )

    completed = worker.process_message(mock_msg)

    # 1. Task completed successfully despite exceeding initial 2s lease TTL
    assert completed is not None
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 100
    mock_msg.ack.assert_called_once()

    # 2. Lease released cleanly after completion
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None

    # Teardown
    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)


def test_7_renewal_stops_if_processing_fails():
    """7. Renewal stops and lease is released if task execution raises an error."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_worker_fail_renew_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(
        redis_client=store.redis,
        prefix=f"{key_prefix}:lease",
        lease_ttl=5,
    )

    task = store.create_task("calculate", {"bad": "data"})
    task_id = task.task_id

    mock_consumer = MagicMock()
    mock_msg = MagicMock(spec=QueueMessage)
    mock_msg.task_id = task_id
    mock_msg.nack = MagicMock()

    worker = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=mock_consumer,
        enable_heartbeat=False,
        task_lease=lease,
        lease_renewal_interval=0.3,
    )

    with pytest.raises(ValueError):
        worker.process_message(mock_msg)

    # Task is FAILED
    failed = store.get_task(task_id)
    assert failed.status == TaskStatus.FAILED

    # Lease is released
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None

    # Wait for renewal interval to ensure no lingering renewal
    time.sleep(0.5)
    assert lease.exists(task_id) is False

    # Teardown
    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)
