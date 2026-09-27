"""Tests for TaskRecovery component and RECOVERABLE state (Week 3, Batch 1).

Verifies:
1. PROCESSING task with healthy agent is NOT recoverable.
2. PROCESSING task with failed agent but active lease is NOT yet recoverable.
3. PROCESSING task with failed agent and expired lease becomes RECOVERABLE.
4. Completed tasks are never marked recoverable.
5. Multiple tasks can be evaluated independently.
6. PENDING and FAILED tasks are not marked recoverable.
"""

from pathlib import Path
import sys
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.failure_detector import FailureDetector
from agents.heartbeat import HeartbeatSender
from models.agent import AgentStatus
from models.task import Task, TaskStatus
from recovery.task_recovery import TaskRecovery
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def recovery_env():
    """Create isolated test harness for TaskRecovery evaluation."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_rec_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease")
    registry = HealthRegistry(redis_client=store.redis, key_prefix=f"{key_prefix}:health")
    detector = FailureDetector(
        redis_client=store.redis,
        health_registry=registry,
        heartbeat_prefix=f"{key_prefix}:hb",
    )
    recovery = TaskRecovery(
        task_store=store,
        task_lease=lease,
        failure_detector=detector,
        health_registry=registry,
    )

    yield {
        "store": store,
        "lease": lease,
        "registry": registry,
        "detector": detector,
        "recovery": recovery,
        "key_prefix": key_prefix,
    }

    # Teardown
    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)


def test_1_processing_task_with_healthy_agent_is_not_recoverable(recovery_env):
    """1. PROCESSING task with healthy agent is NOT recoverable."""
    store: TaskStore = recovery_env["store"]
    lease: TaskLease = recovery_env["lease"]
    detector: FailureDetector = recovery_env["detector"]
    recovery: TaskRecovery = recovery_env["recovery"]
    prefix = recovery_env["key_prefix"]

    task = store.create_task("calculate", {"a": 10, "b": 20})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # Establish active heartbeat for agent_a
    hb = HeartbeatSender(
        agent_id="agent_a",
        redis_client=store.redis,
        interval=1.0,
        ttl=15,
        key_prefix=f"{prefix}:hb",
    )
    hb.send_heartbeat()
    assert detector.is_healthy("agent_a") is True

    # Case A: Lease active & agent healthy -> NOT recoverable
    lease.acquire(task_id, "agent_a", ttl=30)
    assert recovery.is_task_recoverable(task_id) is False
    assert recovery.mark_recoverable(task_id) is None

    # Case B: Lease expired & agent healthy -> NOT recoverable (agent is alive)
    lease.release(task_id, "agent_a")
    assert lease.exists(task_id) is False
    assert recovery.is_task_recoverable(task_id) is False
    assert recovery.mark_recoverable(task_id) is None

    # Status must remain PROCESSING in Redis
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.PROCESSING


def test_2_processing_task_with_failed_agent_but_active_lease_is_not_yet_recoverable(recovery_env):
    """2. PROCESSING task with failed agent but active lease is NOT yet recoverable."""
    store: TaskStore = recovery_env["store"]
    lease: TaskLease = recovery_env["lease"]
    detector: FailureDetector = recovery_env["detector"]
    recovery: TaskRecovery = recovery_env["recovery"]

    task = store.create_task("calculate", {"a": 50, "b": 50})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_crashed")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # Agent is confirmed FAILED (no heartbeat exists)
    assert detector.is_healthy("agent_crashed") is False
    assert detector.is_failed("agent_crashed") is True

    # But lease is still active in Redis (TTL remaining)
    assert lease.acquire(task_id, "agent_crashed", ttl=30) is True
    assert lease.exists(task_id) is True

    # Must NOT be recoverable while lease is active
    assert recovery.is_task_recoverable(task_id) is False
    assert recovery.mark_recoverable(task_id) is None

    # Status must remain PROCESSING
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.PROCESSING


def test_3_processing_task_with_failed_agent_and_expired_lease_becomes_recoverable(recovery_env):
    """3. PROCESSING task with failed agent and expired lease becomes RECOVERABLE."""
    store: TaskStore = recovery_env["store"]
    lease: TaskLease = recovery_env["lease"]
    detector: FailureDetector = recovery_env["detector"]
    recovery: TaskRecovery = recovery_env["recovery"]

    task = store.create_task("calculate", {"a": 100, "b": 200})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_dead")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # 1. Agent is FAILED
    assert detector.is_failed("agent_dead") is True

    # 2. Lease has EXPIRED (no key in Redis)
    assert lease.exists(task_id) is False

    # 3. Eligible for recovery
    assert recovery.is_task_recoverable(task_id) is True

    # 4. Mark recoverable
    recovered = recovery.mark_recoverable(task_id)
    assert recovered is not None
    assert recovered.status == TaskStatus.RECOVERABLE
    assert recovered.agent_id == "agent_dead"

    # 5. Verify persisted status in Redis
    persisted = store.get_task(task_id)
    assert persisted is not None
    assert persisted.status == TaskStatus.RECOVERABLE


def test_4_completed_tasks_are_never_marked_recoverable(recovery_env):
    """4. Completed tasks are never marked recoverable."""
    store: TaskStore = recovery_env["store"]
    lease: TaskLease = recovery_env["lease"]
    detector: FailureDetector = recovery_env["detector"]
    recovery: TaskRecovery = recovery_env["recovery"]

    task = store.create_task("calculate", {"a": 2, "b": 2})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_dead")
    store.store_result(task_id, result=4, status=TaskStatus.COMPLETED)

    # Even though agent is dead and lease is gone
    assert detector.is_failed("agent_dead") is True
    assert lease.exists(task_id) is False

    # Completed tasks must never be recoverable
    assert recovery.is_task_recoverable(task_id) is False
    assert recovery.mark_recoverable(task_id) is None

    # Status remains COMPLETED
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.result == 4


def test_5_multiple_tasks_evaluated_independently(recovery_env):
    """5. Multiple tasks can be evaluated independently without cross-task interference."""
    store: TaskStore = recovery_env["store"]
    lease: TaskLease = recovery_env["lease"]
    recovery: TaskRecovery = recovery_env["recovery"]
    prefix = recovery_env["key_prefix"]

    # Agent A is HEALTHY
    hb_a = HeartbeatSender(
        agent_id="agent_healthy",
        redis_client=store.redis,
        interval=1.0,
        ttl=15,
        key_prefix=f"{prefix}:hb",
    )
    hb_a.send_heartbeat()

    # Agent B is DEAD (no heartbeat)

    # Task 1: PROCESSING, agent_healthy, lease active -> NOT recoverable
    t1 = store.create_task("calculate", {"a": 1, "b": 1})
    store.update_agent_id(t1.task_id, "agent_healthy")
    store.update_status(t1.task_id, TaskStatus.PROCESSING)
    lease.acquire(t1.task_id, "agent_healthy", ttl=30)

    # Task 2: PROCESSING, agent_dead, lease active -> NOT recoverable
    t2 = store.create_task("calculate", {"a": 2, "b": 2})
    store.update_agent_id(t2.task_id, "agent_dead")
    store.update_status(t2.task_id, TaskStatus.PROCESSING)
    lease.acquire(t2.task_id, "agent_dead", ttl=30)

    # Task 3: PROCESSING, agent_dead, lease expired -> RECOVERABLE!
    t3 = store.create_task("calculate", {"a": 3, "b": 3})
    store.update_agent_id(t3.task_id, "agent_dead")
    store.update_status(t3.task_id, TaskStatus.PROCESSING)
    # No lease active for t3

    # Task 4: COMPLETED, agent_dead, lease expired -> NOT recoverable
    t4 = store.create_task("calculate", {"a": 4, "b": 4})
    store.update_agent_id(t4.task_id, "agent_dead")
    store.store_result(t4.task_id, result=8, status=TaskStatus.COMPLETED)

    # Scan and mark
    marked_tasks = recovery.scan_and_mark_recoverable()

    # Only Task 3 should be marked RECOVERABLE
    assert len(marked_tasks) == 1
    assert marked_tasks[0].task_id == t3.task_id
    assert marked_tasks[0].status == TaskStatus.RECOVERABLE

    # Verify statuses of all 4 tasks
    assert store.get_task(t1.task_id).status == TaskStatus.PROCESSING
    assert store.get_task(t2.task_id).status == TaskStatus.PROCESSING
    assert store.get_task(t3.task_id).status == TaskStatus.RECOVERABLE
    assert store.get_task(t4.task_id).status == TaskStatus.COMPLETED


def test_6_pending_and_failed_tasks_are_not_recoverable(recovery_env):
    """6. PENDING and already FAILED tasks are not marked recoverable."""
    store: TaskStore = recovery_env["store"]
    recovery: TaskRecovery = recovery_env["recovery"]

    # PENDING task (never assigned to worker)
    pending_task = store.create_task("calculate", {"a": 5, "b": 5})
    assert pending_task.status == TaskStatus.PENDING
    assert recovery.is_task_recoverable(pending_task.task_id) is False

    # FAILED task (explicit business logic error)
    failed_task = store.create_task("calculate", {"bad": "data"})
    store.update_agent_id(failed_task.task_id, "agent_dead")
    store.store_error(failed_task.task_id, "Invalid input", status=TaskStatus.FAILED)
    assert recovery.is_task_recoverable(failed_task.task_id) is False
