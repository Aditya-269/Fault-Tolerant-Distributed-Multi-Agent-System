"""Tests for deterministic failure injection and crash simulation (Week 3, Batch 4).

Verifies:
1. Full 10-step failure injection lifecycle:
   - Agent A starts processing
   - Agent A fails intentionally
   - Heartbeat disappears
   - Agent A becomes FAILED
   - Lease expires
   - Task becomes RECOVERABLE
   - Task is requeued
   - Agent B acquires it
   - Agent B completes it
   - Final Redis state is COMPLETED
2. Failure injection is disabled by default for normal tasks.
3. Crash occurs AFTER status is PROCESSING and lease is owned, but BEFORE COMPLETED.
4. Worker-level enable_failure_injection toggle.
5. raise_on_crash behavior in process_one.
"""

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
from agents.heartbeat import HeartbeatSender
from agents.worker import SimulatedAgentCrash, Worker
from models.task import Task, TaskStatus
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from recovery.recovery_manager import RecoveryManager
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def failure_injection_env():
    """Create isolated test environment for deterministic failure injection tests."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_fi_{suffix}"
    queue_name = f"test_fi_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease")
    registry = HealthRegistry(redis_client=store.redis, key_prefix=f"{key_prefix}:health")
    detector = FailureDetector(
        redis_client=store.redis,
        health_registry=registry,
        heartbeat_prefix=f"{key_prefix}:hb",
    )
    publisher = TaskPublisher(queue_name=queue_name)
    recovery_manager = RecoveryManager(
        task_store=store,
        task_lease=lease,
        failure_detector=detector,
        health_registry=registry,
        publisher=publisher,
        queue_name=queue_name,
    )

    yield {
        "store": store,
        "lease": lease,
        "registry": registry,
        "detector": detector,
        "publisher": publisher,
        "recovery_manager": recovery_manager,
        "key_prefix": key_prefix,
        "queue_name": queue_name,
    }

    # Teardown
    recovery_manager.close()
    try:
        cleanup_consumer = TaskConsumer(queue_name=queue_name)
        cleanup_consumer.channel.queue_delete(queue=queue_name)
        cleanup_consumer.close()
    except Exception:
        pass

    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)


def test_1_deterministic_failure_injection_10_step_flow(failure_injection_env):
    """Verifies all 10 required steps:
    1. Agent A starts processing.
    2. Agent A fails.
    3. Heartbeat disappears.
    4. Agent A becomes FAILED.
    5. Lease expires.
    6. Task becomes RECOVERABLE.
    7. Task is requeued.
    8. Agent B acquires it.
    9. Agent B completes it.
    10. Final Redis state is COMPLETED.
    """
    store: TaskStore = failure_injection_env["store"]
    lease: TaskLease = failure_injection_env["lease"]
    registry: HealthRegistry = failure_injection_env["registry"]
    detector: FailureDetector = failure_injection_env["detector"]
    publisher: TaskPublisher = failure_injection_env["publisher"]
    rm: RecoveryManager = failure_injection_env["recovery_manager"]
    queue_name = failure_injection_env["queue_name"]
    prefix = failure_injection_env["key_prefix"]

    # Step 0: Dispatch test task with simulate_failure targeting agent_a
    task_payload = {
        "a": 10,
        "b": 20,
        "simulate_failure": True,
        "failure_agent": "agent_a",
    }
    task = store.create_task("calculate", task_payload)
    original_task_id = task.task_id
    publisher.publish(original_task_id)

    # Initialize Agent A with a short heartbeat TTL (1 sec) and short lease TTL
    hb_a = HeartbeatSender(
        agent_id="agent_a",
        redis_client=store.redis,
        interval=0.5,
        ttl=1,
        key_prefix=f"{prefix}:hb",
        health_registry=registry,
    )
    hb_a.start()
    assert detector.is_healthy("agent_a") is True

    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_a = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=consumer_a,
        heartbeat_sender=hb_a,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=True,
        enable_failure_injection=True,
        raise_on_crash=True,
    )

    # Override lease acquire to use 1s TTL for fast, deterministic test execution
    original_acquire = lease.acquire

    def short_ttl_acquire(tid: str, aid: str, ttl: int = 1) -> bool:
        return original_acquire(tid, aid, ttl=1)

    lease.acquire = short_ttl_acquire

    # --- Step 1 & 2: Agent A starts processing and intentionally crashes ---
    with pytest.raises(SimulatedAgentCrash):
        worker_a.process_one(timeout=2.0)

    assert worker_a._crashed is True
    # At this point, task was in PROCESSING when crash occurred
    mid_task = store.get_task(original_task_id)
    assert mid_task.status == TaskStatus.PROCESSING
    assert mid_task.agent_id == "agent_a"
    assert mid_task.result is None
    # Lease was active at the moment of crash
    assert lease.get_owner(original_task_id) == "agent_a"

    # --- Step 3: Heartbeat disappears ---
    # Agent A's heartbeat sender thread stopped upon crash; wait 1.1s for TTL to expire
    time.sleep(1.1)
    hb_key = f"{prefix}:hb:agent_a"
    assert store.redis.exists(hb_key) == 0

    # --- Step 4: Agent A becomes FAILED ---
    assert detector.is_failed("agent_a") is True
    assert detector.is_healthy("agent_a") is False

    # --- Step 5: Lease expires ---
    assert lease.exists(original_task_id) is False

    # --- Step 6: Task becomes RECOVERABLE ---
    assert rm.is_task_recoverable(original_task_id) is True
    recovered = rm.recover_task(original_task_id)
    assert recovered is not None
    assert recovered.task_id == original_task_id
    assert recovered.status == TaskStatus.RECOVERABLE
    assert store.get_task(original_task_id).status == TaskStatus.RECOVERABLE

    # --- Step 7: Task is requeued ---
    # RecoveryManager published the task_id back to RabbitMQ
    consumer_a.close()

    # --- Step 8 & 9: Agent B acquires it and completes it ---
    # Restore normal acquire for Agent B
    lease.acquire = original_acquire

    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_b = Worker(
        agent_id="agent_b",
        task_store=store,
        consumer=consumer_b,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
        enable_failure_injection=True,  # Will not crash because failure_agent == "agent_a"
    )

    completed = worker_b.process_one(timeout=3.0)
    consumer_b.close()

    assert completed is not None
    assert completed.task_id == original_task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 30
    assert completed.agent_id == "agent_b"

    # --- Step 10: Final Redis state is COMPLETED ---
    final_task = store.get_task(original_task_id)
    assert final_task is not None
    assert final_task.task_id == original_task_id
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.result == 30
    assert final_task.agent_id == "agent_b"
    assert lease.exists(original_task_id) is False


def test_2_failure_injection_disabled_by_default_for_normal_tasks(failure_injection_env):
    """Normal tasks without simulate_failure execute without any crash."""
    store: TaskStore = failure_injection_env["store"]
    lease: TaskLease = failure_injection_env["lease"]
    publisher: TaskPublisher = failure_injection_env["publisher"]
    queue_name = failure_injection_env["queue_name"]

    normal_task = store.create_task("calculate", {"a": 25, "b": 75})
    publisher.publish(normal_task.task_id)

    consumer = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=consumer,
        task_lease=lease,
        enable_heartbeat=False,
        enable_failure_injection=True,
    )

    completed = worker.process_one(timeout=2.0)
    consumer.close()

    assert completed is not None
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 100
    assert completed.agent_id == "agent_a"
    assert worker._crashed is False


def test_3_crash_occurs_strictly_after_processing_and_lease_owned(failure_injection_env):
    """Crash triggers strictly AFTER task is in PROCESSING and lease owned, but BEFORE COMPLETED."""
    store: TaskStore = failure_injection_env["store"]
    lease: TaskLease = failure_injection_env["lease"]
    publisher: TaskPublisher = failure_injection_env["publisher"]
    queue_name = failure_injection_env["queue_name"]

    task = store.create_task(
        "calculate",
        {"a": 7, "b": 8, "simulate_failure": True, "failure_agent": "agent_a"},
    )
    task_id = task.task_id
    publisher.publish(task_id)

    consumer = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=consumer,
        task_lease=lease,
        enable_heartbeat=False,
        raise_on_crash=False,  # process_one absorbs crash and returns None
    )

    result = worker.process_one(timeout=2.0)
    consumer.close()

    # process_one returns None due to crash
    assert result is None
    assert worker._crashed is True

    # In Redis: Task status is PROCESSING (NOT COMPLETED, NOT FAILED)
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.PROCESSING
    assert persisted.agent_id == "agent_a"
    assert persisted.result is None

    # In Redis: Lease is still owned by agent_a (NOT released)
    assert lease.get_owner(task_id) == "agent_a"
    assert lease.exists(task_id) is True


def test_4_worker_level_failure_injection_toggle(failure_injection_env):
    """Worker with enable_failure_injection=False ignores simulate_failure."""
    store: TaskStore = failure_injection_env["store"]
    lease: TaskLease = failure_injection_env["lease"]
    publisher: TaskPublisher = failure_injection_env["publisher"]
    queue_name = failure_injection_env["queue_name"]

    task = store.create_task("calculate", {"a": 3, "b": 4, "simulate_failure": True})
    publisher.publish(task.task_id)

    consumer = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=consumer,
        task_lease=lease,
        enable_heartbeat=False,
        enable_failure_injection=False,  # Explicitly disabled
    )

    completed = worker.process_one(timeout=2.0)
    consumer.close()

    assert completed is not None
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 7
    assert worker._crashed is False


def test_5_concrete_agents_simulate_failure_handoff(failure_injection_env):
    """Concrete AgentA simulates crash and concrete AgentB successfully recovers and completes."""
    store: TaskStore = failure_injection_env["store"]
    lease: TaskLease = failure_injection_env["lease"]
    detector: FailureDetector = failure_injection_env["detector"]
    publisher: TaskPublisher = failure_injection_env["publisher"]
    rm: RecoveryManager = failure_injection_env["recovery_manager"]
    queue_name = failure_injection_env["queue_name"]

    task = store.create_task(
        "calculate",
        {"a": 50, "b": 50, "simulate_failure": True, "failure_agent": "agent_a"},
    )
    task_id = task.task_id
    publisher.publish(task_id)

    # Concrete Agent A with short lease TTL
    original_acquire = lease.acquire
    lease.acquire = lambda tid, aid, ttl=1: original_acquire(tid, aid, ttl=1)

    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    agent_a = AgentA(
        task_store=store,
        consumer=consumer_a,
        task_lease=lease,
        enable_heartbeat=False,
        enable_failure_injection=True,
    )

    agent_a.process_one(timeout=2.0)
    consumer_a.close()
    assert agent_a._crashed is True

    # Wait for short lease to expire
    time.sleep(1.1)
    assert lease.exists(task_id) is False

    # Recover task
    assert rm.recover_task(task_id) is not None
    assert store.get_task(task_id).status == TaskStatus.RECOVERABLE

    # Concrete Agent B picks up and completes
    lease.acquire = original_acquire
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    agent_b = AgentB(
        task_store=store,
        consumer=consumer_b,
        task_lease=lease,
        enable_heartbeat=False,
        enable_failure_injection=True,
    )

    completed = agent_b.process_one(timeout=3.0)
    consumer_b.close()

    assert completed is not None
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 100
    assert completed.agent_id == "agent_b"
    assert store.get_task(task_id).status == TaskStatus.COMPLETED
