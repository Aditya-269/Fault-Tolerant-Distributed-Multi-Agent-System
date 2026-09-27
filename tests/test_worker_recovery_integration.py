"""Integration tests connecting recovered tasks to existing workers (Week 3, Batch 3).

Verifies the complete 11-step handoff flow:
1. Agent A owns a task.
2. Agent A fails.
3. Lease expires.
4. Task becomes recoverable.
5. Task is requeued.
6. Agent B receives it.
7. Agent B acquires the lease.
8. Agent B completes it.
9. Final status is COMPLETED.
10. Final agent_id is Agent B.
11. Original task_id is preserved.
"""

from pathlib import Path
import sys
import threading
import time
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from agents.failure_detector import FailureDetector
from agents.worker import Worker
from models.task import Task, TaskStatus
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from recovery.recovery_manager import RecoveryManager
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def recovery_integration_env():
    """Create isolated environment with unique Redis key prefix and RabbitMQ queue."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_rec_int_{suffix}"
    queue_name = f"test_rec_queue_{suffix}"

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


def test_1_agent_a_crash_recovery_handoff_to_agent_b(recovery_integration_env):
    """Proves the full 11-step handoff flow:
    1. Agent A owns a task.
    2. Agent A fails.
    3. Lease expires.
    4. Task becomes recoverable.
    5. Task is requeued.
    6. Agent B receives it.
    7. Agent B acquires the lease.
    8. Agent B completes it.
    9. Final status is COMPLETED.
    10. Final agent_id is Agent B.
    11. Original task_id is preserved.
    """
    store: TaskStore = recovery_integration_env["store"]
    lease: TaskLease = recovery_integration_env["lease"]
    registry: HealthRegistry = recovery_integration_env["registry"]
    publisher: TaskPublisher = recovery_integration_env["publisher"]
    rm: RecoveryManager = recovery_integration_env["recovery_manager"]
    queue_name = recovery_integration_env["queue_name"]

    # --- 1. Coordinator creates task and publishes to RabbitMQ ---
    task = store.create_task("calculate", {"a": 40, "b": 60})
    original_task_id = task.task_id
    publisher.publish(original_task_id)

    # --- Step 1: Agent A owns a task ---
    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    msg_a = consumer_a.consume_one(timeout=2.0)
    assert msg_a is not None
    assert msg_a.task_id == original_task_id

    # Agent A acquires lease with short TTL to simulate failure, sets PROCESSING
    assert lease.acquire(original_task_id, "agent_a", ttl=1) is True
    store.update_agent_id(original_task_id, "agent_a")
    store.update_status(original_task_id, TaskStatus.PROCESSING)

    # Verify Agent A actively owns the task
    owned_task = store.get_task(original_task_id)
    assert owned_task.status == TaskStatus.PROCESSING
    assert owned_task.agent_id == "agent_a"
    assert lease.get_owner(original_task_id) == "agent_a"
    consumer_a.close()

    # --- Step 2 & 3: Agent A fails and its lease expires ---
    # Agent A crashes: no heartbeats emitted, wait for short lease TTL to expire
    time.sleep(1.1)
    assert lease.exists(original_task_id) is False
    assert rm.failure_detector.is_failed("agent_a") is True

    # --- Step 4 & 5: Task becomes recoverable and is requeued ---
    assert rm.is_task_recoverable(original_task_id) is True
    recovered = rm.recover_task(original_task_id)
    assert recovered is not None
    assert recovered.task_id == original_task_id
    assert recovered.status == TaskStatus.RECOVERABLE

    # Verify task state in Redis
    mid_task = store.get_task(original_task_id)
    assert mid_task.status == TaskStatus.RECOVERABLE
    # Original task_id is preserved
    assert mid_task.task_id == original_task_id

    # --- Step 6, 7 & 8: Agent B receives it, acquires lease, and completes it ---
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_b = Worker(
        agent_id="agent_b",
        task_store=store,
        consumer=consumer_b,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
    )

    processed_task = worker_b.process_one(timeout=3.0)
    consumer_b.close()

    # --- Step 9, 10 & 11: Final assertions ---
    assert processed_task is not None
    # 9. Final status is COMPLETED
    assert processed_task.status == TaskStatus.COMPLETED
    assert processed_task.result == 100
    # 10. Final agent_id is Agent B
    assert processed_task.agent_id == "agent_b"
    # 11. Original task_id is preserved
    assert processed_task.task_id == original_task_id

    # Verify state persisted in Redis
    final_task = store.get_task(original_task_id)
    assert final_task is not None
    assert final_task.task_id == original_task_id
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.agent_id == "agent_b"
    assert final_task.result == 100
    # Lease released on completion
    assert lease.exists(original_task_id) is False


def test_2_agent_b_holds_lease_during_recovery_execution(recovery_integration_env):
    """Verify that during execution of recovered task, Agent B holds the active lease."""
    store: TaskStore = recovery_integration_env["store"]
    lease: TaskLease = recovery_integration_env["lease"]
    registry: HealthRegistry = recovery_integration_env["registry"]
    publisher: TaskPublisher = recovery_integration_env["publisher"]
    rm: RecoveryManager = recovery_integration_env["recovery_manager"]
    queue_name = recovery_integration_env["queue_name"]

    task = store.create_task("calculate", {"a": 15, "b": 35})
    task_id = task.task_id

    # Simulate crashed Agent A
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)
    assert rm.failure_detector.is_failed("agent_a") is True
    assert lease.exists(task_id) is False

    # Recover task into RabbitMQ
    assert rm.recover_task(task_id) is not None
    assert store.get_task(task_id).status == TaskStatus.RECOVERABLE

    # Setup Agent B with inspecting executor
    lease_snapshot = {}
    task_snapshot = {}

    def inspecting_executor(t: Task) -> int:
        lease_snapshot["owner"] = lease.get_owner(t.task_id)
        lease_snapshot["exists"] = lease.exists(t.task_id)
        current = store.get_task(t.task_id)
        task_snapshot["agent_id"] = current.agent_id
        task_snapshot["status"] = current.status
        return t.payload["a"] + t.payload["b"]

    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_b = Worker(
        agent_id="agent_b",
        task_store=store,
        consumer=consumer_b,
        executor=inspecting_executor,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
    )

    completed = worker_b.process_one(timeout=3.0)
    consumer_b.close()

    assert completed is not None
    # While executing:
    assert lease_snapshot["exists"] is True
    assert lease_snapshot["owner"] == "agent_b"
    assert task_snapshot["agent_id"] == "agent_b"
    assert task_snapshot["status"] == TaskStatus.PROCESSING

    # After completion:
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 50
    assert completed.agent_id == "agent_b"
    assert lease.exists(task_id) is False


def test_3_worker_skips_and_acks_completed_tasks_without_reprocessing(recovery_integration_env):
    """Verify that if an already-COMPLETED task message arrives, worker acks and does not re-process."""
    store: TaskStore = recovery_integration_env["store"]
    lease: TaskLease = recovery_integration_env["lease"]
    publisher: TaskPublisher = recovery_integration_env["publisher"]
    queue_name = recovery_integration_env["queue_name"]

    task = store.create_task("calculate", {"a": 10, "b": 10})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_a")
    store.store_result(task_id, result=20, status=TaskStatus.COMPLETED)

    # Deliver message to RabbitMQ
    publisher.publish(task_id)

    executor_called = []

    def mock_executor(t: Task) -> int:
        executor_called.append(True)
        return 999

    consumer = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_b = Worker(
        agent_id="agent_b",
        task_store=store,
        consumer=consumer,
        executor=mock_executor,
        task_lease=lease,
        enable_heartbeat=False,
    )

    result = worker_b.process_one(timeout=2.0)
    consumer.close()

    # Executor must not be invoked
    assert len(executor_called) == 0
    # State in Redis remains untouched
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.result == 20
    assert persisted.agent_id == "agent_a"  # NOT overwritten by agent_b


def test_4_concrete_agent_classes_integration(recovery_integration_env):
    """Verify that concrete AgentA and AgentB classes seamlessly handle the handoff flow."""
    store: TaskStore = recovery_integration_env["store"]
    lease: TaskLease = recovery_integration_env["lease"]
    registry: HealthRegistry = recovery_integration_env["registry"]
    publisher: TaskPublisher = recovery_integration_env["publisher"]
    rm: RecoveryManager = recovery_integration_env["recovery_manager"]
    queue_name = recovery_integration_env["queue_name"]

    # 1. Create and dispatch task
    task = store.create_task("calculate", {"a": 75, "b": 25})
    task_id = task.task_id
    publisher.publish(task_id)

    # 2. Agent A starts task but crashes
    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    agent_a = AgentA(
        task_store=store,
        consumer=consumer_a,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
    )
    # Agent A claims task
    msg = consumer_a.consume_one(timeout=2.0)
    assert msg is not None
    lease.acquire(task_id, "agent_a", ttl=1)
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)
    consumer_a.close()

    # 3. Agent A crash simulation
    time.sleep(1.1)
    assert lease.exists(task_id) is False
    assert rm.failure_detector.is_failed("agent_a") is True

    # 4. RecoveryManager recovers task
    recovered = rm.recover_task(task_id)
    assert recovered is not None
    assert recovered.status == TaskStatus.RECOVERABLE

    # 5. Concrete Agent B consumes and finishes task
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    agent_b = AgentB(
        task_store=store,
        consumer=consumer_b,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
    )

    completed = agent_b.process_one(timeout=3.0)
    consumer_b.close()

    assert completed is not None
    assert completed.task_id == task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.agent_id == "agent_b"
    assert completed.result == 100

    # Redis state verification
    final_task = store.get_task(task_id)
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.agent_id == "agent_b"
    assert final_task.result == 100
