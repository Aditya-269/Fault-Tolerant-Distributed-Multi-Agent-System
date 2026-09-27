"""Tests for safe and observable recovery mechanisms (Week 3, Batch 5).

Verifies:
1. Task is recovered only once per failure.
2. Completed task is never recovered.
3. Active task is never requeued.
4. Recovery metadata is recorded (previous_agent_id, recovery_attempts, timestamps).
5. Recovery duration is calculated correctly upon completion.
6. Structured logging markers emitted: [AGENT_FAILED], [TASK_RECOVERABLE],
   [TASK_REQUEUED], [TASK_RECOVERY_STARTED], [TASK_RECOVERED], [TASK_COMPLETED].
"""

from datetime import datetime
import logging
from pathlib import Path
import sys
import time
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.failure_detector import FailureDetector
from agents.heartbeat import HeartbeatSender
from agents.worker import Worker
from models.task import Task, TaskStatus
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from recovery.recovery_manager import RecoveryManager
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def obs_env():
    """Create isolated test environment for recovery observability tests."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_obs_{suffix}"
    queue_name = f"test_obs_queue_{suffix}"

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


def test_1_task_is_recovered_only_once_per_failure(obs_env):
    """1. Task is recovered only once per failure; tight-loop scans do not repeatedly requeue."""
    store: TaskStore = obs_env["store"]
    rm: RecoveryManager = obs_env["recovery_manager"]
    consumer = TaskConsumer(queue_name=obs_env["queue_name"], prefetch_count=1)

    task = store.create_task("calculate", {"a": 10, "b": 20})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # Initial failure: Agent A dead, lease expired
    first_scan = rm.scan_and_recover()
    assert len(first_scan) == 1
    assert first_scan[0].task_id == task_id
    assert first_scan[0].status == TaskStatus.RECOVERABLE
    assert first_scan[0].recovery_attempts == 1
    assert first_scan[0].previous_agent_id == "agent_a"

    # Subsequent scans while task is RECOVERABLE and queued must NOT re-recover
    for _ in range(3):
        repeat_scan = rm.scan_and_recover()
        assert repeat_scan == []
        assert rm.recover_task(task_id) is None

    # Verify task in Redis still has recovery_attempts == 1
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.RECOVERABLE
    assert persisted.recovery_attempts == 1

    # Exactly 1 message in RabbitMQ queue
    msg = consumer.consume_one(timeout=2.0)
    assert msg is not None
    assert msg.task_id == task_id
    msg.ack()
    assert consumer.consume_one(timeout=1.0) is None
    consumer.close()


def test_2_completed_task_is_never_recovered(obs_env):
    """2. A completed task is never recovered or requeued."""
    store: TaskStore = obs_env["store"]
    rm: RecoveryManager = obs_env["recovery_manager"]
    consumer = TaskConsumer(queue_name=obs_env["queue_name"], prefetch_count=1)

    task = store.create_task("calculate", {"a": 30, "b": 40})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_a")
    store.store_result(task_id, result=70, status=TaskStatus.COMPLETED)

    # Agent A is dead, lease is expired, but status is COMPLETED
    assert rm.is_task_recoverable(task_id) is False
    assert rm.recover_task(task_id) is None
    assert rm.scan_and_recover() == []

    # Redis state remains unchanged
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.result == 70

    # No messages published to queue
    assert consumer.consume_one(timeout=1.0) is None
    consumer.close()


def test_3_active_task_is_not_requeued(obs_env):
    """3. An active task owned by a healthy agent is never recovered or requeued."""
    store: TaskStore = obs_env["store"]
    lease: TaskLease = obs_env["lease"]
    rm: RecoveryManager = obs_env["recovery_manager"]
    prefix = obs_env["key_prefix"]
    consumer = TaskConsumer(queue_name=obs_env["queue_name"], prefetch_count=1)

    task = store.create_task("calculate", {"a": 5, "b": 15})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_healthy")
    store.update_status(task_id, TaskStatus.PROCESSING)
    lease.acquire(task_id, "agent_healthy", ttl=30)

    # Healthy heartbeat
    hb = HeartbeatSender(
        agent_id="agent_healthy",
        redis_client=store.redis,
        interval=1.0,
        ttl=15,
        key_prefix=f"{prefix}:hb",
    )
    hb.send_heartbeat()

    assert rm.is_task_recoverable(task_id) is False
    assert rm.recover_task(task_id) is None
    assert rm.scan_and_recover() == []

    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.PROCESSING
    assert persisted.agent_id == "agent_healthy"
    assert consumer.consume_one(timeout=1.0) is None
    consumer.close()


def test_4_recovery_metadata_is_recorded(obs_env):
    """4. Recovery metadata fields are recorded in Redis on recovery."""
    store: TaskStore = obs_env["store"]
    rm: RecoveryManager = obs_env["recovery_manager"]

    task = store.create_task("calculate", {"a": 100, "b": 200})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_crashed")
    store.update_status(task_id, TaskStatus.PROCESSING)

    recovered = rm.recover_task(task_id)
    assert recovered is not None

    # Check in-memory object
    assert recovered.status == TaskStatus.RECOVERABLE
    assert recovered.previous_agent_id == "agent_crashed"
    assert recovered.recovery_attempts == 1
    assert recovered.recovered_at is not None
    assert recovered.failure_detected_at is not None
    assert recovered.recovery_started_at is not None

    # Check persisted object in Redis
    persisted = store.get_task(task_id)
    assert persisted is not None
    assert persisted.status == TaskStatus.RECOVERABLE
    assert persisted.previous_agent_id == "agent_crashed"
    assert persisted.recovery_attempts == 1
    assert persisted.recovered_at == recovered.recovered_at
    assert persisted.failure_detected_at == recovered.failure_detected_at
    assert persisted.recovery_started_at == recovered.recovery_started_at

    # Verify timestamps are valid ISO 8601
    dt_recovered = datetime.fromisoformat(persisted.recovered_at)
    dt_failure = datetime.fromisoformat(persisted.failure_detected_at)
    dt_started = datetime.fromisoformat(persisted.recovery_started_at)
    assert dt_recovered is not None
    assert dt_failure is not None
    assert dt_started is not None


def test_5_recovery_duration_is_calculated_correctly(obs_env):
    """5. Recovery duration is calculated correctly and recorded in Redis upon completion."""
    store: TaskStore = obs_env["store"]
    lease: TaskLease = obs_env["lease"]
    registry: HealthRegistry = obs_env["registry"]
    rm: RecoveryManager = obs_env["recovery_manager"]
    queue_name = obs_env["queue_name"]

    # 1. Create task and simulate crash by agent_a
    task = store.create_task("calculate", {"a": 50, "b": 50})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # 2. Recover task
    recovered = rm.recover_task(task_id)
    assert recovered is not None
    assert recovered.recovery_started_at is not None

    # 3. Small pause to guarantee measurable duration
    time.sleep(0.05)

    # 4. Agent B consumes and completes task
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_b = Worker(
        agent_id="agent_b",
        task_store=store,
        consumer=consumer_b,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
    )

    completed = worker_b.process_one(timeout=3.0)
    consumer_b.close()

    assert completed is not None
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 100
    assert completed.agent_id == "agent_b"
    assert completed.previous_agent_id == "agent_a"
    assert completed.recovery_attempts == 1

    # 5. Check completed_at and recovery_duration
    assert completed.completed_at is not None
    assert completed.recovery_duration is not None
    assert isinstance(completed.recovery_duration, float)
    assert completed.recovery_duration > 0.0

    # Verify calculation: completed_at - recovery_started_at
    start_dt = datetime.fromisoformat(completed.recovery_started_at)
    end_dt = datetime.fromisoformat(completed.completed_at)
    expected_duration = round((end_dt - start_dt).total_seconds(), 4)
    assert abs(completed.recovery_duration - expected_duration) < 0.01

    # Verify in Redis persistence
    final_task = store.get_task(task_id)
    assert final_task.completed_at == completed.completed_at
    assert final_task.recovery_duration == completed.recovery_duration


def test_6_structured_logging_markers_emitted(obs_env, caplog):
    """6. All required structured logging markers are emitted across the recovery flow."""
    store: TaskStore = obs_env["store"]
    lease: TaskLease = obs_env["lease"]
    rm: RecoveryManager = obs_env["recovery_manager"]
    queue_name = obs_env["queue_name"]

    task = store.create_task("calculate", {"a": 1, "b": 2})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)

    with caplog.at_level(logging.INFO):
        # 1. Recover task
        recovered = rm.recover_task(task_id)
        assert recovered is not None

        # 2. Agent B finishes task
        consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
        worker_b = Worker(
            agent_id="agent_b",
            task_store=store,
            consumer=consumer_b,
            task_lease=lease,
            enable_heartbeat=False,
        )
        completed = worker_b.process_one(timeout=3.0)
        consumer_b.close()
        assert completed is not None

    log_text = caplog.text
    assert "[AGENT_FAILED]" in log_text
    assert "[TASK_RECOVERY_STARTED]" in log_text
    assert "[TASK_RECOVERABLE]" in log_text
    assert "[TASK_REQUEUED]" in log_text
    assert "[TASK_RECOVERED]" in log_text
    assert "[TASK_COMPLETED]" in log_text
