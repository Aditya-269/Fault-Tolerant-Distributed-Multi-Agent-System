"""Tests for RecoveryManager and automatic task requeueing (Week 3, Batch 2).

Verifies:
1. Recoverable task is requeued to RabbitMQ.
2. Task ID is preserved in the requeued message and payload is intact.
3. Healthy tasks are not requeued.
4. Completed tasks are not requeued.
5. Same task is not continuously requeued in a tight loop.
6. Recovery does not execute the task itself.
7. End-to-end integration: Agent A crashes -> Recovery requeues -> Agent B completes.
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
from agents.worker import Worker
from models.task import Task, TaskStatus
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from recovery.recovery_manager import RecoveryManager
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def recovery_manager_env():
    """Create isolated test environment with dedicated Redis namespace and RabbitMQ queue."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_rm_{suffix}"
    queue_name = f"test_rm_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease")
    registry = HealthRegistry(redis_client=store.redis, key_prefix=f"{key_prefix}:health")
    detector = FailureDetector(
        redis_client=store.redis,
        health_registry=registry,
        heartbeat_prefix=f"{key_prefix}:hb",
    )
    publisher = TaskPublisher(queue_name=queue_name)
    consumer = TaskConsumer(queue_name=queue_name, prefetch_count=1)

    rm = RecoveryManager(
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
        "consumer": consumer,
        "recovery_manager": rm,
        "key_prefix": key_prefix,
        "queue_name": queue_name,
    }

    # Teardown
    rm.close()
    try:
        consumer.channel.queue_delete(queue=queue_name)
    except Exception:
        pass
    consumer.close()

    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)


def test_1_recoverable_task_is_requeued(recovery_manager_env):
    """1. Recoverable task transitions to RECOVERABLE and is republished to RabbitMQ."""
    store: TaskStore = recovery_manager_env["store"]
    consumer: TaskConsumer = recovery_manager_env["consumer"]
    rm: RecoveryManager = recovery_manager_env["recovery_manager"]

    # Create task, simulate dead worker processing
    task = store.create_task("calculate", {"a": 10, "b": 20})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_crashed")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # Ensure agent is dead (no heartbeat) and lease is expired (no lease)
    assert rm.is_task_recoverable(task_id) is True

    # Recover task
    recovered = rm.recover_task(task_id)
    assert recovered is not None
    assert recovered.task_id == task_id
    assert recovered.status == TaskStatus.RECOVERABLE

    # Verify task state in Redis
    persisted = store.get_task(task_id)
    assert persisted is not None
    assert persisted.status == TaskStatus.RECOVERABLE

    # Verify message appeared in RabbitMQ
    msg = consumer.consume_one(timeout=2.0)
    assert msg is not None
    assert msg.task_id == task_id
    msg.ack()


def test_2_task_id_is_preserved(recovery_manager_env):
    """2. Original task ID and payload are perfectly preserved through recovery."""
    store: TaskStore = recovery_manager_env["store"]
    consumer: TaskConsumer = recovery_manager_env["consumer"]
    rm: RecoveryManager = recovery_manager_env["recovery_manager"]

    original_payload = {"a": 42, "b": 58, "operation": "add"}
    task = store.create_task("calculate", original_payload)
    original_id = task.task_id
    store.update_agent_id(original_id, "agent_dead")
    store.update_status(original_id, TaskStatus.PROCESSING)

    # Recover via scan_and_recover
    recovered_list = rm.scan_and_recover()
    assert len(recovered_list) == 1
    assert recovered_list[0].task_id == original_id

    # Consume from RabbitMQ
    msg = consumer.consume_one(timeout=2.0)
    assert msg is not None
    assert msg.task_id == original_id
    msg.ack()

    # Verify task retrieved using the preserved task_id maintains payload
    recovered_task = store.get_task(msg.task_id)
    assert recovered_task.task_id == original_id
    assert recovered_task.payload == original_payload
    assert recovered_task.task_type == "calculate"


def test_3_healthy_tasks_are_not_requeued(recovery_manager_env):
    """3. Tasks owned by a healthy agent are NOT recovered or requeued."""
    store: TaskStore = recovery_manager_env["store"]
    lease: TaskLease = recovery_manager_env["lease"]
    consumer: TaskConsumer = recovery_manager_env["consumer"]
    rm: RecoveryManager = recovery_manager_env["recovery_manager"]
    prefix = recovery_manager_env["key_prefix"]

    task = store.create_task("calculate", {"a": 10, "b": 20})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_alive")
    store.update_status(task_id, TaskStatus.PROCESSING)
    lease.acquire(task_id, "agent_alive", ttl=30)

    # Emit active heartbeat for agent_alive
    hb = HeartbeatSender(
        agent_id="agent_alive",
        redis_client=store.redis,
        interval=1.0,
        ttl=15,
        key_prefix=f"{prefix}:hb",
    )
    hb.send_heartbeat()

    # Attempt recovery
    assert rm.is_task_recoverable(task_id) is False
    assert rm.recover_task(task_id) is None
    assert rm.scan_and_recover() == []

    # Verify Redis status untouched
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.PROCESSING

    # Verify RabbitMQ queue is empty
    msg = consumer.consume_one(timeout=1.0)
    assert msg is None


def test_4_completed_tasks_are_not_requeued(recovery_manager_env):
    """4. Completed tasks are never recovered or requeued."""
    store: TaskStore = recovery_manager_env["store"]
    consumer: TaskConsumer = recovery_manager_env["consumer"]
    rm: RecoveryManager = recovery_manager_env["recovery_manager"]

    task = store.create_task("calculate", {"a": 5, "b": 5})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_dead")
    store.store_result(task_id, result=10, status=TaskStatus.COMPLETED)

    # Agent is dead, lease is expired, but status is COMPLETED
    assert rm.is_task_recoverable(task_id) is False
    assert rm.recover_task(task_id) is None
    assert rm.scan_and_recover() == []

    # Verify Redis state
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.result == 10

    # Queue remains empty
    msg = consumer.consume_one(timeout=1.0)
    assert msg is None


def test_5_same_task_is_not_continuously_requeued_in_a_tight_loop(recovery_manager_env):
    """5. Same task is not continuously requeued in repeated scan loops."""
    store: TaskStore = recovery_manager_env["store"]
    consumer: TaskConsumer = recovery_manager_env["consumer"]
    rm: RecoveryManager = recovery_manager_env["recovery_manager"]

    task = store.create_task("calculate", {"a": 7, "b": 3})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_crashed")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # First recovery run should succeed and requeue once
    first_run = rm.scan_and_recover()
    assert len(first_run) == 1
    assert first_run[0].task_id == task_id
    assert first_run[0].status == TaskStatus.RECOVERABLE

    # Simulate 5 subsequent iterations in a tight loop
    for _ in range(5):
        subsequent_run = rm.scan_and_recover()
        assert subsequent_run == [], "Task in RECOVERABLE must not be repeatedly requeued"

        direct_recover = rm.recover_task(task_id)
        assert direct_recover is None, "Direct recover on RECOVERABLE task must return None"

    # Verify RabbitMQ received EXACTLY ONE message
    first_msg = consumer.consume_one(timeout=2.0)
    assert first_msg is not None
    assert first_msg.task_id == task_id
    first_msg.ack()

    # Queue must now be empty (no extra duplicate copies)
    second_msg = consumer.consume_one(timeout=1.0)
    assert second_msg is None


def test_6_recovery_does_not_execute_the_task_itself(recovery_manager_env):
    """6. RecoveryManager transitions state and requeues without executing the task."""
    store: TaskStore = recovery_manager_env["store"]
    rm: RecoveryManager = recovery_manager_env["recovery_manager"]

    task = store.create_task("calculate", {"a": 50, "b": 50})
    task_id = task.task_id
    store.update_agent_id(task_id, "agent_dead")
    store.update_status(task_id, TaskStatus.PROCESSING)

    # Perform recovery
    recovered = rm.recover_task(task_id)
    assert recovered is not None

    # Verify task in Redis has NO result and NO completed status
    persisted = store.get_task(task_id)
    assert persisted.status == TaskStatus.RECOVERABLE
    assert persisted.result is None
    assert persisted.error is None
    # Agent ID remains the dead agent until a new worker claims it
    assert persisted.agent_id == "agent_dead"


def test_7_end_to_end_agent_a_crash_requeue_and_agent_b_completion(recovery_manager_env):
    """7. Full flow: Agent A crashes, RecoveryManager requeues, Agent B consumes and completes."""
    store: TaskStore = recovery_manager_env["store"]
    lease: TaskLease = recovery_manager_env["lease"]
    registry: HealthRegistry = recovery_manager_env["registry"]
    publisher: TaskPublisher = recovery_manager_env["publisher"]
    rm: RecoveryManager = recovery_manager_env["recovery_manager"]
    queue_name = recovery_manager_env["queue_name"]
    prefix = recovery_manager_env["key_prefix"]

    # Step 1: Coordinator creates task
    task = store.create_task("calculate", {"a": 100, "b": 250})
    task_id = task.task_id
    publisher.publish(task_id)

    # Step 2: Agent A consumes and starts processing
    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_a = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=consumer_a,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
    )
    # Agent A acquires lease and starts processing
    msg_a = consumer_a.consume_one(timeout=2.0)
    assert msg_a is not None
    assert msg_a.task_id == task_id
    # Agent A simulates lease acquisition & PROCESSING update
    lease.acquire(task_id, "agent_a", ttl=1)
    store.update_agent_id(task_id, "agent_a")
    store.update_status(task_id, TaskStatus.PROCESSING)
    consumer_a.close()

    # Step 3: Agent A crashes (heartbeat never sent, lease expires)
    import time
    time.sleep(1.1)  # wait for lease ttl=1 to expire
    assert lease.exists(task_id) is False
    assert rm.failure_detector.is_failed("agent_a") is True

    # Step 4: RecoveryManager detects failure, marks RECOVERABLE, and requeues to RabbitMQ
    recovered = rm.recover_task(task_id)
    assert recovered is not None
    assert recovered.status == TaskStatus.RECOVERABLE
    assert store.get_task(task_id).status == TaskStatus.RECOVERABLE

    # Step 5: Agent B consumes the requeued task and completes it
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
    assert processed_task is not None
    assert processed_task.task_id == task_id
    assert processed_task.status == TaskStatus.COMPLETED
    assert processed_task.result == 350
    assert processed_task.agent_id == "agent_b"

    # Step 6: Verify final Redis state
    final_task = store.get_task(task_id)
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.result == 350
    assert final_task.agent_id == "agent_b"
    assert lease.exists(task_id) is False  # lease released on completion

    consumer_b.close()
