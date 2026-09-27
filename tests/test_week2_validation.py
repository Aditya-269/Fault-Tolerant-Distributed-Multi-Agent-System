"""Final Week 2 Validation Test Suite.

Verifies the complete Week 2 feature matrix:
1. HEARTBEAT: creation, refresh, TTL, expiration.
2. HEALTH REGISTRY: registration, healthy status, failure transition after heartbeat expiration.
3. TASK LEASES: acquisition, mutual exclusion, periodic renewal, release, expiration, reacquisition.
4. INTEGRATION & PIPELINE:
   - Agent A processes task with lease.
   - Agent B cannot steal active lease.
   - Lease released after completion.
   - Completed tasks do not retain active lease.
   - Existing Week 1 pipeline remains fully functional.
"""

from pathlib import Path
import sys
import threading
import time
from unittest.mock import MagicMock
import uuid
import pytest
import redis

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from agents.failure_detector import FailureDetector
from agents.heartbeat import HeartbeatSender
from config import settings
from coordinator.coordinator import Coordinator
from models.agent import AgentStatus
from models.task import Task, TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.message import QueueMessage
from queue.publisher import TaskPublisher
from state.health_registry import HealthRegistry
from state.lease_renewer import LeaseRenewer
from state.task_lease import TaskLease
from state.task_store import TaskStore


@pytest.fixture
def week2_env():
    """Create isolated test environment for Week 2 final validation."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_w2val_{suffix}"
    queue_name = f"test_w2val_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    lease = TaskLease(redis_client=store.redis, prefix=f"{key_prefix}:lease", lease_ttl=30)
    registry = HealthRegistry(redis_client=store.redis, key_prefix=f"{key_prefix}:health")
    detector = FailureDetector(
        redis_client=store.redis,
        health_registry=registry,
        heartbeat_prefix=f"{key_prefix}:hb",
    )
    publisher = TaskPublisher(queue_name=queue_name)
    coordinator = Coordinator(task_store=store, publisher=publisher)

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
        "lease": lease,
        "registry": registry,
        "detector": detector,
        "coordinator": coordinator,
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


# ============================================================================
# 1. HEARTBEAT TESTS
# ============================================================================


def test_heartbeat_creation_refresh_ttl_and_expiration(week2_env):
    """Verify heartbeat creation, TTL, refresh, and expiration."""
    store = week2_env["store"]
    prefix = week2_env["key_prefix"]

    # 1. Creation & TTL
    hb = HeartbeatSender(
        agent_id="agent_hb_test",
        redis_client=store.redis,
        interval=0.3,
        ttl=1,
        key_prefix=f"{prefix}:hb",
    )
    payload = hb.send_heartbeat()
    assert payload["agent_id"] == "agent_hb_test"
    ttl = store.redis.ttl(f"{prefix}:hb:agent_hb_test")
    assert 0 < ttl <= 1

    # 2. Refresh
    hb.start()
    time.sleep(0.5)
    # Still alive due to refresh
    assert store.redis.exists(f"{prefix}:hb:agent_hb_test") == 1

    # 3. Expiration after stop
    hb.stop()
    time.sleep(1.2)
    assert store.redis.exists(f"{prefix}:hb:agent_hb_test") == 0


# ============================================================================
# 2. HEALTH REGISTRY TESTS
# ============================================================================


def test_health_registry_registration_healthy_and_failure_transition(week2_env):
    """Verify agent registration, healthy state, and failure detection transition."""
    registry: HealthRegistry = week2_env["registry"]
    detector: FailureDetector = week2_env["detector"]
    store = week2_env["store"]
    prefix = week2_env["key_prefix"]

    # 1. Registration (STARTING)
    rec = registry.register_agent("agent_test_reg")
    assert rec.status == AgentStatus.STARTING

    # 2. Healthy
    hb = HeartbeatSender(
        agent_id="agent_test_reg",
        redis_client=store.redis,
        interval=0.3,
        ttl=1,
        key_prefix=f"{prefix}:hb",
        health_registry=registry,
    )
    hb.start()
    time.sleep(0.4)

    # Health status is HEALTHY
    assert detector.is_healthy("agent_test_reg") is True
    assert detector.check_agent("agent_test_reg", update_registry=True) == AgentStatus.HEALTHY
    current_rec = registry.get_agent_health("agent_test_reg")
    assert current_rec.status == AgentStatus.HEALTHY

    # 3. Failed after expiration
    hb.stop()
    time.sleep(1.2)
    assert detector.is_healthy("agent_test_reg") is False
    assert detector.is_failed("agent_test_reg") is True
    new_status = detector.check_agent("agent_test_reg", update_registry=True)
    assert new_status == AgentStatus.FAILED
    failed_rec = registry.get_agent_health("agent_test_reg")
    assert failed_rec.status == AgentStatus.FAILED


# ============================================================================
# 3. TASK LEASE TESTS
# ============================================================================


def test_task_lease_lifecycle_and_mutual_exclusion(week2_env):
    """Verify lease acquisition, mutual exclusion, renewal, release, expiry, and reacquisition."""
    lease: TaskLease = week2_env["lease"]
    task_id = f"task_{uuid.uuid4().hex[:6]}"

    # 1. Acquisition
    assert lease.acquire(task_id, "agent_a", ttl=2) is True
    assert lease.get_owner(task_id) == "agent_a"
    assert lease.exists(task_id) is True

    # 2. Mutual Exclusion (Agent B cannot acquire)
    assert lease.acquire(task_id, "agent_b", ttl=2) is False
    assert lease.get_owner(task_id) == "agent_a"

    # 3. Renewal (Owner can renew, wrong agent cannot)
    assert lease.renew(task_id, "agent_b", ttl=5) is False
    assert lease.renew(task_id, "agent_a", ttl=5) is True
    assert lease.get_ttl(task_id) > 2

    # 4. Release (Wrong agent cannot release, owner can)
    assert lease.release(task_id, "agent_b") is False
    assert lease.exists(task_id) is True
    assert lease.release(task_id, "agent_a") is True
    assert lease.exists(task_id) is False

    # 5. Expiration & Reacquisition
    assert lease.acquire(task_id, "agent_a", ttl=1) is True
    time.sleep(1.2)
    assert lease.exists(task_id) is False  # Expired
    # Agent B can reacquire after expiration
    assert lease.acquire(task_id, "agent_b", ttl=10) is True
    assert lease.get_owner(task_id) == "agent_b"
    assert lease.release(task_id, "agent_b") is True


# ============================================================================
# 4. INTEGRATION & PIPELINE TESTS
# ============================================================================


def test_agent_a_processes_task_with_lease_and_agent_b_cannot_steal(week2_env):
    """Verify Agent A executes with lease, Agent B cannot steal it, and lease releases on completion."""
    coord: Coordinator = week2_env["coordinator"]
    agent_a: AgentA = week2_env["agent_a"]
    agent_b: AgentB = week2_env["agent_b"]
    store: TaskStore = week2_env["store"]
    lease: TaskLease = week2_env["lease"]

    task = coord.create_task("calculate", {"a": 40, "b": 60})
    task_id = task.task_id

    exec_entered = threading.Event()
    exec_can_finish = threading.Event()

    def slow_exec(t: Task) -> int:
        exec_entered.set()
        exec_can_finish.wait(timeout=5.0)
        return 100

    agent_a.executor = slow_exec

    t_worker = threading.Thread(target=lambda: agent_a.process_one(timeout=5.0))
    t_worker.start()

    # Wait until Agent A is in execution
    assert exec_entered.wait(timeout=5.0) is True
    assert lease.get_owner(task_id) == "agent_a"
    assert store.get_task(task_id).status == TaskStatus.PROCESSING

    # Agent B tries to process a duplicate message for the same task
    mock_channel = MagicMock()
    duplicate_msg = QueueMessage(
        task_id=task_id,
        delivery_tag=101,
        channel=mock_channel,
        body={"task_id": task_id},
    )
    result_b = agent_b.process_message(duplicate_msg)
    assert result_b is None
    mock_channel.basic_nack.assert_called_once_with(delivery_tag=101, requeue=False)
    assert lease.get_owner(task_id) == "agent_a"  # Unchanged

    # Agent A completes
    exec_can_finish.set()
    t_worker.join(timeout=5.0)

    # Task completed, result stored, lease released
    final_task = store.get_task(task_id)
    assert final_task.status == TaskStatus.COMPLETED
    assert final_task.result == 100
    assert final_task.agent_id == "agent_a"
    assert lease.exists(task_id) is False
    assert lease.get_owner(task_id) is None


def test_existing_week1_pipeline_works_seamlessly(week2_env):
    """Verify that the standard Week 1 end-to-end task pipeline executes seamlessly."""
    coord: Coordinator = week2_env["coordinator"]
    agent_a: AgentA = week2_env["agent_a"]
    agent_b: AgentB = week2_env["agent_b"]
    store: TaskStore = week2_env["store"]

    # Submit 2 tasks
    t1 = coord.create_task("calculate", {"a": 10, "b": 15})
    t2 = coord.create_task("calculate", {"a": 20, "b": 25})

    res1 = agent_a.process_one(timeout=5.0)
    res2 = agent_b.process_one(timeout=5.0)

    assert res1 is not None and res1.task_id == t1.task_id
    assert res1.result == 25 and res1.status == TaskStatus.COMPLETED
    assert res1.agent_id == "agent_a"

    assert res2 is not None and res2.task_id == t2.task_id
    assert res2.result == 45 and res2.status == TaskStatus.COMPLETED
    assert res2.agent_id == "agent_b"
