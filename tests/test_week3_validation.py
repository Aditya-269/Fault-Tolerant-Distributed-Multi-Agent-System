"""Final Week 3 Validation Test Suite: End-to-End Automatic Failure Recovery.

Validates the complete Week 3 recovery lifecycle:
1. Scenario (19 Steps):
   - Start Agent A.
   - Start Agent B.
   - Confirm both are healthy.
   - Submit a task.
   - Ensure Agent A begins processing.
   - Inject a controlled Agent A failure.
   - Confirm Agent A heartbeat expires.
   - Confirm Agent A becomes FAILED.
   - Wait for Agent A's task lease to expire.
   - Confirm task becomes RECOVERABLE.
   - Confirm task is requeued.
   - Agent B receives the task.
   - Agent B acquires the lease.
   - Agent B executes the task.
   - Task becomes COMPLETED.
   - Verify the correct result.
   - Verify the original task_id is preserved.
   - Verify the final agent_id is Agent B.
   - Verify no active lease remains.
2. Multi-Run Chaos Recovery Experiment:
   - At least 10 successful recovery runs.
   - Collects recovery duration, original owner, recovering owner.
   - Emits structured logging.
   - Calculates average recovery duration.
3. Symmetric Recovery:
   - Agent B fails -> Agent A recovers and completes.
4. Resource & Lease Cleanliness:
   - No active leases remain after recovery runs.
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

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from agents.failure_detector import FailureDetector
from agents.heartbeat import HeartbeatSender
from agents.worker import SimulatedAgentCrash, Worker
from coordinator.coordinator import Coordinator
from models.agent import AgentStatus
from models.task import Task, TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from recovery.recovery_manager import RecoveryManager
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore

logger = logging.getLogger(__name__)


@pytest.fixture
def week3_val_env():
    """Create isolated test environment for Week 3 final validation."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_w3val_{suffix}"
    queue_name = f"test_w3val_queue_{suffix}"

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

    yield {
        "store": store,
        "lease": lease,
        "registry": registry,
        "detector": detector,
        "publisher": publisher,
        "coordinator": coordinator,
        "recovery_manager": recovery_manager,
        "key_prefix": key_prefix,
        "queue_name": queue_name,
    }

    # Teardown
    recovery_manager.close()
    coordinator.close()

    try:
        cleanup_consumer = TaskConsumer(queue_name=queue_name)
        cleanup_consumer.channel.queue_delete(queue=queue_name)
        cleanup_consumer.close()
    except Exception:
        pass

    for k in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(k)


def test_1_end_to_end_19_step_recovery_flow(week3_val_env):
    """Proves the exact 19-step end-to-end automatic failure recovery scenario:

    1. Start Agent A.
    2. Start Agent B.
    3. Confirm both are healthy.
    4. Submit a task.
    5. Ensure Agent A begins processing.
    6. Inject a controlled Agent A failure.
    7. Confirm Agent A heartbeat expires.
    8. Confirm Agent A becomes FAILED.
    9. Wait for Agent A's task lease to expire.
    10. Confirm task becomes RECOVERABLE.
    11. Confirm task is requeued.
    12. Agent B receives the task.
    13. Agent B acquires the lease.
    14. Agent B executes the task.
    15. Task becomes COMPLETED.
    16. Verify the correct result.
    17. Verify the original task_id is preserved.
    18. Verify the final agent_id is Agent B.
    19. Verify no active lease remains.
    """
    store: TaskStore = week3_val_env["store"]
    lease: TaskLease = week3_val_env["lease"]
    registry: HealthRegistry = week3_val_env["registry"]
    detector: FailureDetector = week3_val_env["detector"]
    coordinator: Coordinator = week3_val_env["coordinator"]
    rm: RecoveryManager = week3_val_env["recovery_manager"]
    prefix = week3_val_env["key_prefix"]
    queue_name = week3_val_env["queue_name"]

    # --- Step 1: Start Agent A ---
    hb_a = HeartbeatSender(
        agent_id="agent_a",
        redis_client=store.redis,
        interval=0.2,
        ttl=1,
        key_prefix=f"{prefix}:hb",
        health_registry=registry,
    )
    hb_a.start()

    # --- Step 2: Start Agent B ---
    hb_b = HeartbeatSender(
        agent_id="agent_b",
        redis_client=store.redis,
        interval=0.2,
        ttl=1,
        key_prefix=f"{prefix}:hb",
        health_registry=registry,
    )
    hb_b.start()

    try:
        # --- Step 3: Confirm both are healthy ---
        time.sleep(0.4)
        assert detector.is_healthy("agent_a") is True
        assert detector.is_healthy("agent_b") is True
        assert detector.check_agent("agent_a", update_registry=True) == AgentStatus.HEALTHY
        assert detector.check_agent("agent_b", update_registry=True) == AgentStatus.HEALTHY

        # --- Step 4: Submit a task ---
        task = coordinator.create_task(
            task_type="calculate",
            payload={"a": 35, "b": 65, "simulate_failure": True, "failure_agent": "agent_a"},
        )
        original_task_id = task.task_id
        assert task.status == TaskStatus.PENDING

        # Configure short 1-second lease TTL for fast, deterministic test execution
        original_acquire = lease.acquire
        lease.acquire = lambda tid, aid, ttl=1: original_acquire(tid, aid, ttl=1)

        # Worker A configured to process first and intentionally simulate crash
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

        # --- Step 5 & 6: Ensure Agent A begins processing & inject controlled failure ---
        with pytest.raises(SimulatedAgentCrash):
            worker_a.process_one(timeout=2.0)

        assert worker_a._crashed is True
        consumer_a.close()

        # Intermediate check: Task was PROCESSING and owned by Agent A when crash occurred
        mid_task = store.get_task(original_task_id)
        assert mid_task.status == TaskStatus.PROCESSING
        assert mid_task.agent_id == "agent_a"
        assert lease.get_owner(original_task_id) == "agent_a"

        # --- Step 7: Confirm Agent A heartbeat expires ---
        # Worker A stopped hb_a on crash; wait 1.1s for 1s TTL to expire in Redis
        time.sleep(1.1)
        assert store.redis.exists(f"{prefix}:hb:agent_a") == 0

        # --- Step 8: Confirm Agent A becomes FAILED ---
        status_a = detector.check_agent("agent_a", update_registry=True)
        assert status_a == AgentStatus.FAILED
        assert detector.is_failed("agent_a") is True
        # Agent B remains healthy throughout
        assert detector.is_healthy("agent_b") is True

        # --- Step 9: Wait for Agent A's task lease to expire ---
        assert lease.exists(original_task_id) is False
        assert lease.get_owner(original_task_id) is None

        # --- Step 10: Confirm task becomes RECOVERABLE ---
        assert rm.is_task_recoverable(original_task_id) is True

        # --- Step 11: Confirm task is requeued ---
        recovered = rm.recover_task(original_task_id)
        assert recovered is not None
        assert recovered.task_id == original_task_id
        assert recovered.status == TaskStatus.RECOVERABLE
        assert recovered.previous_agent_id == "agent_a"
        assert recovered.recovery_attempts == 1
        assert recovered.recovered_at is not None
        assert recovered.failure_detected_at is not None
        assert recovered.recovery_started_at is not None

        # Confirm persisted status in Redis is RECOVERABLE
        redis_rec = store.get_task(original_task_id)
        assert redis_rec.status == TaskStatus.RECOVERABLE

        # --- Step 12, 13, 14 & 15: Agent B receives, acquires lease, executes, completes ---
        # Restore standard lease TTL for Agent B
        lease.acquire = original_acquire

        consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
        worker_b = Worker(
            agent_id="agent_b",
            task_store=store,
            consumer=consumer_b,
            heartbeat_sender=hb_b,
            task_lease=lease,
            health_registry=registry,
            enable_heartbeat=True,
            enable_failure_injection=True,  # will not crash because failure_agent == "agent_a"
        )

        completed = worker_b.process_one(timeout=3.0)
        consumer_b.close()

        assert completed is not None
        assert completed.status == TaskStatus.COMPLETED

        # --- Step 16: Verify the correct result ---
        assert completed.result == 100

        # --- Step 17: Verify the original task_id is preserved ---
        assert completed.task_id == original_task_id

        # --- Step 18: Verify the final agent_id is Agent B ---
        assert completed.agent_id == "agent_b"
        assert completed.previous_agent_id == "agent_a"
        assert completed.recovery_attempts == 1

        # Timing and duration fields verified
        assert completed.completed_at is not None
        assert completed.recovery_duration is not None
        assert completed.recovery_duration > 0.0

        # --- Step 19: Verify no active lease remains ---
        assert lease.exists(original_task_id) is False
        assert lease.get_owner(original_task_id) is None

        # Verify final state in Redis
        final_task = store.get_task(original_task_id)
        assert final_task.status == TaskStatus.COMPLETED
        assert final_task.agent_id == "agent_b"
        assert final_task.result == 100
        assert final_task.task_id == original_task_id

    finally:
        hb_a.stop()
        hb_b.stop()


def test_2_recovery_experiment_ten_runs(week3_val_env):
    """Run the complete recovery scenario at least 10 times.

    Collects:
    - recovery success/failure
    - recovery duration
    - agent that originally owned the task (Agent A)
    - agent that recovered it (Agent B)
    - average recovery time
    - structured logs
    """
    store: TaskStore = week3_val_env["store"]
    lease: TaskLease = week3_val_env["lease"]
    registry: HealthRegistry = week3_val_env["registry"]
    detector: FailureDetector = week3_val_env["detector"]
    coordinator: Coordinator = week3_val_env["coordinator"]
    rm: RecoveryManager = week3_val_env["recovery_manager"]
    prefix = week3_val_env["key_prefix"]
    queue_name = week3_val_env["queue_name"]

    target_runs = 10
    experiment_results: list[dict] = []

    # Configure short lease TTL (1s) for fast, responsive multi-run execution
    original_acquire = lease.acquire
    lease.acquire = lambda tid, aid, ttl=1: original_acquire(tid, aid, ttl=1)

    # Agent B remains continuously healthy with its background heartbeat
    hb_b = HeartbeatSender(
        agent_id="agent_b",
        redis_client=store.redis,
        interval=0.2,
        ttl=1,
        key_prefix=f"{prefix}:hb",
        health_registry=registry,
    )
    hb_b.start()
    time.sleep(0.3)
    assert detector.is_healthy("agent_b") is True

    try:
        for run_idx in range(1, target_runs + 1):
            run_start_time = time.time()
            a_val = run_idx * 10
            b_val = run_idx * 5
            expected_result = a_val + b_val

            logger.info(f"[RECOVERY_RUN_STARTED] run={run_idx}/{target_runs}")

            # 1. Start Agent A heartbeat for this run
            hb_a = HeartbeatSender(
                agent_id="agent_a",
                redis_client=store.redis,
                interval=0.2,
                ttl=1,
                key_prefix=f"{prefix}:hb",
                health_registry=registry,
            )
            hb_a.start()
            time.sleep(0.2)
            assert detector.is_healthy("agent_a") is True

            # 2. Submit task
            task = coordinator.create_task(
                task_type="calculate",
                payload={"a": a_val, "b": b_val, "simulate_failure": True, "failure_agent": "agent_a"},
            )
            task_id = task.task_id

            # 3. Agent A consumes and crashes
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
            with pytest.raises(SimulatedAgentCrash):
                worker_a.process_one(timeout=2.0)
            consumer_a.close()

            # 4. Wait for Agent A heartbeat and lease (1s TTL) to expire
            time.sleep(1.1)
            assert detector.is_failed("agent_a") is True
            assert lease.exists(task_id) is False

            # 5. RecoveryManager recovers task
            assert rm.is_task_recoverable(task_id) is True
            recovered = rm.recover_task(task_id)
            assert recovered is not None
            assert recovered.status == TaskStatus.RECOVERABLE
            assert recovered.previous_agent_id == "agent_a"
            assert recovered.recovery_attempts == 1

            # 6. Agent B processes recovered task
            consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
            worker_b = Worker(
                agent_id="agent_b",
                task_store=store,
                consumer=consumer_b,
                heartbeat_sender=hb_b,
                task_lease=lease,
                health_registry=registry,
                enable_heartbeat=True,
                enable_failure_injection=True,
            )
            completed = worker_b.process_one(timeout=3.0)
            consumer_b.close()

            # 7. Verification of run results
            assert completed is not None
            assert completed.task_id == task_id
            assert completed.status == TaskStatus.COMPLETED
            assert completed.result == expected_result
            assert completed.agent_id == "agent_b"
            assert completed.previous_agent_id == "agent_a"
            assert lease.exists(task_id) is False

            total_run_elapsed = round(time.time() - run_start_time, 4)
            rec_duration = completed.recovery_duration if completed.recovery_duration is not None else 0.0

            run_result = {
                "run": run_idx,
                "task_id": task_id,
                "success": True,
                "original_agent": "agent_a",
                "recovering_agent": "agent_b",
                "recovery_started_at": completed.recovery_started_at,
                "completed_at": completed.completed_at,
                "recovery_duration": rec_duration,
                "total_elapsed": total_run_elapsed,
                "result": completed.result,
            }
            experiment_results.append(run_result)

            logger.info(
                f"[RECOVERY_EXPERIMENT_RUN] run={run_idx}/{target_runs} "
                f"task_id={task_id} success=True "
                f"original_agent={run_result['original_agent']} "
                f"recovering_agent={run_result['recovering_agent']} "
                f"recovery_duration={rec_duration:.4f}s "
                f"total_elapsed={total_run_elapsed:.2f}s"
            )

    finally:
        hb_b.stop()
        lease.acquire = original_acquire

    # Assertions over all 10 runs
    assert len(experiment_results) == target_runs
    successful_runs = [r for r in experiment_results if r["success"]]
    assert len(successful_runs) == target_runs

    durations = [r["recovery_duration"] for r in experiment_results]
    avg_recovery_duration = sum(durations) / len(durations)
    min_duration = min(durations)
    max_duration = max(durations)

    logger.info(
        f"[RECOVERY_EXPERIMENT_SUMMARY] total_runs={target_runs} "
        f"successful={len(successful_runs)} failed=0 "
        f"avg_recovery_duration={avg_recovery_duration:.4f}s "
        f"min={min_duration:.4f}s max={max_duration:.4f}s"
    )

    # Ensure no active leases remain in Redis for all executed tasks
    for r in experiment_results:
        assert lease.exists(r["task_id"]) is False
        persisted = store.get_task(r["task_id"])
        assert persisted.status == TaskStatus.COMPLETED
        assert persisted.agent_id == "agent_b"
        assert persisted.previous_agent_id == "agent_a"


def test_3_symmetric_recovery_agent_b_fails_agent_a_recovers(week3_val_env):
    """Verify symmetric failure recovery: Agent B crashes, Agent A recovers and completes."""
    store: TaskStore = week3_val_env["store"]
    lease: TaskLease = week3_val_env["lease"]
    registry: HealthRegistry = week3_val_env["registry"]
    detector: FailureDetector = week3_val_env["detector"]
    coordinator: Coordinator = week3_val_env["coordinator"]
    rm: RecoveryManager = week3_val_env["recovery_manager"]
    prefix = week3_val_env["key_prefix"]
    queue_name = week3_val_env["queue_name"]

    # Start Agent A & Agent B heartbeats
    hb_a = HeartbeatSender(
        agent_id="agent_a",
        redis_client=store.redis,
        interval=0.2,
        ttl=1,
        key_prefix=f"{prefix}:hb",
        health_registry=registry,
    )
    hb_b = HeartbeatSender(
        agent_id="agent_b",
        redis_client=store.redis,
        interval=0.2,
        ttl=1,
        key_prefix=f"{prefix}:hb",
        health_registry=registry,
    )
    hb_a.start()
    hb_b.start()
    time.sleep(0.3)

    try:
        task = coordinator.create_task(
            "calculate",
            {"a": 200, "b": 300, "simulate_failure": True, "failure_agent": "agent_b"},
        )
        task_id = task.task_id

        # Use short lease TTL for Agent B
        original_acquire = lease.acquire
        lease.acquire = lambda tid, aid, ttl=1: original_acquire(tid, aid, ttl=1)

        # Agent B starts and intentionally crashes
        consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)
        worker_b = Worker(
            agent_id="agent_b",
            task_store=store,
            consumer=consumer_b,
            heartbeat_sender=hb_b,
            task_lease=lease,
            health_registry=registry,
            enable_heartbeat=True,
            enable_failure_injection=True,
            raise_on_crash=True,
        )
        with pytest.raises(SimulatedAgentCrash):
            worker_b.process_one(timeout=2.0)
        consumer_b.close()

        # Wait for Agent B heartbeat and lease to expire
        time.sleep(1.1)
        assert detector.is_failed("agent_b") is True
        assert detector.is_healthy("agent_a") is True
        assert lease.exists(task_id) is False

        # Recover task
        assert rm.is_task_recoverable(task_id) is True
        recovered = rm.recover_task(task_id)
        assert recovered is not None
        assert recovered.previous_agent_id == "agent_b"

        # Agent A consumes and completes
        lease.acquire = original_acquire
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
        )
        completed = worker_a.process_one(timeout=3.0)
        consumer_a.close()

        assert completed is not None
        assert completed.task_id == task_id
        assert completed.status == TaskStatus.COMPLETED
        assert completed.result == 500
        assert completed.agent_id == "agent_a"
        assert completed.previous_agent_id == "agent_b"
        assert lease.exists(task_id) is False

    finally:
        hb_a.stop()
        hb_b.stop()


def test_4_recovery_isolation_and_no_resource_leakage(week3_val_env):
    """Verify that multiple recovered tasks leave zero orphaned Redis leases or dirty state."""
    store: TaskStore = week3_val_env["store"]
    lease: TaskLease = week3_val_env["lease"]
    registry: HealthRegistry = week3_val_env["registry"]
    coordinator: Coordinator = week3_val_env["coordinator"]
    rm: RecoveryManager = week3_val_env["recovery_manager"]
    prefix = week3_val_env["key_prefix"]
    queue_name = week3_val_env["queue_name"]

    # Submit 3 normal tasks
    tasks = [coordinator.create_task("calculate", {"a": i, "b": i * 2}) for i in range(1, 4)]
    task_ids = [t.task_id for t in tasks]

    # Process all 3 tasks with Agent A
    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    worker_a = Worker(
        agent_id="agent_a",
        task_store=store,
        consumer=consumer_a,
        task_lease=lease,
        health_registry=registry,
        enable_heartbeat=False,
    )
    for _ in range(3):
        res = worker_a.process_one(timeout=2.0)
        assert res is not None
        assert res.status == TaskStatus.COMPLETED
    consumer_a.close()

    # RecoveryManager scan should find 0 recoverable tasks
    assert rm.scan_and_recover() == []

    # Verify no leases remain in Redis
    for tid in task_ids:
        assert lease.exists(tid) is False
        assert store.get_task(tid).status == TaskStatus.COMPLETED

    lease_keys = store.redis.keys(f"{prefix}:lease:*")
    assert len(lease_keys) == 0
