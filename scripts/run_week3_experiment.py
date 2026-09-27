"""Week 3 Automatic Failure Recovery Experiment.

Executes a repeatable chaos recovery experiment across 10 consecutive runs:
1. Starts Agent A and Agent B (confirming both are healthy via heartbeats).
2. Submits a task to Coordinator / RabbitMQ.
3. Ensures Agent A begins processing.
4. Injects a controlled Agent A crash (heartbeat stops, lease retained).
5. Confirms Agent A heartbeat expires and status becomes FAILED.
6. Waits for Agent A's lease to expire.
7. Confirms task becomes RECOVERABLE and is requeued to RabbitMQ.
8. Agent B receives the task, acquires a new lease, and executes the task to completion.
9. Verifies the result, original task_id preservation, final agent_id is Agent B, and no active lease remains.
10. Collects and logs metrics for each run and outputs a summary report.

Run directly:
    python scripts/run_week3_experiment.py
"""

from datetime import datetime
import logging
from pathlib import Path
import sys
import time
import uuid

# Ensure project root is in python path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from agents.failure_detector import FailureDetector
from agents.heartbeat import HeartbeatSender
from agents.worker import SimulatedAgentCrash, Worker
from config import settings
from coordinator.coordinator import Coordinator
from models.agent import AgentStatus
from models.task import TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from recovery.recovery_manager import RecoveryManager
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("week3_experiment")


def run_experiment(total_runs: int = 10) -> list[dict]:
    print("=" * 80)
    print(f"WEEK 3 AUTOMATIC FAILURE RECOVERY EXPERIMENT ({total_runs} RUNS)")
    print("=" * 80)

    exp_prefix = f"exp_w3_{uuid.uuid4().hex[:6]}"
    queue_name = f"queue_w3_{uuid.uuid4().hex[:6]}"

    store = TaskStore(key_prefix=exp_prefix)
    redis_client = store.redis
    lease = TaskLease(redis_client=redis_client, prefix=f"{exp_prefix}:lease")
    registry = HealthRegistry(redis_client=redis_client, key_prefix=f"{exp_prefix}:health")
    detector = FailureDetector(
        redis_client=redis_client,
        health_registry=registry,
        heartbeat_prefix=f"{exp_prefix}:hb",
    )
    publisher = TaskPublisher(queue_name=queue_name)
    coordinator = Coordinator(task_store=store, publisher=publisher)
    rm = RecoveryManager(
        task_store=store,
        task_lease=lease,
        failure_detector=detector,
        health_registry=registry,
        publisher=publisher,
        queue_name=queue_name,
    )

    # Use short integer TTLs (1s) for responsive, deterministic runs
    hb_interval = 0.2
    hb_ttl = 1
    lease_ttl = 1
    wait_time = 1.1  # Wait for heartbeat and lease to expire

    original_acquire = lease.acquire
    lease.acquire = lambda tid, aid, ttl=lease_ttl: original_acquire(tid, aid, ttl=lease_ttl)

    # Agent B remains healthy throughout with persistent heartbeat
    hb_b = HeartbeatSender(
        agent_id="agent_b",
        redis_client=redis_client,
        interval=hb_interval,
        ttl=1,
        key_prefix=f"{exp_prefix}:hb",
        health_registry=registry,
    )
    hb_b.start()
    time.sleep(0.3)
    assert detector.is_healthy("agent_b") is True

    experiment_results = []

    try:
        for run_idx in range(1, total_runs + 1):
            run_start = time.time()
            a_val = run_idx * 10
            b_val = run_idx * 20
            expected_result = a_val + b_val

            print(f"\n--- [Run {run_idx}/{total_runs}] ---")
            logger.info(f"[RECOVERY_RUN_STARTED] run={run_idx}/{total_runs}")

            # 1. Start Agent A heartbeat
            hb_a = HeartbeatSender(
                agent_id="agent_a",
                redis_client=redis_client,
                interval=hb_interval,
                ttl=hb_ttl,
                key_prefix=f"{exp_prefix}:hb",
                health_registry=registry,
            )
            hb_a.start()
            time.sleep(0.2)
            assert detector.is_healthy("agent_a") is True
            assert detector.is_healthy("agent_b") is True

            # 2. Submit task
            task = coordinator.create_task(
                "calculate",
                {"a": a_val, "b": b_val, "simulate_failure": True, "failure_agent": "agent_a"},
            )
            task_id = task.task_id
            print(f"  [Task Created] task_id={task_id} payload={{a: {a_val}, b: {b_val}}}")

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

            try:
                worker_a.process_one(timeout=2.0)
            except SimulatedAgentCrash:
                print(f"  [Agent A Crashed] Intentionally halted under lease ownership.")
            finally:
                consumer_a.close()

            # 4. Wait for heartbeat and lease expiration
            print(f"  [Waiting Expiration] Sleeping {wait_time}s for heartbeat & lease TTL...")
            time.sleep(wait_time)

            assert detector.is_failed("agent_a") is True
            assert lease.exists(task_id) is False
            print(f"  [Failure Detected] Agent A FAILED, lease expired.")

            # 5. Recovery Manager detects and requeues
            assert rm.is_task_recoverable(task_id) is True
            recovered = rm.recover_task(task_id)
            assert recovered is not None
            assert recovered.status == TaskStatus.RECOVERABLE
            print(f"  [Task Recovered & Requeued] Status=RECOVERABLE, attempts={recovered.recovery_attempts}")

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

            # 7. Verification
            assert completed is not None
            assert completed.task_id == task_id
            assert completed.status == TaskStatus.COMPLETED
            assert completed.result == expected_result
            assert completed.agent_id == "agent_b"
            assert completed.previous_agent_id == "agent_a"
            assert lease.exists(task_id) is False

            elapsed = round(time.time() - run_start, 4)
            rec_duration = completed.recovery_duration if completed.recovery_duration is not None else 0.0

            run_record = {
                "run": run_idx,
                "task_id": task_id,
                "success": True,
                "original_agent": "agent_a",
                "recovering_agent": "agent_b",
                "recovery_started_at": completed.recovery_started_at,
                "completed_at": completed.completed_at,
                "recovery_duration": rec_duration,
                "total_elapsed": elapsed,
                "result": completed.result,
            }
            experiment_results.append(run_record)

            print(
                f"  [COMPLETED] task_id={task_id} result={completed.result} "
                f"recovering_agent={completed.agent_id} "
                f"recovery_duration={rec_duration:.4f}s total_elapsed={elapsed:.2f}s"
            )
            logger.info(
                f"[RECOVERY_EXPERIMENT_RUN] run={run_idx}/{total_runs} "
                f"task_id={task_id} success=True "
                f"original_agent=agent_a recovering_agent=agent_b "
                f"recovery_duration={rec_duration:.4f}s "
                f"total_elapsed={elapsed:.2f}s"
            )

    finally:
        hb_b.stop()
        lease.acquire = original_acquire
        rm.close()
        coordinator.close()

        # Teardown queue & Redis keys
        try:
            conn = create_connection()
            ch = conn.channel()
            ch.queue_delete(queue=queue_name)
            conn.close()
        except Exception:
            pass

        for k in redis_client.keys(f"{exp_prefix}:*"):
            redis_client.delete(k)

    # Print Formatted Report
    print("\n" + "=" * 80)
    print("WEEK 3 RECOVERY EXPERIMENT REPORT")
    print("=" * 80)

    durations = [r["recovery_duration"] for r in experiment_results]
    elapseds = [r["total_elapsed"] for r in experiment_results]
    avg_recovery_duration = sum(durations) / len(durations)
    avg_elapsed = sum(elapseds) / len(elapseds)
    min_duration = min(durations)
    max_duration = max(durations)

    print("\n### Recovery Experiment Results Table\n")
    print("| Run | Task ID | Original Agent | Recovering Agent | Recovery Started At | Completed At | Recovery Duration | Status |")
    print("| :---: | :--- | :---: | :---: | :--- | :--- | :---: | :---: |")
    for r in experiment_results:
        st = r["recovery_started_at"].split("T")[1][:12] if r["recovery_started_at"] else "N/A"
        ct = r["completed_at"].split("T")[1][:12] if r["completed_at"] else "N/A"
        print(
            f"| {r['run']} | `{r['task_id'][:8]}...` | `{r['original_agent']}` | "
            f"`{r['recovering_agent']}` | `{st}` | `{ct}` | **{r['recovery_duration']:.4f}s** | PASSED |"
        )

    print("\n### Summary Statistics")
    print(f"- Total Recovery Runs: **{len(experiment_results)}**")
    print(f"- Successful Runs: **{len([r for r in experiment_results if r['success']])}**")
    print(f"- Failed Runs: **0**")
    print(f"- Recovery Success Rate: **100.0%**")
    print(f"- Average Recovery Duration: **{avg_recovery_duration:.4f} seconds**")
    print(f"- Min Recovery Duration: **{min_duration:.4f} seconds**")
    print(f"- Max Recovery Duration: **{max_duration:.4f} seconds**")
    print(f"- Average Total Run Time (including crash & TTL): **{avg_elapsed:.2f} seconds**")
    print("=" * 80 + "\n")

    logger.info(
        f"[RECOVERY_EXPERIMENT_SUMMARY] total_runs={total_runs} "
        f"successful={len(experiment_results)} failed=0 "
        f"avg_recovery_duration={avg_recovery_duration:.4f}s "
        f"min={min_duration:.4f}s max={max_duration:.4f}s"
    )

    return experiment_results


if __name__ == "__main__":
    run_experiment(total_runs=10)
