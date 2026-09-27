"""Week 2 Manual Experiment: Agent Failure Detection Validation.

Steps:
1. Start Agent A (with heartbeat).
2. Start Agent B (with heartbeat).
3. Confirm both heartbeats exist.
4. Stop Agent A.
5. Wait for heartbeat TTL to expire.
6. Confirm Agent A is detected as failed.
7. Confirm Agent B remains healthy.
"""

from pathlib import Path
import sys
import time

# Ensure project root is in python path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from agents.failure_detector import FailureDetector
from agents.heartbeat import HeartbeatSender
from config import settings
from models.agent import AgentStatus
from state.health_registry import HealthRegistry
from state.task_store import TaskStore


def run_experiment():
    print("=" * 60)
    print("WEEK 2 MANUAL EXPERIMENT: LIVENESS & FAILURE DETECTION")
    print("=" * 60)

    # Use a clean, isolated prefix for this experiment run
    exp_prefix = "exp_w2"
    ttl = 3  # Fast 3-second TTL for responsive experiment demonstration
    interval = 1.0  # 1-second refresh interval

    store = TaskStore()
    redis_client = store.redis
    registry = HealthRegistry(redis_client=redis_client, key_prefix=f"{exp_prefix}:health")
    detector = FailureDetector(
        redis_client=redis_client,
        health_registry=registry,
        heartbeat_prefix=f"{exp_prefix}:hb",
    )

    # Step 1: Start Agent A
    print("\n[Step 1] Starting Agent A...")
    hb_a = HeartbeatSender(
        agent_id="agent_a",
        redis_client=redis_client,
        interval=interval,
        ttl=ttl,
        key_prefix=f"{exp_prefix}:hb",
        health_registry=registry,
    )
    hb_a.start()
    print("  -> Agent A started (Heartbeat sender active, registered in Health Registry).")

    # Step 2: Start Agent B
    print("\n[Step 2] Starting Agent B...")
    hb_b = HeartbeatSender(
        agent_id="agent_b",
        redis_client=redis_client,
        interval=interval,
        ttl=ttl,
        key_prefix=f"{exp_prefix}:hb",
        health_registry=registry,
    )
    hb_b.start()
    print("  -> Agent B started (Heartbeat sender active, registered in Health Registry).")

    # Step 3: Confirm both heartbeats exist
    print("\n[Step 3] Confirming both heartbeats exist in Redis...")
    time.sleep(0.5)  # Allow initial heartbeats to register
    alive_a = detector.is_healthy("agent_a")
    alive_b = detector.is_healthy("agent_b")
    ttl_a = redis_client.ttl(f"{exp_prefix}:hb:agent_a")
    ttl_b = redis_client.ttl(f"{exp_prefix}:hb:agent_b")
    status_a = detector.check_agent("agent_a", update_registry=True)
    status_b = detector.check_agent("agent_b", update_registry=True)

    print(f"  -> Agent A: healthy={alive_a}, ttl={ttl_a}s, status={status_a.value}")
    print(f"  -> Agent B: healthy={alive_b}, ttl={ttl_b}s, status={status_b.value}")
    assert alive_a is True, "Agent A heartbeat must exist!"
    assert alive_b is True, "Agent B heartbeat must exist!"
    assert status_a == AgentStatus.HEALTHY
    assert status_b == AgentStatus.HEALTHY
    print("  -> CONFIRMED: Both Agent A and Agent B heartbeats exist and are HEALTHY.")

    # Step 4: Stop Agent A
    print("\n[Step 4] Stopping Agent A (simulating worker crash/shutdown)...")
    hb_a.stop()
    print("  -> Agent A heartbeat sender stopped.")

    # Step 5: Wait for heartbeat TTL to expire
    wait_time = ttl + 0.8
    print(f"\n[Step 5] Waiting {wait_time:.1f}s for Agent A's heartbeat key TTL ({ttl}s) to expire in Redis...")
    time.sleep(wait_time)

    # Step 6: Confirm Agent A is detected as failed
    print("\n[Step 6] Evaluating Agent A health status...")
    alive_a_after = detector.is_healthy("agent_a")
    failed_a_after = detector.is_failed("agent_a")
    status_a_after = detector.check_agent("agent_a", update_registry=True)
    print(f"  -> Agent A: alive={alive_a_after}, failed={failed_a_after}, detected_status={status_a_after.value}")
    assert alive_a_after is False, "Agent A heartbeat must have expired!"
    assert failed_a_after is True, "Agent A must be detected as failed!"
    assert status_a_after == AgentStatus.FAILED
    print("  -> CONFIRMED: Agent A heartbeat expired and Agent A is detected as FAILED.")

    # Step 7: Confirm Agent B remains healthy
    print("\n[Step 7] Evaluating Agent B health status...")
    alive_b_after = detector.is_healthy("agent_b")
    ttl_b_after = redis_client.ttl(f"{exp_prefix}:hb:agent_b")
    status_b_after = detector.check_agent("agent_b", update_registry=True)
    print(f"  -> Agent B: alive={alive_b_after}, ttl={ttl_b_after}s, detected_status={status_b_after.value}")
    assert alive_b_after is True, "Agent B must remain alive!"
    assert status_b_after == AgentStatus.HEALTHY, "Agent B must remain HEALTHY!"
    print("  -> CONFIRMED: Agent B heartbeat continues to refresh and remains HEALTHY.")

    # Teardown
    hb_b.stop()
    for key in redis_client.keys(f"{exp_prefix}:*"):
        redis_client.delete(key)

    print("\n" + "=" * 60)
    print("EXPERIMENT RESULT: SUCCESSFUL")
    print("All 7 verification steps completed as expected.")
    print("=" * 60)


if __name__ == "__main__":
    run_experiment()
