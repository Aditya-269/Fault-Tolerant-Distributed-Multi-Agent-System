"""Tests for agent failure detection based on Redis heartbeat TTL expiration."""

from pathlib import Path
import sys
import time
import uuid
import pytest
import redis

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.failure_detector import FailureDetector
from agents.heartbeat import HeartbeatSender
from config import settings
from models.agent import AgentStatus
from state.health_registry import HealthRegistry


@pytest.fixture
def redis_client():
    """Create a Redis client instance."""
    client = redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
    )
    yield client
    client.close()


@pytest.fixture
def test_environment(redis_client):
    """Provide isolated prefixes and instances for FailureDetector and HealthRegistry."""
    suffix = uuid.uuid4().hex[:8]
    hb_prefix = f"test_hb_{suffix}"
    reg_prefix = f"test_reg_{suffix}"

    registry = HealthRegistry(redis_client=redis_client, key_prefix=reg_prefix)
    detector = FailureDetector(
        redis_client=redis_client,
        health_registry=registry,
        heartbeat_prefix=hb_prefix,
    )

    yield {
        "registry": registry,
        "detector": detector,
        "hb_prefix": hb_prefix,
        "reg_prefix": reg_prefix,
    }

    # Teardown
    for prefix in (hb_prefix, reg_prefix):
        keys = redis_client.keys(f"{prefix}*")
        if keys:
            redis_client.delete(*keys)


def test_1_healthy_agent_detection(test_environment, redis_client):
    """TEST 1: Healthy agent detection when heartbeat exists in Redis."""
    detector: FailureDetector = test_environment["detector"]
    hb_prefix: str = test_environment["hb_prefix"]

    sender = HeartbeatSender(
        agent_id="agent_healthy",
        redis_client=redis_client,
        key_prefix=hb_prefix,
        ttl=15,
    )
    sender.send_heartbeat()

    assert detector.is_healthy("agent_healthy") is True
    assert detector.is_failed("agent_healthy") is False
    assert detector.check_agent("agent_healthy") == AgentStatus.HEALTHY


def test_2_expired_heartbeat_detection(test_environment):
    """TEST 2: Expired heartbeat detection when key is absent or expired."""
    detector: FailureDetector = test_environment["detector"]

    # Non-existent or expired agent
    assert detector.is_healthy("agent_expired") is False
    assert detector.is_failed("agent_expired") is True
    assert detector.check_agent("agent_expired") == AgentStatus.FAILED


def test_3_multiple_agents_one_healthy_one_expired(test_environment, redis_client):
    """TEST 3: Multiple agents where one is healthy and another has expired."""
    detector: FailureDetector = test_environment["detector"]
    registry: HealthRegistry = test_environment["registry"]
    hb_prefix: str = test_environment["hb_prefix"]

    # Register both agents
    registry.register_agent("agent_alive")
    registry.register_agent("agent_dead")

    # Alive agent gets active heartbeat
    sender_alive = HeartbeatSender(
        agent_id="agent_alive",
        redis_client=redis_client,
        key_prefix=hb_prefix,
        ttl=10,
    )
    sender_alive.send_heartbeat()

    # Dead agent gets 1-second heartbeat that expires
    sender_dead = HeartbeatSender(
        agent_id="agent_dead",
        redis_client=redis_client,
        key_prefix=hb_prefix,
        ttl=1,
    )
    sender_dead.send_heartbeat()

    # Wait for agent_dead heartbeat key to expire via Redis TTL
    time.sleep(1.2)

    # 1. Independent boolean liveness checks
    assert detector.is_healthy("agent_alive") is True
    assert detector.is_failed("agent_alive") is False

    assert detector.is_healthy("agent_dead") is False
    assert detector.is_failed("agent_dead") is True

    # 2. Filtering lists
    assert detector.get_healthy_agents() == ["agent_alive"]
    assert detector.get_failed_agents() == ["agent_dead"]

    # 3. Synchronize status with registry
    scanned = detector.scan_registered_agents(update_registry=True)
    assert scanned == {
        "agent_alive": AgentStatus.HEALTHY,
        "agent_dead": AgentStatus.FAILED,
    }

    # Verify updated records in Redis
    assert registry.get_agent_health("agent_alive").status == AgentStatus.HEALTHY
    assert registry.get_agent_health("agent_dead").status == AgentStatus.FAILED


def test_4_agent_becomes_failed_after_heartbeat_expiration(test_environment, redis_client):
    """TEST 4: Agent transitions from HEALTHY to FAILED strictly after Redis TTL expires."""
    detector: FailureDetector = test_environment["detector"]
    registry: HealthRegistry = test_environment["registry"]
    hb_prefix: str = test_environment["hb_prefix"]

    registry.register_agent("agent_monitored")

    # Start with active 1s heartbeat
    sender = HeartbeatSender(
        agent_id="agent_monitored",
        redis_client=redis_client,
        key_prefix=hb_prefix,
        ttl=1,
    )
    sender.send_heartbeat()

    # Step A: Initially healthy
    initial_status = detector.check_agent("agent_monitored", update_registry=True)
    assert initial_status == AgentStatus.HEALTHY
    assert registry.get_agent_health("agent_monitored").status == AgentStatus.HEALTHY

    # Step B: Wait for Redis TTL to expire (no Python timers, pure Redis TTL)
    time.sleep(1.2)

    # Step C: Re-evaluating confirms failure
    expired_status = detector.check_agent("agent_monitored", update_registry=True)
    assert expired_status == AgentStatus.FAILED
    assert registry.get_agent_health("agent_monitored").status == AgentStatus.FAILED
