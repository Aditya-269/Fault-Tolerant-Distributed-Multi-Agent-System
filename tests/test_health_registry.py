"""Unit tests for the Agent Health Registry."""

from pathlib import Path
import sys
import time
import uuid
import pytest
import redis

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

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
def registry(redis_client):
    """Provide an isolated HealthRegistry instance with teardown cleanup."""
    prefix = f"test_reg_{uuid.uuid4().hex[:8]}"
    reg = HealthRegistry(redis_client=redis_client, key_prefix=prefix)
    yield reg

    # Teardown: delete all keys under test prefix
    keys = redis_client.keys(f"{prefix}*")
    if keys:
        redis_client.delete(*keys)


def test_agent_registration(registry: HealthRegistry):
    """Verify registering an agent creates record in Redis with STARTING status."""
    record = registry.register_agent("agent_test_1")

    assert record.agent_id == "agent_test_1"
    assert record.status == AgentStatus.STARTING
    assert record.last_heartbeat is None
    assert record.created_at is not None
    assert record.updated_at is not None

    # Verify retrieval from Redis
    retrieved = registry.get_agent_health("agent_test_1")
    assert retrieved is not None
    assert retrieved.agent_id == "agent_test_1"
    assert retrieved.status == AgentStatus.STARTING


def test_mark_agent_healthy(registry: HealthRegistry):
    """Verify transition to HEALTHY status and recording of last_heartbeat."""
    registry.register_agent("agent_healthy")

    updated = registry.mark_healthy("agent_healthy")
    assert updated is not None
    assert updated.status == AgentStatus.HEALTHY
    assert updated.last_heartbeat is not None

    # Verify persistence
    persisted = registry.get_agent_health("agent_healthy")
    assert persisted is not None
    assert persisted.status == AgentStatus.HEALTHY
    assert persisted.last_heartbeat == updated.last_heartbeat


def test_agent_becomes_healthy_via_heartbeat_sender(registry: HealthRegistry, redis_client):
    """Verify agent transitions to HEALTHY automatically upon starting its heartbeat."""
    hb_prefix = f"test_hb_{uuid.uuid4().hex[:8]}"
    sender = HeartbeatSender(
        agent_id="agent_auto_healthy",
        redis_client=redis_client,
        key_prefix=hb_prefix,
        health_registry=registry,
        interval=0.2,
        ttl=5,
    )

    try:
        # Start heartbeat
        sender.start()

        # Agent should now be registered and marked HEALTHY in the health registry
        health = registry.get_agent_health("agent_auto_healthy")
        assert health is not None
        assert health.status == AgentStatus.HEALTHY
        assert health.last_heartbeat is not None
    finally:
        sender.stop()
        keys = redis_client.keys(f"{hb_prefix}*")
        if keys:
            redis_client.delete(*keys)


def test_heartbeat_timestamp_update(registry: HealthRegistry):
    """Verify updating the heartbeat timestamp advances last_heartbeat and updated_at."""
    registry.register_agent("agent_time")
    registry.mark_healthy("agent_time", timestamp="2026-01-01T00:00:00+00:00")

    initial = registry.get_agent_health("agent_time")
    assert initial is not None
    assert initial.last_heartbeat == "2026-01-01T00:00:00+00:00"

    time.sleep(0.01)
    new_timestamp = "2026-01-01T00:01:00+00:00"
    updated = registry.update_heartbeat("agent_time", timestamp=new_timestamp)

    assert updated is not None
    assert updated.last_heartbeat == new_timestamp
    assert updated.updated_at >= initial.updated_at

    persisted = registry.get_agent_health("agent_time")
    assert persisted is not None
    assert persisted.last_heartbeat == new_timestamp


def test_multiple_independent_agents(registry: HealthRegistry):
    """Verify multiple agents can be registered, listed, and updated independently."""
    registry.register_agent("agent_alpha")
    registry.register_agent("agent_beta")
    registry.register_agent("agent_gamma")

    registry.mark_healthy("agent_alpha")
    registry.mark_healthy("agent_beta")
    # Leave agent_gamma as STARTING

    agents = registry.list_agents()
    assert len(agents) == 3
    agent_map = {a.agent_id: a for a in agents}

    assert agent_map["agent_alpha"].status == AgentStatus.HEALTHY
    assert agent_map["agent_beta"].status == AgentStatus.HEALTHY
    assert agent_map["agent_gamma"].status == AgentStatus.STARTING

    # Update one agent's status to SUSPECTED or FAILED
    registry.update_status("agent_alpha", AgentStatus.SUSPECTED)

    assert registry.get_agent_health("agent_alpha").status == AgentStatus.SUSPECTED
    assert registry.get_agent_health("agent_beta").status == AgentStatus.HEALTHY


def test_missing_agent_handling(registry: HealthRegistry):
    """Verify that operations on non-registered agents return None gracefully."""
    dummy_id = "non_existent_agent_999"

    assert registry.get_agent_health(dummy_id) is None
    assert registry.mark_healthy(dummy_id) is None
    assert registry.update_heartbeat(dummy_id) is None
    assert registry.update_status(dummy_id, AgentStatus.FAILED) is None
    assert registry.deregister_agent(dummy_id) is False
