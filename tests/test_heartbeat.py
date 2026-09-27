"""Unit tests for the reusable Redis-based heartbeat mechanism."""

from pathlib import Path
import sys
import time
import uuid
import pytest
import redis

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.heartbeat import (
    HeartbeatSender,
    get_agent_heartbeat,
    get_agent_ttl,
    is_agent_alive,
)
from config import settings


@pytest.fixture
def redis_client():
    """Create a Redis client instance for heartbeat tests."""
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
def test_prefix(redis_client):
    """Generate an isolated heartbeat prefix and clean up keys upon test completion."""
    prefix = f"test_hb_{uuid.uuid4().hex[:8]}"
    yield prefix

    # Teardown: delete all keys under test prefix
    keys = redis_client.keys(f"{prefix}:*")
    if keys:
        redis_client.delete(*keys)


def test_heartbeat_key_creation(redis_client, test_prefix):
    """Verify heartbeat key creation in Redis with proper JSON payload."""
    sender = HeartbeatSender(
        agent_id="agent_1",
        redis_client=redis_client,
        key_prefix=test_prefix,
        ttl=10,
    )

    payload = sender.send_heartbeat()
    assert payload["agent_id"] == "agent_1"
    assert "last_heartbeat" in payload
    assert "timestamp" in payload

    # Verify key in Redis
    assert is_agent_alive("agent_1", redis_client, test_prefix) is True
    data = get_agent_heartbeat("agent_1", redis_client, test_prefix)
    assert data is not None
    assert data["agent_id"] == "agent_1"
    assert data["timestamp"] == payload["timestamp"]


def test_heartbeat_ttl_exists(redis_client, test_prefix):
    """Verify that a Redis TTL is applied and retrievable."""
    sender = HeartbeatSender(
        agent_id="agent_ttl",
        redis_client=redis_client,
        key_prefix=test_prefix,
        ttl=15,
    )

    sender.send_heartbeat()
    remaining_ttl = sender.get_ttl()
    assert 0 < remaining_ttl <= 15

    helper_ttl = get_agent_ttl("agent_ttl", redis_client, test_prefix)
    assert 0 < helper_ttl <= 15


def test_heartbeat_refresh(redis_client, test_prefix):
    """Verify periodic background refresh updates the timestamp in Redis."""
    sender = HeartbeatSender(
        agent_id="agent_refresh",
        redis_client=redis_client,
        interval=0.1,  # Fast 100ms interval for testing
        ttl=5,
        key_prefix=test_prefix,
    )

    sender.start()
    assert sender.is_running is True

    # Record first heartbeat timestamp
    first_hb = get_agent_heartbeat("agent_refresh", redis_client, test_prefix)
    assert first_hb is not None
    first_ts = first_hb["timestamp"]

    # Allow background thread to refresh
    time.sleep(0.25)

    refreshed_hb = get_agent_heartbeat("agent_refresh", redis_client, test_prefix)
    assert refreshed_hb is not None
    second_ts = refreshed_hb["timestamp"]

    assert second_ts > first_ts

    sender.stop()
    assert sender.is_running is False


def test_heartbeat_expiry(redis_client, test_prefix):
    """Verify that a heartbeat key automatically expires when refresh stops."""
    sender = HeartbeatSender(
        agent_id="agent_expiring",
        redis_client=redis_client,
        ttl=1,  # 1-second TTL
        key_prefix=test_prefix,
    )

    # Send one-off heartbeat without running the refresh thread
    sender.send_heartbeat()
    assert is_agent_alive("agent_expiring", redis_client, test_prefix) is True

    # Wait for TTL to expire
    time.sleep(1.2)

    assert is_agent_alive("agent_expiring", redis_client, test_prefix) is False
    assert get_agent_heartbeat("agent_expiring", redis_client, test_prefix) is None
    assert get_agent_ttl("agent_expiring", redis_client, test_prefix) == -2


def test_multiple_agents_independent_heartbeats(redis_client, test_prefix):
    """Verify that multiple agents maintain independent heartbeat keys and lifespans."""
    sender_a = HeartbeatSender(
        agent_id="agent_a",
        redis_client=redis_client,
        ttl=1,  # Short TTL to expire first
        key_prefix=test_prefix,
    )
    sender_b = HeartbeatSender(
        agent_id="agent_b",
        redis_client=redis_client,
        ttl=10,  # Longer TTL to stay alive
        key_prefix=test_prefix,
    )

    sender_a.send_heartbeat()
    sender_b.send_heartbeat()

    # Initially both are alive
    assert is_agent_alive("agent_a", redis_client, test_prefix) is True
    assert is_agent_alive("agent_b", redis_client, test_prefix) is True

    # Wait for agent_a to expire
    time.sleep(1.2)

    assert is_agent_alive("agent_a", redis_client, test_prefix) is False
    assert is_agent_alive("agent_b", redis_client, test_prefix) is True


def test_heartbeat_stops_cleanly_without_blocking(redis_client, test_prefix):
    """Verify that stopping a heartbeat thread terminates immediately without waiting for interval."""
    sender = HeartbeatSender(
        agent_id="agent_quick_stop",
        redis_client=redis_client,
        interval=30.0,  # Long interval
        ttl=60,
        key_prefix=test_prefix,
    )

    sender.start()
    assert sender.is_running is True

    start_time = time.time()
    sender.stop()
    stop_duration = time.time() - start_time

    assert sender.is_running is False
    # Stop should wake immediately from wait, taking well under 0.5s
    assert stop_duration < 0.5
