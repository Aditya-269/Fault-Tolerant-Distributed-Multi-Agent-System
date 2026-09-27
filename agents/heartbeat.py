"""Reusable Redis-based agent heartbeat mechanism for liveness tracking."""

from datetime import datetime, timezone
import json
import logging
import threading
import time
from typing import Any, Optional
import redis

from config import settings

logger = logging.getLogger(__name__)


def _default_redis_client() -> redis.Redis:
    """Create default Redis client using global settings."""
    return redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
    )


class HeartbeatSender:
    """Periodically writes agent heartbeat to Redis with automatic TTL expiration.

    Operates in a background daemon thread to avoid blocking task processing.
    """

    def __init__(
        self,
        agent_id: str,
        redis_client: Optional[redis.Redis] = None,
        interval: Optional[float] = None,
        ttl: Optional[int] = None,
        key_prefix: Optional[str] = None,
        health_registry: Optional[Any] = None,
    ) -> None:
        """Initialize the heartbeat sender.

        Args:
            agent_id: Unique agent identifier (e.g. 'agent_a').
            redis_client: Optional Redis client instance.
            interval: Periodic heartbeat interval in seconds (default: 5.0).
            ttl: Redis key time-to-live in seconds (default: 15).
            key_prefix: Redis key prefix (default: 'heartbeat').
            health_registry: Optional HealthRegistry instance to register and mark HEALTHY.
        """
        self.agent_id = agent_id
        self.redis = redis_client or _default_redis_client()
        self.interval = (
            interval if interval is not None else settings.heartbeat.interval
        )
        self.ttl = ttl if ttl is not None else settings.heartbeat.ttl
        self.key_prefix = (
            key_prefix if key_prefix is not None else settings.heartbeat.prefix
        )
        self.health_registry = health_registry

        self.key = f"{self.key_prefix}:{self.agent_id}"
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def send_heartbeat(self) -> dict[str, Any]:
        """Register or update the heartbeat key in Redis with configured TTL.

        Returns:
            The recorded heartbeat payload dict.
        """
        payload: dict[str, Any] = {
            "agent_id": self.agent_id,
            "last_heartbeat": datetime.now(timezone.utc).isoformat(),
            "timestamp": time.time(),
        }
        self.redis.set(self.key, json.dumps(payload), ex=self.ttl)
        logger.debug(
            f"[HEARTBEAT_SENT] agent_id={self.agent_id} key={self.key} ttl={self.ttl}"
        )

        if self.health_registry:
            self.health_registry.update_heartbeat(
                self.agent_id, payload["last_heartbeat"]
            )

        return payload

    def _run_loop(self) -> None:
        """Background thread loop refreshing heartbeat periodically."""
        while not self._stop_event.is_set():
            try:
                self.send_heartbeat()
            except Exception as exc:
                logger.warning(
                    f"Failed to refresh heartbeat for {self.agent_id}: {exc}"
                )

            # Wait for next interval or wake immediately on stop
            self._stop_event.wait(timeout=self.interval)

    def start(self) -> None:
        """Start the periodic background heartbeat thread and mark agent HEALTHY."""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        # Send initial heartbeat immediately before loop begins
        payload = self.send_heartbeat()

        if self.health_registry:
            if self.health_registry.get_agent_health(self.agent_id) is None:
                self.health_registry.register_agent(self.agent_id)
            self.health_registry.mark_healthy(
                self.agent_id, payload["last_heartbeat"]
            )

        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"heartbeat-{self.agent_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"Heartbeat thread started for [{self.agent_id}] (interval={self.interval}s, ttl={self.ttl}s)"
        )


    def stop(self) -> None:
        """Stop the background heartbeat thread cleanly."""
        if not self._thread or not self._thread.is_alive():
            return

        self._stop_event.set()
        self._thread.join(timeout=2.0)
        self._thread = None
        logger.info(f"Heartbeat thread stopped for [{self.agent_id}]")

    @property
    def is_running(self) -> bool:
        """Check if background heartbeat thread is currently active."""
        return bool(
            self._thread
            and self._thread.is_alive()
            and not self._stop_event.is_set()
        )

    def get_heartbeat(self) -> Optional[dict[str, Any]]:
        """Retrieve current heartbeat data from Redis."""
        return get_agent_heartbeat(
            agent_id=self.agent_id,
            redis_client=self.redis,
            key_prefix=self.key_prefix,
        )

    def get_ttl(self) -> int:
        """Retrieve remaining TTL in seconds for this agent's heartbeat."""
        return get_agent_ttl(
            agent_id=self.agent_id,
            redis_client=self.redis,
            key_prefix=self.key_prefix,
        )

    def __enter__(self) -> "HeartbeatSender":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


def get_agent_heartbeat(
    agent_id: str,
    redis_client: Optional[redis.Redis] = None,
    key_prefix: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Retrieve and deserialize an agent's heartbeat payload from Redis.

    Returns:
        Heartbeat dictionary if key exists, None otherwise.
    """
    client = redis_client or _default_redis_client()
    prefix = key_prefix if key_prefix is not None else settings.heartbeat.prefix
    key = f"{prefix}:{agent_id}"

    data = client.get(key)
    if data is None:
        return None
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return None


def is_agent_alive(
    agent_id: str,
    redis_client: Optional[redis.Redis] = None,
    key_prefix: Optional[str] = None,
) -> bool:
    """Check if an agent is currently considered alive based on heartbeat existence."""
    client = redis_client or _default_redis_client()
    prefix = key_prefix if key_prefix is not None else settings.heartbeat.prefix
    key = f"{prefix}:{agent_id}"
    return bool(client.exists(key))


def get_agent_ttl(
    agent_id: str,
    redis_client: Optional[redis.Redis] = None,
    key_prefix: Optional[str] = None,
) -> int:
    """Return remaining TTL seconds for an agent's heartbeat key (-2 if missing, -1 if no TTL)."""
    client = redis_client or _default_redis_client()
    prefix = key_prefix if key_prefix is not None else settings.heartbeat.prefix
    key = f"{prefix}:{agent_id}"
    return client.ttl(key)
