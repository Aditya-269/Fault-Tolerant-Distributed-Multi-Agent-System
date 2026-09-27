"""Redis-based Agent Health Registry maintaining worker agent records."""

from datetime import datetime, timezone
import json
import logging
from typing import Optional
import redis

from config import settings
from models.agent import AgentHealth, AgentStatus

logger = logging.getLogger(__name__)


def _default_redis_client() -> redis.Redis:
    """Create a default Redis client using global settings."""
    return redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
    )


class HealthRegistry:
    """Maintains agent liveness metadata, statuses, and registration in Redis."""

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        key_prefix: str = "agent:health",
    ) -> None:
        """Initialize the health registry.

        Args:
            redis_client: Optional Redis client instance.
            key_prefix: Redis key prefix for health records (default: 'agent:health').
        """
        self.redis = redis_client or _default_redis_client()
        self.key_prefix = key_prefix
        self.registry_set_key = f"{self.key_prefix}:registered"

    def _agent_key(self, agent_id: str) -> str:
        """Generate Redis key for an agent's health record."""
        return f"{self.key_prefix}:{agent_id}"

    def register_agent(
        self,
        agent_id: str,
        status: AgentStatus | str = AgentStatus.STARTING,
    ) -> AgentHealth:
        """Register a new agent in Redis with initial status (default: STARTING).

        Args:
            agent_id: Unique agent identifier (e.g. 'agent_a').
            status: Initial status (default: AgentStatus.STARTING).

        Returns:
            The created AgentHealth instance.
        """
        record = AgentHealth(agent_id=agent_id, status=status)
        self.redis.sadd(self.registry_set_key, agent_id)
        self.redis.set(self._agent_key(agent_id), record.to_json())
        logger.info(
            f"[AGENT_REGISTERED] agent_id={agent_id} status={record.status.value}"
        )
        return record

    def mark_healthy(
        self,
        agent_id: str,
        timestamp: Optional[str] = None,
    ) -> Optional[AgentHealth]:
        """Update agent status to HEALTHY and update last_heartbeat timestamp.

        Args:
            agent_id: Unique agent identifier.
            timestamp: Optional ISO 8601 timestamp string (defaults to current UTC).

        Returns:
            Updated AgentHealth instance, or None if agent is not registered.
        """
        record = self.get_agent_health(agent_id)
        if record is None:
            return None

        record.status = AgentStatus.HEALTHY
        record.last_heartbeat = timestamp or datetime.now(timezone.utc).isoformat()
        record.touch()

        self.redis.set(self._agent_key(agent_id), record.to_json())
        logger.info(
            f"[AGENT_HEALTHY] agent_id={agent_id} last_heartbeat={record.last_heartbeat}"
        )
        return record

    def update_heartbeat(
        self,
        agent_id: str,
        timestamp: Optional[str] = None,
    ) -> Optional[AgentHealth]:
        """Update agent's last_heartbeat timestamp and touch updated_at.

        Args:
            agent_id: Unique agent identifier.
            timestamp: Optional ISO 8601 timestamp string (defaults to current UTC).

        Returns:
            Updated AgentHealth instance, or None if agent is not registered.
        """
        record = self.get_agent_health(agent_id)
        if record is None:
            return None

        record.last_heartbeat = timestamp or datetime.now(timezone.utc).isoformat()
        record.touch()

        self.redis.set(self._agent_key(agent_id), record.to_json())
        return record

    def update_status(
        self,
        agent_id: str,
        status: AgentStatus | str,
    ) -> Optional[AgentHealth]:
        """Update the health status of an agent (e.g. SUSPECTED, FAILED).

        Args:
            agent_id: Unique agent identifier.
            status: New health status.

        Returns:
            Updated AgentHealth instance, or None if agent is not registered.
        """
        record = self.get_agent_health(agent_id)
        if record is None:
            return None

        record.status = AgentStatus(status) if isinstance(status, str) else status
        record.touch()

        self.redis.set(self._agent_key(agent_id), record.to_json())
        return record

    def get_agent_health(self, agent_id: str) -> Optional[AgentHealth]:
        """Retrieve an agent's health record from Redis.

        Returns:
            AgentHealth if agent exists, None otherwise.
        """
        data = self.redis.get(self._agent_key(agent_id))
        if data is None:
            return None
        try:
            return AgentHealth.from_json(data)
        except (json.JSONDecodeError, KeyError, TypeError):
            return None

    def list_agents(self) -> list[AgentHealth]:
        """List all currently registered agent health records.

        Returns:
            List of AgentHealth records sorted by agent_id.
        """
        agent_ids = self.redis.smembers(self.registry_set_key)
        records: list[AgentHealth] = []
        for agent_id in sorted(agent_ids):
            rec = self.get_agent_health(agent_id)
            if rec is not None:
                records.append(rec)
        return records

    def deregister_agent(self, agent_id: str) -> bool:
        """Remove an agent from the health registry."""
        self.redis.srem(self.registry_set_key, agent_id)
        return bool(self.redis.delete(self._agent_key(agent_id)))
