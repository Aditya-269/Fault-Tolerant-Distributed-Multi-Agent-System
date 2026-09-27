"""Agent failure detection based on Redis heartbeat TTL expiration."""

import logging
from typing import Optional
import redis

from config import settings
from models.agent import AgentStatus
from state.health_registry import HealthRegistry

logger = logging.getLogger(__name__)


def _default_redis_client() -> redis.Redis:
    """Create default Redis client from global settings."""
    return redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
    )


class FailureDetector:
    """Detects agent failures based strictly on Redis heartbeat key existence and TTL expiration.

    Rules:
    - If `heartbeat:<agent_id>` exists in Redis:
        Agent is considered alive / healthy.
    - If `heartbeat:<agent_id>` has expired / does not exist:
        Agent is considered failed / unhealthy.
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        health_registry: Optional[HealthRegistry] = None,
        heartbeat_prefix: Optional[str] = None,
    ) -> None:
        """Initialize the FailureDetector.

        Args:
            redis_client: Optional Redis client instance.
            health_registry: Optional HealthRegistry instance for status updates.
            heartbeat_prefix: Redis prefix for heartbeat keys (default: 'heartbeat').
        """
        self.redis = redis_client or _default_redis_client()
        self.health_registry = health_registry
        self.heartbeat_prefix = (
            heartbeat_prefix
            if heartbeat_prefix is not None
            else settings.heartbeat.prefix
        )

    def _heartbeat_key(self, agent_id: str) -> str:
        """Generate the Redis key for an agent's heartbeat."""
        return f"{self.heartbeat_prefix}:{agent_id}"

    def is_healthy(self, agent_id: str) -> bool:
        """Check whether an agent's heartbeat key exists in Redis.

        Returns:
            True if the heartbeat key is present and unexpired; False otherwise.
        """
        return bool(self.redis.exists(self._heartbeat_key(agent_id)))

    def is_failed(self, agent_id: str) -> bool:
        """Check whether an agent's heartbeat key has expired or is missing.

        Returns:
            True if the agent has failed (heartbeat missing/expired); False if healthy.
        """
        return not self.is_healthy(agent_id)

    def check_agent(
        self,
        agent_id: str,
        update_registry: bool = False,
    ) -> AgentStatus:
        """Determine health status of a specific agent.

        Args:
            agent_id: Identifier of the agent to check.
            update_registry: If True and health_registry is set, persists new status to Redis.

        Returns:
            AgentStatus.HEALTHY if heartbeat key exists, AgentStatus.FAILED otherwise.
        """
        status = AgentStatus.HEALTHY if self.is_healthy(agent_id) else AgentStatus.FAILED

        if update_registry and self.health_registry:
            self.health_registry.update_status(agent_id, status)
            logger.info(f"[FAILURE_DETECTOR] agent_id={agent_id} detected_status={status.value}")

        return status

    def scan_registered_agents(
        self,
        update_registry: bool = False,
    ) -> dict[str, AgentStatus]:
        """Scan all registered agents from the HealthRegistry and evaluate their liveness.

        Returns:
            Dictionary mapping agent_id -> AgentStatus (HEALTHY or FAILED).
        """
        if not self.health_registry:
            return {}

        results: dict[str, AgentStatus] = {}
        registered = self.health_registry.list_agents()

        for agent in registered:
            status = self.check_agent(agent.agent_id, update_registry=update_registry)
            results[agent.agent_id] = status

        return results

    def get_healthy_agents(
        self,
        agent_ids: Optional[list[str]] = None,
    ) -> list[str]:
        """Return list of agent IDs whose heartbeats are currently active."""
        targets = agent_ids
        if targets is None:
            if self.health_registry:
                targets = [a.agent_id for a in self.health_registry.list_agents()]
            else:
                targets = []

        return [aid for aid in targets if self.is_healthy(aid)]

    def get_failed_agents(
        self,
        agent_ids: Optional[list[str]] = None,
    ) -> list[str]:
        """Return list of agent IDs whose heartbeats have expired or are missing."""
        targets = agent_ids
        if targets is None:
            if self.health_registry:
                targets = [a.agent_id for a in self.health_registry.list_agents()]
            else:
                targets = []

        return [aid for aid in targets if self.is_failed(aid)]
