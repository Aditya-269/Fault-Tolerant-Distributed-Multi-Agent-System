"""Distributed task lease management using atomic Redis operations."""

import logging
from typing import Optional
import redis

from config import settings

logger = logging.getLogger(__name__)

# Atomic Lua script for renewing a lease only if the caller currently owns it
_RENEW_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], tonumber(ARGV[2]))
else
    return 0
end
"""

# Atomic Lua script for releasing a lease only if the caller currently owns it
_RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


def _default_redis_client() -> redis.Redis:
    """Create default Redis client from global settings."""
    return redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
    )


class TaskLease:
    """Distributed task lease manager using atomic Redis operations.

    Distinguishes agent liveness (heartbeat) from task ownership (lease):
    - Heartbeat: 'Is the agent alive?'
    - Lease: 'Which agent currently owns this task?'

    Redis Key:
        lease:<task_id> -> <agent_id>

    Guarantees:
    - Atomicity: Uses SET NX EX, Lua scripts for safe renewal and release.
    - Mutual Exclusion: Only one agent can hold an active lease on a task.
    - Automatic Expiration: Leases expire automatically after TTL seconds if not renewed or released.
    """

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        lease_ttl: Optional[int] = None,
        prefix: Optional[str] = None,
    ) -> None:
        """Initialize the TaskLease manager.

        Args:
            redis_client: Optional Redis client instance.
            lease_ttl: Default lease duration in seconds (defaults to settings.lease.ttl, usually 30).
            prefix: Key prefix for lease keys in Redis (defaults to settings.lease.prefix, usually 'lease').
        """
        self.redis = redis_client or _default_redis_client()
        self.lease_ttl = lease_ttl if lease_ttl is not None else settings.lease.ttl
        self.prefix = prefix if prefix is not None else settings.lease.prefix

    def _lease_key(self, task_id: str) -> str:
        """Construct the Redis key for a task lease."""
        return f"{self.prefix}:{task_id}"

    def acquire(
        self,
        task_id: str,
        agent_id: str,
        ttl: Optional[int] = None,
    ) -> bool:
        """Atomically acquire a lease on a task.

        Equivalent to:
            SET lease:<task_id> <agent_id> NX EX <ttl>

        Args:
            task_id: Unique task identifier.
            agent_id: Identifier of the agent attempting to acquire the lease.
            ttl: Lease TTL in seconds (defaults to self.lease_ttl).

        Returns:
            True if the lease was acquired successfully; False if already leased.
        """
        key = self._lease_key(task_id)
        effective_ttl = ttl if ttl is not None else self.lease_ttl

        # NX: Only set the key if it does not already exist
        # EX: Set expiration in seconds
        success = bool(self.redis.set(key, agent_id, nx=True, ex=effective_ttl))

        if success:
            logger.info(
                f"[LEASE_ACQUIRED] task_id={task_id} agent_id={agent_id} ttl={effective_ttl}s"
            )
        else:
            owner = self.get_owner(task_id)
            logger.warning(
                f"[LEASE_ACQUIRE_FAILED] task_id={task_id} agent_id={agent_id} current_owner={owner}"
            )

        return success

    def renew(
        self,
        task_id: str,
        agent_id: str,
        ttl: Optional[int] = None,
    ) -> bool:
        """Atomically renew an existing lease if and only if the caller owns it.

        Args:
            task_id: Unique task identifier.
            agent_id: Identifier of the agent requesting renewal.
            ttl: New TTL in seconds (defaults to self.lease_ttl).

        Returns:
            True if the lease was renewed; False if caller is not owner or lease expired.
        """
        key = self._lease_key(task_id)
        effective_ttl = ttl if ttl is not None else self.lease_ttl

        # Atomically check ownership and reset expiration
        result = self.redis.eval(_RENEW_LUA, 1, key, agent_id, effective_ttl)
        success = bool(result)

        if success:
            logger.info(
                f"[LEASE_RENEWED] task_id={task_id} agent_id={agent_id} ttl={effective_ttl}s"
            )
        else:
            owner = self.get_owner(task_id)
            logger.warning(
                f"[LEASE_RENEW_FAILED] task_id={task_id} agent_id={agent_id} current_owner={owner}"
            )

        return success

    def release(
        self,
        task_id: str,
        agent_id: str,
    ) -> bool:
        """Atomically release an active lease if and only if the caller owns it.

        Prevents Agent B from mistakenly deleting Agent A's lease.

        Args:
            task_id: Unique task identifier.
            agent_id: Identifier of the agent requesting release.

        Returns:
            True if the lease was deleted by owner; False if caller is not owner or lease expired.
        """
        key = self._lease_key(task_id)

        # Atomically check ownership and delete key
        result = self.redis.eval(_RELEASE_LUA, 1, key, agent_id)
        success = bool(result)

        if success:
            logger.info(f"[LEASE_RELEASED] task_id={task_id} agent_id={agent_id}")
        else:
            owner = self.get_owner(task_id)
            logger.warning(
                f"[LEASE_RELEASE_FAILED] task_id={task_id} agent_id={agent_id} current_owner={owner}"
            )

        return success

    def get_owner(self, task_id: str) -> Optional[str]:
        """Get the identifier of the agent currently holding the lease.

        Args:
            task_id: Unique task identifier.

        Returns:
            The agent_id string if leased, or None if not leased / expired.
        """
        key = self._lease_key(task_id)
        owner = self.redis.get(key)
        return str(owner) if owner is not None else None

    def exists(self, task_id: str) -> bool:
        """Check whether an active lease currently exists for the task.

        Args:
            task_id: Unique task identifier.

        Returns:
            True if the task is currently leased; False otherwise.
        """
        key = self._lease_key(task_id)
        return bool(self.redis.exists(key))

    def is_leased(self, task_id: str) -> bool:
        """Convenience alias for exists()."""
        return self.exists(task_id)

    def get_ttl(self, task_id: str) -> int:
        """Get the remaining TTL for the task lease in seconds.

        Returns:
            Remaining seconds, -2 if key does not exist/expired, -1 if no TTL set.
        """
        key = self._lease_key(task_id)
        return int(self.redis.ttl(key))

    def create_renewer(
        self,
        task_id: str,
        agent_id: str,
        interval: Optional[float] = None,
        ttl: Optional[int] = None,
    ):
        """Create a LeaseRenewer instance for periodic background renewal of this lease.

        Args:
            task_id: Unique task identifier.
            agent_id: Owning agent identifier.
            interval: Periodic renewal interval (defaults to settings.lease.renewal_interval).
            ttl: Lease duration to refresh to (defaults to self.lease_ttl).

        Returns:
            LeaseRenewer configured for this lease and agent.
        """
        from state.lease_renewer import LeaseRenewer

        return LeaseRenewer(
            lease=self,
            task_id=task_id,
            agent_id=agent_id,
            interval=interval,
            ttl=ttl,
        )
