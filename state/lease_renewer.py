"""Periodic lease renewal mechanism for long-running distributed tasks."""

import logging
import threading
from typing import TYPE_CHECKING, Optional

from config import settings

if TYPE_CHECKING:
    from state.task_lease import TaskLease

logger = logging.getLogger(__name__)


class LeaseRenewer:
    """Periodically renews a distributed task lease while an agent processes a task.

    Guarantees:
    - Only renews the lease owned by that agent (enforced atomically by Redis Lua).
    - Periodically refreshes the TTL in the background (default: ~10s for 30s lease).
    - Stops immediately upon task completion or execution failure.
    - If the agent crashes, renewal ceases, allowing Redis TTL to expire the lease.
    """

    def __init__(
        self,
        lease: "TaskLease",
        task_id: str,
        agent_id: str,
        interval: Optional[float] = None,
        ttl: Optional[int] = None,
    ) -> None:
        """Initialize the lease renewer.

        Args:
            lease: The TaskLease manager instance.
            task_id: Identifier of the task being processed.
            agent_id: Identifier of the owning agent (e.g. 'agent_a').
            interval: Periodic renewal interval in seconds (default from settings: 10.0s).
            ttl: Lease TTL to refresh to upon renewal (defaults to lease.lease_ttl).
        """
        self.lease = lease
        self.task_id = task_id
        self.agent_id = agent_id
        self.interval = (
            interval
            if interval is not None
            else settings.lease.renewal_interval
        )
        self.ttl = ttl if ttl is not None else self.lease.lease_ttl

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        """Check whether the renewal background thread is active."""
        return self._thread is not None and self._thread.is_alive()

    def renew_once(self) -> bool:
        """Attempt a single lease renewal.

        Returns:
            True if the lease was renewed by owner; False otherwise.
        """
        return self.lease.renew(self.task_id, self.agent_id, ttl=self.ttl)

    def start(self) -> None:
        """Start background periodic renewal thread."""
        if self.is_running:
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"LeaseRenewer-{self.agent_id}-{self.task_id[:8]}",
            daemon=True,
        )
        self._thread.start()
        logger.debug(
            f"[LEASE_RENEWAL_STARTED] task_id={self.task_id} agent_id={self.agent_id} "
            f"interval={self.interval}s ttl={self.ttl}s"
        )

    def stop(self, timeout: float = 2.0) -> None:
        """Stop background renewal thread cleanly.

        Args:
            timeout: Maximum seconds to wait for thread join (default: 2.0s).
        """
        if not self.is_running:
            return

        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.debug(
            f"[LEASE_RENEWAL_STOPPED] task_id={self.task_id} agent_id={self.agent_id}"
        )

    def _run_loop(self) -> None:
        """Background loop executing periodic renewals until stopped."""
        while not self._stop_event.is_set():
            # Wait for next interval or stop signal
            stopped = self._stop_event.wait(self.interval)
            if stopped:
                break

            try:
                success = self.renew_once()
                if not success:
                    logger.warning(
                        f"[LEASE_RENEWAL_FAILED] task_id={self.task_id} agent_id={self.agent_id} "
                        f"Lease is no longer owned by agent or expired. Halting renewal."
                    )
                    break
            except Exception as exc:
                logger.error(
                    f"Error renewing lease for task {self.task_id}: {exc}",
                    exc_info=True,
                )

    def __enter__(self) -> "LeaseRenewer":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
