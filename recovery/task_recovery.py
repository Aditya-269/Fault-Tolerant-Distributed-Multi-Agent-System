"""Task recovery component for detecting and transitioning orphaned tasks."""

import logging
from typing import Optional

from agents.failure_detector import FailureDetector
from models.task import Task, TaskStatus, _utc_now_iso
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore

logger = logging.getLogger(__name__)


class TaskRecovery:
    """Evaluates tasks in PROCESSING state and identifies orphaned tasks as RECOVERABLE.

    Rules for a task to be RECOVERABLE:
    1. Task is currently in PROCESSING status.
    2. Task has an assigned agent_id.
    3. That assigned agent is confirmed FAILED (heartbeat expired or missing).
    4. The distributed task lease for that task has EXPIRED (no active lease in Redis).

    Guarantees:
    - If an agent is healthy, the task is NOT recoverable.
    - If the task lease is still active (even if agent is suspected/failed), the task is NOT yet recoverable.
    - Completed or failed tasks are never marked recoverable.
    - Multiple tasks can be evaluated independently without cross-task interference.
    """

    def __init__(
        self,
        task_store: Optional[TaskStore] = None,
        task_lease: Optional[TaskLease] = None,
        failure_detector: Optional[FailureDetector] = None,
        health_registry: Optional[HealthRegistry] = None,
    ) -> None:
        """Initialize the task recovery component.

        Args:
            task_store: TaskStore instance for querying and updating tasks.
            task_lease: TaskLease instance for checking lease expiration.
            failure_detector: FailureDetector instance for checking agent health.
            health_registry: Optional HealthRegistry instance.
        """
        self.task_store = task_store or TaskStore()
        self.task_lease = task_lease or TaskLease(redis_client=self.task_store.redis)
        self.health_registry = health_registry
        self.failure_detector = failure_detector or FailureDetector(
            redis_client=self.task_store.redis,
            health_registry=self.health_registry,
        )

    def find_processing_tasks(self) -> list[Task]:
        """Find all tasks currently in PROCESSING status in Redis."""
        return self.task_store.get_processing_tasks()

    def is_agent_failed(self, agent_id: str) -> bool:
        """Check whether an agent is confirmed failed."""
        return self.failure_detector.is_failed(agent_id)

    def is_lease_active(self, task_id: str) -> bool:
        """Check whether the task lease is still active in Redis."""
        return self.task_lease.exists(task_id)

    def is_task_recoverable(self, task_id: str) -> bool:
        """Determine whether a task is eligible for recovery.

        Criteria:
        1. Task exists and has status PROCESSING
        2. Task has an assigned agent_id
        3. Assigned agent is FAILED (heartbeat missing/expired)
        4. Task lease has EXPIRED (not active in Redis)

        Returns:
            True if all criteria are satisfied; False otherwise.
        """
        task = self.task_store.get_task(task_id)
        if task is None:
            return False

        # Only tasks in PROCESSING are eligible for recovery evaluation
        if task.status != TaskStatus.PROCESSING:
            return False

        # Must have an assigned agent
        if not task.agent_id:
            return False

        # Condition 1: Assigned agent must be FAILED
        if not self.is_agent_failed(task.agent_id):
            logger.debug(
                f"[RECOVERY_CHECK] task_id={task_id} agent_id={task.agent_id} "
                f"agent_healthy=True -> NOT recoverable"
            )
            return False

        # Condition 2: Task lease must be EXPIRED
        if self.is_lease_active(task_id):
            owner = self.task_lease.get_owner(task_id)
            logger.debug(
                f"[RECOVERY_CHECK] task_id={task_id} agent_id={task.agent_id} "
                f"lease_active=True owner={owner} -> NOT yet recoverable"
            )
            return False

        logger.info(
            f"[RECOVERY_CHECK] task_id={task_id} agent_id={task.agent_id} "
            f"agent_failed=True lease_expired=True -> RECOVERABLE"
        )
        return True

    def mark_recoverable(self, task_id: str) -> Optional[Task]:
        """Identify and transition an eligible task to RECOVERABLE in Redis.

        Records recovery metadata:
        - previous_agent_id
        - recovery_attempts (+1)
        - recovered_at
        - failure_detected_at
        - recovery_started_at

        Args:
            task_id: Unique task identifier.

        Returns:
            The updated Task instance with status RECOVERABLE if eligible; None otherwise.
        """
        if not self.is_task_recoverable(task_id):
            return None

        task = self.task_store.get_task(task_id)
        if not task:
            return None

        now = _utc_now_iso()
        prev_agent = task.agent_id

        logger.info(f"[AGENT_FAILED] agent_id={prev_agent} task_id={task_id}")
        logger.info(f"[TASK_RECOVERY_STARTED] task_id={task_id} previous_agent_id={prev_agent}")

        task.status = TaskStatus.RECOVERABLE
        task.previous_agent_id = prev_agent
        task.recovery_attempts += 1
        task.recovered_at = now
        task.failure_detected_at = now
        task.recovery_started_at = now
        task.touch()

        self.task_store.save_task(task)

        logger.info(
            f"[TASK_RECOVERABLE] task_id={task_id} previous_agent_id={prev_agent} "
            f"attempts={task.recovery_attempts} status={task.status.value}"
        )
        return task

    def find_recoverable_tasks(self) -> list[Task]:
        """Find all PROCESSING tasks that currently satisfy recovery criteria."""
        return [t for t in self.find_processing_tasks() if self.is_task_recoverable(t.task_id)]

    def scan_and_mark_recoverable(self) -> list[Task]:
        """Scan all PROCESSING tasks, check eligibility, and mark recoverable tasks.

        Returns:
            List of tasks successfully transitioned to RECOVERABLE.
        """
        recoverable_tasks: list[Task] = []
        processing_tasks = self.find_processing_tasks()

        for task in processing_tasks:
            marked = self.mark_recoverable(task.task_id)
            if marked:
                recoverable_tasks.append(marked)

        return recoverable_tasks


def __getattr__(name: str):
    if name == "RecoveryManager":
        from recovery.recovery_manager import RecoveryManager
        return RecoveryManager
    raise AttributeError(f"module {__name__} has no attribute {name}")
