"""Recovery Manager for fault-tolerant task recovery and requeueing."""

import logging
from pathlib import Path
import sys
from typing import Optional

# Ensure project root is in sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from agents.failure_detector import FailureDetector
from models.task import Task, TaskStatus
from queue.publisher import TaskPublisher
from recovery.task_recovery import TaskRecovery
from state.health_registry import HealthRegistry
from state.task_lease import TaskLease
from state.task_store import TaskStore

logger = logging.getLogger(__name__)


class RecoveryManager:
    """Manages automatic detection and requeueing of recoverable tasks into RabbitMQ.

    Responsibilities:
    1. Find recoverable tasks (in PROCESSING state with confirmed dead agent and expired lease).
    2. Transition task status: PROCESSING -> RECOVERABLE in Redis.
    3. Publish the task_id back to RabbitMQ for healthy agents to consume.
    4. Make the task available without assigning a specific replacement agent.
    5. Do NOT directly execute the task.
    6. Prevent continuous requeueing of the same task in a tight loop.
    """

    def __init__(
        self,
        task_store: Optional[TaskStore] = None,
        task_lease: Optional[TaskLease] = None,
        failure_detector: Optional[FailureDetector] = None,
        health_registry: Optional[HealthRegistry] = None,
        publisher: Optional[TaskPublisher] = None,
        task_recovery: Optional[TaskRecovery] = None,
        queue_name: Optional[str] = None,
    ) -> None:
        """Initialize the RecoveryManager.

        Args:
            task_store: TaskStore instance for querying and updating task state in Redis.
            task_lease: TaskLease instance for checking lease expiration.
            failure_detector: FailureDetector instance for evaluating agent health.
            health_registry: Optional HealthRegistry instance.
            publisher: Optional TaskPublisher instance for republishing to RabbitMQ.
            task_recovery: Optional TaskRecovery instance for recovery evaluation logic.
            queue_name: Optional RabbitMQ queue name (passed to TaskPublisher if created).
        """
        self.task_store = task_store or TaskStore()
        self.task_lease = task_lease or TaskLease(redis_client=self.task_store.redis)
        self.health_registry = health_registry
        self.failure_detector = failure_detector or FailureDetector(
            redis_client=self.task_store.redis,
            health_registry=self.health_registry,
        )
        self.task_recovery = task_recovery or TaskRecovery(
            task_store=self.task_store,
            task_lease=self.task_lease,
            failure_detector=self.failure_detector,
            health_registry=self.health_registry,
        )

        self._owns_publisher = publisher is None
        if publisher is not None:
            self.publisher = publisher
        else:
            self.publisher = TaskPublisher(queue_name=queue_name)

    @property
    def queue_name(self) -> str:
        """Return the target RabbitMQ queue name."""
        return self.publisher.queue_name

    def find_processing_tasks(self) -> list[Task]:
        """Find all tasks currently in PROCESSING status in Redis."""
        return self.task_recovery.find_processing_tasks()

    def is_task_recoverable(self, task_id: str) -> bool:
        """Check whether a task currently satisfies all recovery criteria:
        1. Status is PROCESSING.
        2. Has an assigned agent_id.
        3. Assigned agent is confirmed FAILED (heartbeat missing/expired).
        4. Distributed task lease has EXPIRED.
        """
        return self.task_recovery.is_task_recoverable(task_id)

    def find_recoverable_tasks(self) -> list[Task]:
        """Scan all PROCESSING tasks and return those eligible for recovery."""
        return [t for t in self.find_processing_tasks() if self.is_task_recoverable(t.task_id)]

    def mark_recoverable(self, task_id: str) -> Optional[Task]:
        """Transition an eligible task to RECOVERABLE status in Redis."""
        return self.task_recovery.mark_recoverable(task_id)

    def requeue_task(self, task_id: str) -> bool:
        """Publish task_id back to RabbitMQ queue for distribution to healthy agents.

        Args:
            task_id: Unique identifier of the task to requeue.

        Returns:
            True if published successfully.
        """
        self.publisher.publish(task_id=task_id)
        logger.info(f"[TASK_REQUEUED] task_id={task_id} queue={self.publisher.queue_name}")
        return True

    def recover_task(self, task_id: str) -> Optional[Task]:
        """Recover a specific task: verify eligibility, transition state, and requeue.

        Guarantees:
        - Only tasks in PROCESSING with failed agent and expired lease are recovered.
        - Does NOT re-recover tasks that are already RECOVERABLE, preventing tight-loop requeueing.
        - Does NOT execute the task or assign a replacement agent.

        Args:
            task_id: Unique task identifier.

        Returns:
            Updated Task instance with status RECOVERABLE if successfully recovered,
            or None if the task is not eligible.
        """
        # 1. Verify eligibility (must be PROCESSING, agent failed, lease expired)
        if not self.is_task_recoverable(task_id):
            return None

        # 2. Transition PROCESSING -> RECOVERABLE in Redis
        task = self.mark_recoverable(task_id)
        if task is None:
            return None

        # 3. Publish task_id back to RabbitMQ
        self.requeue_task(task_id)
        logger.info(
            f"[TASK_RECOVERED] task_id={task_id} previous_agent_id={task.previous_agent_id} "
            f"attempts={task.recovery_attempts} status={task.status.value}"
        )
        return task

    def scan_and_recover(self) -> list[Task]:
        """Scan all PROCESSING tasks, transition eligible ones to RECOVERABLE, and requeue.

        Returns:
            List of tasks successfully recovered and published to RabbitMQ.
        """
        recovered_tasks: list[Task] = []
        processing_tasks = self.find_processing_tasks()

        for task in processing_tasks:
            recovered = self.recover_task(task.task_id)
            if recovered:
                recovered_tasks.append(recovered)

        return recovered_tasks

    def recover_tasks(self) -> list[Task]:
        """Alias for scan_and_recover."""
        return self.scan_and_recover()

    def close(self) -> None:
        """Cleanly close RabbitMQ publisher if owned by this manager."""
        if self._owns_publisher and self.publisher:
            self.publisher.close()

    def __enter__(self) -> "RecoveryManager":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
