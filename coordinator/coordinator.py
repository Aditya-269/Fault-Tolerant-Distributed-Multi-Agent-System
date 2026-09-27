import logging
from pathlib import Path
import sys
from typing import Any, Optional

# Ensure project root is in sys.path for direct execution
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from models.task import Task, TaskStatus
from queue.publisher import TaskPublisher
from state.task_store import TaskStore

logger = logging.getLogger(__name__)



class Coordinator:
    """Coordinates task submission.

    Responsibilities:
    1. Accept/create a task.
    2. Generate a unique task_id.
    3. Create the task with status=PENDING.
    4. Store the task in Redis.
    5. Publish the task_id to RabbitMQ.
    6. Return the created Task instance.

    The coordinator strictly dispatches tasks and does NOT execute them.
    """

    def __init__(
        self,
        task_store: Optional[TaskStore] = None,
        publisher: Optional[TaskPublisher] = None,
    ) -> None:
        """Initialize the Coordinator with state store and message publisher.

        Args:
            task_store: Redis TaskStore instance (defaults to standard store).
            publisher: RabbitMQ TaskPublisher instance (defaults to standard publisher).
        """
        self.task_store = task_store or TaskStore()
        self._owns_publisher = publisher is None
        self.publisher = publisher or TaskPublisher()

    def create_task(
        self,
        task_type: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> Task:
        """Create a new task, persist it in Redis with status=PENDING, and publish to RabbitMQ.

        Args:
            task_type: Category of task (e.g., 'calculate').
            payload: Input parameters for the task (e.g., {'a': 10, 'b': 20}).

        Returns:
            The created Task with unique task_id and status=PENDING.
        """
        task = Task(
            task_type=task_type,
            payload=payload or {},
            status=TaskStatus.PENDING,
        )

        # 1. Store task in Redis
        self.task_store.save_task(task)
        logger.info(
            f"[TASK_CREATED] task_id={task.task_id} type={task.task_type} status={task.status.value} payload={task.payload}"
        )

        # 2. Publish task_id to RabbitMQ
        self.publisher.publish(task_id=task.task_id)
        logger.info(
            f"[TASK_PUBLISHED] task_id={task.task_id} queue={self.publisher.queue_name}"
        )

        return task


    def submit_task(
        self,
        task_type: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> Task:
        """Alias for create_task."""
        return self.create_task(task_type=task_type, payload=payload)

    def get_task(self, task_id: str) -> Optional[Task]:
        """Query the task state store for a given task ID."""
        return self.task_store.get_task(task_id)

    def close(self) -> None:
        """Release publisher resources cleanly."""
        if self._owns_publisher and self.publisher:
            self.publisher.close()

    def __enter__(self) -> "Coordinator":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


if __name__ == "__main__":
    # Demonstration CLI usage
    with Coordinator() as coord:
        demo_task = coord.create_task(
            task_type="calculate",
            payload={"a": 10, "b": 20},
        )
        print(f"Created and dispatched Task ID: {demo_task.task_id}")
        print(f"Status: {demo_task.status.value}")
        print(f"Payload: {demo_task.payload}")
