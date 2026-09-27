"""Redis-based shared state store for tasks."""

from typing import Any, Optional
import redis

from config import settings
from models.task import Task, TaskStatus


class TaskStore:
    """Shared Redis task store providing CRUD and lifecycle operations."""

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        key_prefix: str = "task",
    ) -> None:
        """Initialize the task store with a Redis client.

        Args:
            redis_client: Optional existing Redis client. If omitted, creates
                          one from the global settings.
            key_prefix: Prefix for Redis keys (default: "task").
        """
        if redis_client is not None:
            self.redis = redis_client
        else:
            self.redis = redis.Redis(
                host=settings.redis.host,
                port=settings.redis.port,
                db=settings.redis.db,
                password=settings.redis.password,
                decode_responses=True,
            )
        self.key_prefix = key_prefix

    def _key(self, task_id: str) -> str:
        """Generate Redis key for a task ID."""
        return f"{self.key_prefix}:{task_id}"

    def save_task(self, task: Task) -> None:
        """Store or overwrite a task in Redis."""
        key = self._key(task.task_id)
        self.redis.set(key, task.to_json())

    def create_task(
        self,
        task_type: str,
        payload: Optional[dict[str, Any]] = None,
        task_id: Optional[str] = None,
    ) -> Task:
        """Create, store, and return a new task."""
        kwargs: dict[str, Any] = {"task_type": task_type}
        if payload is not None:
            kwargs["payload"] = payload
        if task_id is not None:
            kwargs["task_id"] = task_id

        task = Task(**kwargs)
        self.save_task(task)
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Retrieve a task by ID. Returns None if task does not exist."""
        data = self.redis.get(self._key(task_id))
        if data is None:
            return None
        return Task.from_json(data)

    def task_exists(self, task_id: str) -> bool:
        """Check whether a task exists in Redis."""
        return bool(self.redis.exists(self._key(task_id)))

    def update_status(
        self,
        task_id: str,
        status: TaskStatus | str,
    ) -> Optional[Task]:
        """Update the status of an existing task and touch updated_at."""
        task = self.get_task(task_id)
        if task is None:
            return None

        task.status = TaskStatus(status) if isinstance(status, str) else status
        task.touch()
        self.save_task(task)
        return task

    def update_agent_id(
        self,
        task_id: str,
        agent_id: str,
    ) -> Optional[Task]:
        """Assign or update the agent ID executing the task."""
        task = self.get_task(task_id)
        if task is None:
            return None

        task.agent_id = agent_id
        task.touch()
        self.save_task(task)
        return task

    def store_result(
        self,
        task_id: str,
        result: Any,
        status: TaskStatus | str = TaskStatus.COMPLETED,
        completed_at: Optional[str] = None,
        recovery_duration: Optional[float] = None,
    ) -> Optional[Task]:
        """Record the execution result and transition task status (default: COMPLETED)."""
        task = self.get_task(task_id)
        if task is None:
            return None

        task.result = result
        task.status = TaskStatus(status) if isinstance(status, str) else status
        if completed_at is not None:
            task.completed_at = completed_at
        if recovery_duration is not None:
            task.recovery_duration = recovery_duration
        task.touch()
        self.save_task(task)
        return task

    def store_error(
        self,
        task_id: str,
        error: str,
        status: TaskStatus | str = TaskStatus.FAILED,
    ) -> Optional[Task]:
        """Record an error message and transition task status (default: FAILED)."""
        task = self.get_task(task_id)
        if task is None:
            return None

        task.error = error
        task.status = TaskStatus(status) if isinstance(status, str) else status
        task.touch()
        self.save_task(task)
        return task

    def delete_task(self, task_id: str) -> bool:
        """Delete a task from Redis. Returns True if deleted, False otherwise."""
        return bool(self.redis.delete(self._key(task_id)))

    def list_tasks(self) -> list[Task]:
        """List all tasks stored in Redis under this store's key prefix."""
        tasks: list[Task] = []
        for key in self.redis.keys(f"{self.key_prefix}:*"):
            # Skip lease or subkey namespaces if present
            if ":lease:" in key:
                continue
            data = self.redis.get(key)
            if data:
                try:
                    tasks.append(Task.from_json(data))
                except Exception:
                    pass
        return tasks

    def get_tasks_by_status(self, status: TaskStatus | str) -> list[Task]:
        """Retrieve all tasks currently in a specific status."""
        target_status = TaskStatus(status) if isinstance(status, str) else status
        return [t for t in self.list_tasks() if t.status == target_status]

    def get_processing_tasks(self) -> list[Task]:
        """Retrieve all tasks currently in PROCESSING status."""
        return self.get_tasks_by_status(TaskStatus.PROCESSING)
