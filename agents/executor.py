"""Task execution engine containing deterministic task logic.

Decouples task computation from message broker and state persistence logic.
"""

from typing import Any
from models.task import Task


def execute_task(task: Task) -> Any:
    """Execute a task deterministically based on its task_type and payload.

    Args:
        task: Task model instance containing task_type and payload.

    Returns:
        The computed result.

    Raises:
        ValueError: If the task_type is unsupported or payload is invalid.
    """
    if task.task_type == "calculate":
        if not isinstance(task.payload, dict):
            raise ValueError(f"Payload for 'calculate' task must be a dict, got {type(task.payload)}")

        if "a" not in task.payload or "b" not in task.payload:
            raise ValueError(f"Payload for 'calculate' task must contain 'a' and 'b': {task.payload}")

        a = task.payload["a"]
        b = task.payload["b"]

        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            raise ValueError(f"Inputs 'a' and 'b' must be numbers, got {type(a)} and {type(b)}")

        return a + b

    raise ValueError(f"Unsupported task_type: '{task.task_type}'")
