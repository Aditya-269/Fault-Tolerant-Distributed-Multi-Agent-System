"""Task data model and lifecycle definitions."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, Optional
import uuid


class TaskStatus(str, Enum):
    """Task lifecycle states."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Task:
    """Represents a discrete unit of work in the distributed system."""

    task_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: TaskStatus = TaskStatus.PENDING
    agent_id: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)

    def __post_init__(self) -> None:
        """Ensure status is a TaskStatus enum instance."""
        if isinstance(self.status, str):
            self.status = TaskStatus(self.status)

    def touch(self) -> None:
        """Update the updated_at timestamp to current UTC time."""
        self.updated_at = _utc_now_iso()

    def to_dict(self) -> dict[str, Any]:
        """Serialize task to a plain dictionary."""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "payload": self.payload,
            "status": self.status.value,
            "agent_id": self.agent_id,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        """Reconstruct a Task from a dictionary."""
        return cls(
            task_id=data["task_id"],
            task_type=data["task_type"],
            payload=data.get("payload", {}),
            status=TaskStatus(data["status"]),
            agent_id=data.get("agent_id"),
            result=data.get("result"),
            error=data.get("error"),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
        )

    def to_json(self) -> str:
        """Serialize task to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "Task":
        """Deserialize task from JSON string."""
        return cls.from_dict(json.loads(json_str))
