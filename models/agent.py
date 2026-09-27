"""Agent health status and data models."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, Optional


class AgentStatus(str, Enum):
    """Lifecycle health status of an agent."""

    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    SUSPECTED = "SUSPECTED"
    FAILED = "FAILED"


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class AgentHealth:
    """Health record for a registered worker agent."""

    agent_id: str
    status: AgentStatus = AgentStatus.STARTING
    last_heartbeat: Optional[str] = None
    created_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)

    def __post_init__(self) -> None:
        """Ensure status is an AgentStatus enum."""
        if isinstance(self.status, str):
            self.status = AgentStatus(self.status)

    def touch(self) -> None:
        """Update updated_at timestamp to current UTC time."""
        self.updated_at = _utc_now_iso()

    def to_dict(self) -> dict[str, Any]:
        """Serialize health record to dictionary."""
        return {
            "agent_id": self.agent_id,
            "status": self.status.value,
            "last_heartbeat": self.last_heartbeat,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AgentHealth":
        """Reconstruct an AgentHealth instance from dictionary."""
        return cls(
            agent_id=data["agent_id"],
            status=AgentStatus(data["status"]),
            last_heartbeat=data.get("last_heartbeat"),
            created_at=data.get("created_at", _utc_now_iso()),
            updated_at=data.get("updated_at", _utc_now_iso()),
        )

    def to_json(self) -> str:
        """Serialize health record to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> "AgentHealth":
        """Deserialize health record from JSON string."""
        return cls.from_dict(json.loads(json_str))
