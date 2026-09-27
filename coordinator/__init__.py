"""Coordinator package.

Manages task submission, unique ID generation, state persistence, and dispatch.
"""

from coordinator.coordinator import Coordinator
from recovery.recovery_manager import RecoveryManager
from recovery.task_recovery import TaskRecovery

__all__ = ["Coordinator", "RecoveryManager", "TaskRecovery"]
