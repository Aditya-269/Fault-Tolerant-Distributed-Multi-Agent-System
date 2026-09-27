"""State package.

Provides Redis task state management and persistence.
"""

from state.task_store import TaskStore

__all__ = ["TaskStore"]
