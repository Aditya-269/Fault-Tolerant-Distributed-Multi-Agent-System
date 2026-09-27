"""Queue package.

Will provide RabbitMQ message queue connections, channel management, and
queue declaration helpers.
(Implementation deferred to subsequent batches).

NOTE: The Python standard library also contains a module named 'queue'.
To prevent third-party packages (such as redis, urllib3, etc.) from breaking
when importing the standard library queue, we gracefully re-export standard
library queue members here.
"""

from __future__ import annotations
import importlib.util
from pathlib import Path
import sys

# Dynamically locate and re-export standard library queue members
_project_root = Path(__file__).resolve().parent.parent
_stdlib_queue_path = None
for _p in sys.path:
    if _p:
        try:
            if Path(_p).resolve() != _project_root:
                _candidate = Path(_p) / "queue.py"
                if _candidate.is_file():
                    _stdlib_queue_path = str(_candidate)
                    break
        except Exception:
            continue

if _stdlib_queue_path:
    _spec = importlib.util.spec_from_file_location("_stdlib_queue", _stdlib_queue_path)
    if _spec and _spec.loader:
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        for _name in dir(_mod):
            if not _name.startswith("__"):
                globals()[_name] = getattr(_mod, _name)

# Project Queue exports
from queue.connection import create_connection
from queue.message import QueueMessage
from queue.publisher import TaskPublisher
from queue.consumer import TaskConsumer

__all__ = [
    "create_connection",
    "QueueMessage",
    "TaskPublisher",
    "TaskConsumer",
]

