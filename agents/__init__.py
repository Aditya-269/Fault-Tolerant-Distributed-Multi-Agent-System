"""Agents package.

Contains worker agent consumers and task executors.
"""

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from agents.executor import execute_task
from agents.failure_detector import FailureDetector
from agents.heartbeat import (
    HeartbeatSender,
    get_agent_heartbeat,
    get_agent_ttl,
    is_agent_alive,
)
from agents.worker import Worker
from state.lease_renewer import LeaseRenewer
from state.task_lease import TaskLease

__all__ = [
    "AgentA",
    "AgentB",
    "FailureDetector",
    "HeartbeatSender",
    "LeaseRenewer",
    "TaskLease",
    "Worker",
    "execute_task",
    "get_agent_heartbeat",
    "get_agent_ttl",
    "is_agent_alive",
]


