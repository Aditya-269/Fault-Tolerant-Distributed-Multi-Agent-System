"""Agents package.

Contains worker agent consumers and task executors.
"""

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from agents.executor import execute_task
from agents.worker import Worker

__all__ = ["AgentA", "AgentB", "Worker", "execute_task"]
