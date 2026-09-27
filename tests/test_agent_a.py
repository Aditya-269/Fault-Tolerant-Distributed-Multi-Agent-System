"""Tests for Agent A worker process, execution logic, and lifecycle transitions."""

import sys
from pathlib import Path
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.executor import execute_task
from coordinator.coordinator import Coordinator
from models.task import Task, TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from state.task_store import TaskStore


@pytest.fixture
def agent_environment():
    """Create an isolated test harness for Agent A and Coordinator."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_agent_{suffix}"
    queue_name = f"test_agent_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    publisher = TaskPublisher(queue_name=queue_name)
    consumer = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    coordinator = Coordinator(task_store=store, publisher=publisher)
    agent = AgentA(task_store=store, consumer=consumer)

    yield {
        "store": store,
        "publisher": publisher,
        "consumer": consumer,
        "coordinator": coordinator,
        "agent": agent,
        "queue_name": queue_name,
    }

    # Teardown
    coordinator.close()
    agent.close()

    # Clean up Redis keys
    for key in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(key)

    # Clean up RabbitMQ queue
    try:
        conn = create_connection()
        ch = conn.channel()
        ch.queue_delete(queue=queue_name)
        conn.close()
    except Exception:
        pass


def test_executor_calculation():
    """Verify calculation business logic runs in complete isolation."""
    task = Task(task_type="calculate", payload={"a": 10, "b": 20})
    result = execute_task(task)
    assert result == 30

    # Negative numbers and floats
    task2 = Task(task_type="calculate", payload={"a": -15, "b": 25.5})
    assert execute_task(task2) == 10.5

    # Missing parameters raise ValueError
    with pytest.raises(ValueError, match="must contain 'a' and 'b'"):
        execute_task(Task(task_type="calculate", payload={"a": 10}))

    # Unsupported task type
    with pytest.raises(ValueError, match="Unsupported task_type"):
        execute_task(Task(task_type="unknown_task", payload={}))


def test_agent_processes_task_end_to_end(agent_environment):
    """Verify complete lifecycle: receive -> execute -> store result -> complete -> ack."""
    coord: Coordinator = agent_environment["coordinator"]
    agent: AgentA = agent_environment["agent"]
    store: TaskStore = agent_environment["store"]
    consumer: TaskConsumer = agent_environment["consumer"]

    # 1. Dispatch task via coordinator
    task = coord.create_task(task_type="calculate", payload={"a": 10, "b": 20})
    assert task.status == TaskStatus.PENDING

    # 2. Agent consumes and processes task
    completed = agent.process_one(timeout=5.0)
    assert completed is not None
    assert completed.task_id == task.task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.agent_id == "agent_a"
    assert completed.result == 30
    assert completed.error is None

    # 3. Verify persisted state in Redis
    persisted = store.get_task(task.task_id)
    assert persisted is not None
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.agent_id == "agent_a"
    assert persisted.result == 30

    # 4. Verify message was ACK'd and queue is empty
    assert consumer.consume_one(timeout=0.2) is None


def test_task_transitions_to_processing_and_records_agent_id_during_execution(agent_environment):
    """Verify task is explicitly in PROCESSING state with agent_id set during calculation."""
    coord: Coordinator = agent_environment["coordinator"]
    store: TaskStore = agent_environment["store"]
    consumer: TaskConsumer = agent_environment["consumer"]

    observed_intermediate_state = {}

    def inspecting_executor(task: Task):
        # Query Redis midway through execution to verify intermediate state
        midway_state = store.get_task(task.task_id)
        if midway_state:
            observed_intermediate_state["status"] = midway_state.status
            observed_intermediate_state["agent_id"] = midway_state.agent_id
        return task.payload["a"] + task.payload["b"]

    custom_agent = AgentA(
        task_store=store,
        consumer=consumer,
        executor=inspecting_executor,
    )

    task = coord.create_task(task_type="calculate", payload={"a": 15, "b": 35})
    result_task = custom_agent.process_one(timeout=5.0)

    # Assert intermediate state was recorded in Redis
    assert observed_intermediate_state["status"] == TaskStatus.PROCESSING
    assert observed_intermediate_state["agent_id"] == "agent_a"

    # Assert final completed state
    assert result_task is not None
    assert result_task.status == TaskStatus.COMPLETED
    assert result_task.result == 50
    assert result_task.agent_id == "agent_a"
