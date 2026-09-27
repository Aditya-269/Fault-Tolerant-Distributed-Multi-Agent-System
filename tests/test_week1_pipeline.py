"""Comprehensive Week 1 integration test suite verifying the end-to-end pipeline.

Verifies:
TEST 1: Single task end-to-end execution.
TEST 2: Multiple tasks with multiple workers.
TEST 3: Correct task state transitions (PENDING -> PROCESSING -> COMPLETED).
TEST 4: Correct result storage.
TEST 5: Correct agent_id recording.
TEST 6: Invalid task handling (FAILED state, error capture, clean nack).
TEST 7: RabbitMQ message acknowledgement.
TEST 8: Redis state remains consistent after successful processing.
"""

import logging
from pathlib import Path
import sys
import uuid
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.agent_a import AgentA
from agents.agent_b import AgentB
from coordinator.coordinator import Coordinator
from models.task import Task, TaskStatus
from queue.connection import create_connection
from queue.consumer import TaskConsumer
from queue.publisher import TaskPublisher
from state.task_store import TaskStore


@pytest.fixture
def pipeline_env():
    """Create an isolated pipeline harness for Week 1 integration testing."""
    suffix = uuid.uuid4().hex[:8]
    key_prefix = f"test_pipe_{suffix}"
    queue_name = f"test_pipe_queue_{suffix}"

    store = TaskStore(key_prefix=key_prefix)
    publisher = TaskPublisher(queue_name=queue_name)
    coordinator = Coordinator(task_store=store, publisher=publisher)

    consumer_a = TaskConsumer(queue_name=queue_name, prefetch_count=1)
    consumer_b = TaskConsumer(queue_name=queue_name, prefetch_count=1)

    agent_a = AgentA(task_store=store, consumer=consumer_a)
    agent_b = AgentB(task_store=store, consumer=consumer_b)

    yield {
        "store": store,
        "coordinator": coordinator,
        "agent_a": agent_a,
        "agent_b": agent_b,
        "consumer_a": consumer_a,
        "consumer_b": consumer_b,
        "queue_name": queue_name,
    }

    # Clean teardown
    coordinator.close()
    agent_a.close()
    agent_b.close()

    # Clear Redis keys
    for key in store.redis.keys(f"{key_prefix}:*"):
        store.redis.delete(key)

    # Delete RabbitMQ test queue
    try:
        conn = create_connection()
        ch = conn.channel()
        ch.queue_delete(queue=queue_name)
        conn.close()
    except Exception:
        pass


def test_1_single_task_end_to_end_execution(pipeline_env, caplog):
    """TEST 1: Single task end-to-end execution with structured logging verification."""
    coord: Coordinator = pipeline_env["coordinator"]
    agent_a: AgentA = pipeline_env["agent_a"]
    store: TaskStore = pipeline_env["store"]
    consumer_a: TaskConsumer = pipeline_env["consumer_a"]

    with caplog.at_level(logging.INFO):
        # 1. Coordinator creates task
        task = coord.create_task("calculate", {"a": 12, "b": 18})
        assert task.status == TaskStatus.PENDING

        # 2. Agent A consumes and executes task
        completed = agent_a.process_one(timeout=5.0)

    # Assert execution outcome
    assert completed is not None
    assert completed.task_id == task.task_id
    assert completed.status == TaskStatus.COMPLETED
    assert completed.result == 30
    assert completed.agent_id == "agent_a"

    # Assert queue is now empty
    assert consumer_a.consume_one(timeout=0.2) is None

    # Verify structured logging markers
    log_text = caplog.text
    assert f"[TASK_CREATED] task_id={task.task_id}" in log_text
    assert f"[TASK_PUBLISHED] task_id={task.task_id}" in log_text
    assert f"[TASK_RECEIVED] task_id={task.task_id} agent_id=agent_a" in log_text
    assert f"[TASK_PROCESSING] task_id={task.task_id} agent_id=agent_a" in log_text
    assert f"[TASK_COMPLETED] task_id={task.task_id} agent_id=agent_a result=30" in log_text
    assert f"[TASK_ACKED] task_id={task.task_id} agent_id=agent_a" in log_text


def test_2_multiple_tasks_with_multiple_workers(pipeline_env):
    """TEST 2: Multiple tasks distributed across multiple workers."""
    coord: Coordinator = pipeline_env["coordinator"]
    agent_a: AgentA = pipeline_env["agent_a"]
    agent_b: AgentB = pipeline_env["agent_b"]
    store: TaskStore = pipeline_env["store"]

    # Submit 6 tasks
    tasks = [
        coord.create_task("calculate", {"a": i, "b": i * 2})
        for i in range(1, 7)
    ]

    completed_tasks = []
    # Both workers pull tasks alternately
    for _ in range(3):
        res_a = agent_a.process_one(timeout=5.0)
        assert res_a is not None
        completed_tasks.append(res_a)

        res_b = agent_b.process_one(timeout=5.0)
        assert res_b is not None
        completed_tasks.append(res_b)

    assert len(completed_tasks) == 6

    # Verify distribution
    agent_a_tasks = [t for t in completed_tasks if t.agent_id == "agent_a"]
    agent_b_tasks = [t for t in completed_tasks if t.agent_id == "agent_b"]

    assert len(agent_a_tasks) == 3
    assert len(agent_b_tasks) == 3

    # Verify all calculations match
    for t in completed_tasks:
        expected = t.payload["a"] + t.payload["b"]
        assert t.result == expected
        assert t.status == TaskStatus.COMPLETED


def test_3_correct_task_state_transitions(pipeline_env):
    """TEST 3: Verify the strict lifecycle sequence PENDING -> PROCESSING -> COMPLETED."""
    coord: Coordinator = pipeline_env["coordinator"]
    store: TaskStore = pipeline_env["store"]
    consumer_a: TaskConsumer = pipeline_env["consumer_a"]

    observed_lifecycle = []

    def lifecycle_tracker(task: Task):
        midway = store.get_task(task.task_id)
        if midway:
            observed_lifecycle.append(midway.status)
        return task.payload["a"] + task.payload["b"]

    custom_agent = AgentA(
        task_store=store,
        consumer=consumer_a,
        executor=lifecycle_tracker,
    )

    # 1. State at submission: PENDING
    task = coord.create_task("calculate", {"a": 20, "b": 30})
    observed_lifecycle.append(store.get_task(task.task_id).status)

    # 2. Worker executes: PROCESSING observed midway
    custom_agent.process_one(timeout=5.0)

    # 3. State after completion: COMPLETED
    observed_lifecycle.append(store.get_task(task.task_id).status)

    assert observed_lifecycle == [
        TaskStatus.PENDING,
        TaskStatus.PROCESSING,
        TaskStatus.COMPLETED,
    ]


def test_4_correct_result_storage(pipeline_env):
    """TEST 4: Correct calculation result storage across various numeric inputs."""
    coord: Coordinator = pipeline_env["coordinator"]
    agent_a: AgentA = pipeline_env["agent_a"]
    store: TaskStore = pipeline_env["store"]

    cases = [
        ({"a": 100, "b": 200}, 300),
        ({"a": -50, "b": 20}, -30),
        ({"a": 1.5, "b": 2.25}, 3.75),
        ({"a": 0, "b": 0}, 0),
    ]

    for payload, expected_result in cases:
        task = coord.create_task("calculate", payload)
        agent_a.process_one(timeout=5.0)

        persisted = store.get_task(task.task_id)
        assert persisted is not None
        assert persisted.result == expected_result
        assert persisted.status == TaskStatus.COMPLETED


def test_5_correct_agent_id(pipeline_env):
    """TEST 5: Correct agent_id assignment for respective workers."""
    coord: Coordinator = pipeline_env["coordinator"]
    agent_a: AgentA = pipeline_env["agent_a"]
    agent_b: AgentB = pipeline_env["agent_b"]
    store: TaskStore = pipeline_env["store"]

    # Process task with Agent A
    task_a = coord.create_task("calculate", {"a": 1, "b": 1})
    agent_a.process_one(timeout=5.0)
    assert store.get_task(task_a.task_id).agent_id == "agent_a"

    # Process task with Agent B
    task_b = coord.create_task("calculate", {"a": 2, "b": 2})
    agent_b.process_one(timeout=5.0)
    assert store.get_task(task_b.task_id).agent_id == "agent_b"


def test_6_invalid_task_handling(pipeline_env):
    """TEST 6: Invalid task handling (FAILED state, error capture, and message rejected without requeue)."""
    coord: Coordinator = pipeline_env["coordinator"]
    agent_a: AgentA = pipeline_env["agent_a"]
    store: TaskStore = pipeline_env["store"]
    consumer_a: TaskConsumer = pipeline_env["consumer_a"]

    # Submit task with non-numeric inputs
    invalid_task = coord.create_task("calculate", {"a": "invalid", "b": 10})

    # Agent encounters ValueError during processing
    with pytest.raises(ValueError, match="Inputs 'a' and 'b' must be numbers"):
        agent_a.process_one(timeout=5.0)

    # Verify task state in Redis is FAILED and captures error details
    persisted = store.get_task(invalid_task.task_id)
    assert persisted is not None
    assert persisted.status == TaskStatus.FAILED
    assert persisted.result is None
    assert "must be numbers" in str(persisted.error)

    # Verify message was nack'd without requeue, so queue is not poisoned
    assert consumer_a.consume_one(timeout=0.2) is None


def test_7_rabbitmq_message_acknowledgement(pipeline_env):
    """TEST 7: Verify RabbitMQ message is acknowledged and removed from queue upon successful processing."""
    coord: Coordinator = pipeline_env["coordinator"]
    agent_a: AgentA = pipeline_env["agent_a"]
    consumer_a: TaskConsumer = pipeline_env["consumer_a"]

    # 1. Publish task
    task = coord.create_task("calculate", {"a": 7, "b": 8})

    # 2. Inspect queue: exactly 1 message is present
    msg_before = consumer_a.consume_one(timeout=5.0)
    assert msg_before is not None
    assert msg_before.task_id == task.task_id

    # Reject back into queue for worker to consume normally
    msg_before.nack(requeue=True)

    # 3. Agent processes task and issues manual ACK
    completed = agent_a.process_one(timeout=5.0)
    assert completed is not None
    assert completed.task_id == task.task_id

    # 4. Verify queue is completely empty (message was ACK'd)
    assert consumer_a.consume_one(timeout=0.3) is None


def test_8_redis_state_remains_consistent_after_successful_processing(pipeline_env):
    """TEST 8: Verify comprehensive Redis state consistency after task processing."""
    coord: Coordinator = pipeline_env["coordinator"]
    agent_a: AgentA = pipeline_env["agent_a"]
    store: TaskStore = pipeline_env["store"]

    original_payload = {"a": 42, "b": 58}
    task = coord.create_task("calculate", original_payload)

    completed = agent_a.process_one(timeout=5.0)
    assert completed is not None

    persisted = store.get_task(task.task_id)
    assert persisted is not None

    # Consistent identity and inputs
    assert persisted.task_id == task.task_id
    assert persisted.task_type == "calculate"
    assert persisted.payload == original_payload

    # Consistent lifecycle and results
    assert persisted.status == TaskStatus.COMPLETED
    assert persisted.agent_id == "agent_a"
    assert persisted.result == 100
    assert persisted.error is None

    # Consistent timestamps
    assert persisted.created_at is not None
    assert persisted.updated_at is not None
    assert persisted.updated_at >= persisted.created_at
