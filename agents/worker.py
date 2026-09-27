"""Worker abstraction for distributed task consumers."""

import logging
import signal
import sys
from typing import Any, Callable, Optional

from agents.executor import execute_task
from models.task import Task, TaskStatus
from queue.consumer import TaskConsumer
from queue.message import QueueMessage
from state.task_store import TaskStore

logger = logging.getLogger(__name__)


class Worker:
    """Worker process consuming tasks from RabbitMQ and tracking state in Redis."""

    def __init__(
        self,
        agent_id: str,
        task_store: Optional[TaskStore] = None,
        consumer: Optional[TaskConsumer] = None,
        executor: Callable[[Task], Any] = execute_task,
    ) -> None:
        """Initialize the worker.

        Args:
            agent_id: Unique identifier for this worker agent (e.g. 'agent_a').
            task_store: Redis TaskStore instance (defaults to standard store).
            consumer: RabbitMQ TaskConsumer instance (defaults to standard consumer).
            executor: Callable that executes the task payload (defaults to execute_task).
        """
        self.agent_id = agent_id
        self.task_store = task_store or TaskStore()
        self._owns_consumer = consumer is None
        self.consumer = consumer or TaskConsumer(prefetch_count=1)
        self.executor = executor
        self._running = False

    def process_message(self, msg: QueueMessage) -> Optional[Task]:
        """Process a single queue message following the required lifecycle:

        1. Read task_id from message.
        2. Fetch task from Redis.
        3. Transition status PENDING -> PROCESSING.
        4. Set agent_id.
        5. Execute deterministic task.
        6. Store result in Redis and transition status PROCESSING -> COMPLETED.
        7. ACK RabbitMQ message only after successful processing.
        """
        task_id = msg.task_id
        logger.info(f"[TASK_RECEIVED] task_id={task_id} agent_id={self.agent_id}")
        task = self.task_store.get_task(task_id)

        if task is None:
            logger.warning(f"Task {task_id} not found in Redis. Acknowledging message to clear queue.")
            msg.ack()
            logger.info(f"[TASK_ACKED] task_id={task_id} agent_id={self.agent_id}")
            return None

        # 1. Update status to PROCESSING and record agent_id
        self.task_store.update_status(task_id, TaskStatus.PROCESSING)
        self.task_store.update_agent_id(task_id, self.agent_id)
        logger.info(f"[TASK_PROCESSING] task_id={task_id} agent_id={self.agent_id}")

        try:
            # 2. Execute deterministic task
            result = self.executor(task)

            # 3. Store result and transition to COMPLETED
            completed_task = self.task_store.store_result(
                task_id=task_id,
                result=result,
                status=TaskStatus.COMPLETED,
            )
            logger.info(
                f"[TASK_COMPLETED] task_id={task_id} agent_id={self.agent_id} result={result}"
            )

            # 4. ACK message only after successful processing
            msg.ack()
            logger.info(f"[TASK_ACKED] task_id={task_id} agent_id={self.agent_id}")
            return completed_task


        except Exception as exc:
            logger.error(f"Task {task_id} execution failed: {exc}", exc_info=True)
            self.task_store.store_error(
                task_id=task_id,
                error=str(exc),
                status=TaskStatus.FAILED,
            )
            # Rejection without requeue for bad payloads/errors
            msg.nack(requeue=False)
            raise

    def process_one(self, timeout: Optional[float] = 5.0) -> Optional[Task]:
        """Consume and process a single task within the timeout period."""
        msg = self.consumer.consume_one(timeout=timeout)
        if msg is None:
            return None
        return self.process_message(msg)

    def run(self, max_tasks: Optional[int] = None) -> None:
        """Run the worker loop continuously until stopped or max_tasks processed."""
        self._running = True
        processed_count = 0

        def _signal_handler(sig, frame):
            logger.info("Shutdown signal received. Stopping worker...")
            self._running = False
            self.consumer.stop_consuming()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        logger.info(f"Worker [{self.agent_id}] started. Waiting for tasks...")

        while self._running:
            if max_tasks is not None and processed_count >= max_tasks:
                break

            processed_task = self.process_one(timeout=1.0)
            if processed_task:
                processed_count += 1
                logger.info(
                    f"Worker [{self.agent_id}] completed task {processed_task.task_id} "
                    f"with result: {processed_task.result}"
                )

        logger.info(f"Worker [{self.agent_id}] shutdown complete.")

    def close(self) -> None:
        """Release consumer and network resources cleanly."""
        if self._owns_consumer and self.consumer:
            self.consumer.close()

    def __enter__(self) -> "Worker":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
