from datetime import datetime, timezone
import logging
import signal
import sys
from typing import Any, Callable, Optional

from config import settings
from agents.executor import execute_task
from agents.heartbeat import HeartbeatSender
from models.task import Task, TaskStatus, _utc_now_iso
from queue.consumer import TaskConsumer
from queue.message import QueueMessage
from state.task_lease import TaskLease
from state.task_store import TaskStore

logger = logging.getLogger(__name__)


class SimulatedAgentCrash(Exception):
    """Exception raised when a worker intentionally simulates a crash for testing."""
    pass


class Worker:
    """Worker process consuming tasks from RabbitMQ and tracking state in Redis."""

    def __init__(
        self,
        agent_id: str,
        task_store: Optional[TaskStore] = None,
        consumer: Optional[TaskConsumer] = None,
        executor: Callable[[Task], Any] = execute_task,
        heartbeat_sender: Optional[HeartbeatSender] = None,
        enable_heartbeat: bool = True,
        health_registry: Optional[Any] = None,
        task_lease: Optional[TaskLease] = None,
        enable_lease: bool = True,
        lease_renewal_interval: Optional[float] = None,
        enable_failure_injection: bool = True,
        raise_on_crash: bool = False,
    ) -> None:
        """Initialize the worker.

        Args:
            agent_id: Unique identifier for this worker agent (e.g. 'agent_a').
            task_store: Redis TaskStore instance (defaults to standard store).
            consumer: RabbitMQ TaskConsumer instance (defaults to standard consumer).
            executor: Callable that executes the task payload (defaults to execute_task).
            heartbeat_sender: Optional custom HeartbeatSender instance.
            enable_heartbeat: Whether to enable agent liveness heartbeat (default: True).
            health_registry: Optional HealthRegistry instance for agent registration/liveness.
            task_lease: Optional TaskLease instance for distributed task leases.
            enable_lease: Whether to acquire/release distributed task leases (default: True).
            lease_renewal_interval: Interval in seconds for periodic lease renewal (default: 10.0s).
            enable_failure_injection: Whether to permit deterministic test failure injection (default: True).
            raise_on_crash: Whether process_one should re-raise SimulatedAgentCrash (default: False).
        """
        self.agent_id = agent_id
        self.task_store = task_store or TaskStore()
        self._owns_consumer = consumer is None
        self.consumer = consumer or TaskConsumer(prefetch_count=1)
        self.executor = executor
        self.enable_heartbeat = enable_heartbeat
        self.enable_lease = enable_lease
        self.enable_failure_injection = enable_failure_injection
        self.raise_on_crash = raise_on_crash
        self._crashed = False
        self.lease_renewal_interval = (
            lease_renewal_interval
            if lease_renewal_interval is not None
            else settings.lease.renewal_interval
        )

        if self.enable_heartbeat:
            self.heartbeat: Optional[HeartbeatSender] = (
                heartbeat_sender
                or HeartbeatSender(
                    agent_id=self.agent_id,
                    redis_client=self.task_store.redis,
                    health_registry=health_registry,
                )
            )
        else:
            self.heartbeat = None

        if self.enable_lease:
            if task_lease is not None:
                self.task_lease: Optional[TaskLease] = task_lease
            else:
                lease_prefix = (
                    f"{self.task_store.key_prefix}:lease"
                    if self.task_store.key_prefix != "task"
                    else settings.lease.prefix
                )
                self.task_lease = TaskLease(
                    redis_client=self.task_store.redis,
                    prefix=lease_prefix,
                )
        else:
            self.task_lease = None

        self._running = False


    def start_heartbeat(self) -> None:
        """Start background heartbeat thread if enabled."""
        if self.heartbeat and not self.heartbeat.is_running:
            self.heartbeat.start()

    def stop_heartbeat(self) -> None:
        """Stop background heartbeat thread cleanly."""
        if self.heartbeat and self.heartbeat.is_running:
            self.heartbeat.stop()


    def is_task_processable(self, task: Task) -> bool:
        """Verify whether a task is in a valid state to be processed or recovered.

        Processable states:
        - PENDING: Standard newly submitted task.
        - RECOVERABLE: Requeued task ready to be claimed by a healthy recovering agent.
        - PROCESSING: May be claimed if no active lease exists.

        Non-processable states:
        - COMPLETED: Already finished tasks must not be re-executed.
        - FAILED: Failed tasks without recovery must not be re-executed.
        """
        return task.status in (TaskStatus.PENDING, TaskStatus.RECOVERABLE, TaskStatus.PROCESSING)

    def process_message(self, msg: QueueMessage) -> Optional[Task]:
        """Process a single queue message following the required 10-step lifecycle:

        1. Read task from Redis.
        2. Verify it is recoverable/processable.
        3. Acquire the task lease.
        4. Set agent_id = self.agent_id.
        5. Set status = PROCESSING.
        6. Execute the deterministic task.
        7. Store result.
        8. Set status = COMPLETED.
        9. Release lease.
        10. ACK RabbitMQ.
        """
        task_id = msg.task_id
        logger.info(f"[TASK_RECEIVED] task_id={task_id} agent_id={self.agent_id}")

        # 1. Read task from Redis
        task = self.task_store.get_task(task_id)
        if task is None:
            logger.warning(f"Task {task_id} not found in Redis. Acknowledging message to clear queue.")
            msg.ack()
            logger.info(f"[TASK_ACKED] task_id={task_id} agent_id={self.agent_id}")
            return None

        # 2. Verify it is recoverable/processable
        if not self.is_task_processable(task):
            logger.info(
                f"[TASK_SKIPPED] task_id={task_id} status={task.status.value} "
                f"is not processable by agent {self.agent_id}. Acknowledging message."
            )
            msg.ack()
            logger.info(f"[TASK_ACKED] task_id={task_id} agent_id={self.agent_id}")
            return task if task.status == TaskStatus.COMPLETED else None

        # 3. Acquire distributed lease if enabled
        if self.enable_lease and self.task_lease:
            acquired = self.task_lease.acquire(task_id, self.agent_id)
            if not acquired:
                owner = self.task_lease.get_owner(task_id)
                logger.warning(
                    f"[LEASE_CONFLICT] task_id={task_id} agent_id={self.agent_id} "
                    f"owner={owner}. Skipping processing."
                )
                # Safely reject message without requeueing to avoid blocking worker prefetch
                msg.nack(requeue=False)
                return None

        # If task was RECOVERABLE, log the recovery claim
        if task.status == TaskStatus.RECOVERABLE:
            logger.info(
                f"[TASK_RECOVERED_CLAIMED] task_id={task_id} recovering_agent={self.agent_id} "
                f"previous_agent={task.agent_id}"
            )

        # 4. Set agent_id = self.agent_id
        self.task_store.update_agent_id(task_id, self.agent_id)

        # 5. Set status = PROCESSING
        updated_task = self.task_store.update_status(task_id, TaskStatus.PROCESSING)
        if updated_task is not None:
            task = updated_task
        logger.info(f"[TASK_PROCESSING] task_id={task_id} agent_id={self.agent_id}")

        # Start periodic lease renewal in background for long-running tasks
        renewer = None
        if self.enable_lease and self.task_lease:
            renewer = self.task_lease.create_renewer(
                task_id=task_id,
                agent_id=self.agent_id,
                interval=self.lease_renewal_interval,
            )
            renewer.start()

        try:
            # Deterministic test failure injection (AFTER status is PROCESSING and lease acquired)
            if self._should_simulate_failure(task):
                logger.warning(
                    f"[SIMULATED_FAILURE] Agent {self.agent_id} intentionally crashing "
                    f"while processing task_id={task_id}"
                )
                self.simulate_crash()
                if renewer:
                    renewer.stop()
                if msg:
                    try:
                        msg.nack(requeue=False)
                    except Exception:
                        pass
                raise SimulatedAgentCrash(
                    f"Agent {self.agent_id} simulated crash during task {task_id}"
                )

            # 6. Execute deterministic task
            result = self.executor(task)

            # Record recovery timing information if task was recovered
            completed_at = _utc_now_iso()
            recovery_duration = None
            if task.recovery_started_at:
                try:
                    start_dt = datetime.fromisoformat(task.recovery_started_at)
                    end_dt = datetime.fromisoformat(completed_at)
                    recovery_duration = max(0.0, round((end_dt - start_dt).total_seconds(), 4))
                except Exception:
                    pass

            # 7 & 8. Store result and set status = COMPLETED
            completed_task = self.task_store.store_result(
                task_id=task_id,
                result=result,
                status=TaskStatus.COMPLETED,
                completed_at=completed_at,
                recovery_duration=recovery_duration,
            )
            dur_info = f" recovery_duration={recovery_duration}s" if recovery_duration is not None else ""
            logger.info(
                f"[TASK_COMPLETED] task_id={task_id} agent_id={self.agent_id} result={result}{dur_info}"
            )

            # 9. Stop renewal and release task lease upon completion
            if renewer:
                renewer.stop()
            if self.enable_lease and self.task_lease:
                self.task_lease.release(task_id, self.agent_id)

            # 10. ACK message only after successful processing
            msg.ack()
            logger.info(f"[TASK_ACKED] task_id={task_id} agent_id={self.agent_id}")
            return completed_task

        except SimulatedAgentCrash:
            # Controlled test crash: leave task in PROCESSING with unreleased lease in Redis
            logger.info(
                f"[AGENT_CRASHED] Agent {self.agent_id} halted. "
                f"Task {task_id} remains in PROCESSING with active unreleased lease."
            )
            raise
        except Exception as exc:
            logger.error(f"Task {task_id} execution failed: {exc}", exc_info=True)
            if renewer:
                renewer.stop()
            self.task_store.store_error(
                task_id=task_id,
                error=str(exc),
                status=TaskStatus.FAILED,
            )
            # Release task lease upon failure
            if self.enable_lease and self.task_lease:
                self.task_lease.release(task_id, self.agent_id)
            # Rejection without requeue for bad payloads/errors
            msg.nack(requeue=False)
            raise

    def _should_simulate_failure(self, task: Task) -> bool:
        """Determine whether deterministic failure should be injected for this task.

        Failure injection is disabled by default for normal tasks:
        - Task payload must explicitly specify 'simulate_failure': True.
        - If payload specifies 'failure_agent' or 'simulate_failure_agent', it must match self.agent_id.
        - Worker attribute enable_failure_injection must be True.
        """
        if not getattr(self, "enable_failure_injection", True):
            return False

        payload = task.payload or {}
        if not payload.get("simulate_failure"):
            return False

        target_agent = payload.get("failure_agent") or payload.get("simulate_failure_agent")
        if target_agent is not None and target_agent != self.agent_id:
            return False

        return True

    def simulate_crash(self) -> None:
        """Simulate an immediate process crash for testing.

        Halts background heartbeat emission without deleting existing Redis keys,
        terminates worker loop, and records crashed state.
        """
        logger.warning(f"[SIMULATED_CRASH] Agent [{self.agent_id}] simulated crash triggered.")
        self.stop_heartbeat()
        self._running = False
        self._crashed = True

    def process_one(self, timeout: Optional[float] = 5.0) -> Optional[Task]:
        """Consume and process a single task within the timeout period."""
        msg = self.consumer.consume_one(timeout=timeout)
        if msg is None:
            return None
        try:
            return self.process_message(msg)
        except SimulatedAgentCrash:
            if getattr(self, "raise_on_crash", False):
                raise
            return None

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
        self.start_heartbeat()

        try:
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
        finally:
            self.stop_heartbeat()
            logger.info(f"Worker [{self.agent_id}] shutdown complete.")

    def close(self) -> None:
        """Release consumer, heartbeat, and network resources cleanly."""
        self.stop_heartbeat()
        if self._owns_consumer and self.consumer:
            self.consumer.close()

    def __enter__(self) -> "Worker":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

