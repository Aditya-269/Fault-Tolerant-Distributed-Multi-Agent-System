# Fault-Tolerant Distributed Multi-Agent System

A distributed task-processing system built with Python, RabbitMQ, Redis, and Docker Compose.

> [!NOTE]
> **Week 3 Milestone Scope (COMPLETE)**:
> - **Week 1 (Complete)**: Foundation pipeline (Coordinator → RabbitMQ + Redis → Agent A & Agent B).
> - **Week 2 (Complete)**: Agent Liveness, Health Registry, Failure Detection, and Distributed Task Leases.
> - **Week 3 (Complete)**: Automatic Failure Recovery:
>   - Batch 1: Extended task lifecycle with `RECOVERABLE` state; `TaskRecovery` detection.
>   - Batch 2: `RecoveryManager` automatic requeueing to RabbitMQ preserving task IDs.
>   - Batch 3: Connected recovered tasks to workers with lease reacquisition and new agent attribution.
>   - Batch 4: Deterministic failure injection (`simulate_failure`) for controlled crash simulation.
>   - Batch 5: Recovery safety and observability metadata (`previous_agent_id`, timing, structured logs).
>   - Validation Batch: 19-step end-to-end chaos recovery validation and 10-run recovery experiment (100% success rate, 0.0257s average recovery duration).
> - **Strict Boundaries**: Idempotency keys, exactly-once execution, leader election, consensus, and AI/LLMs are intentionally omitted at this stage.

---

## Project Purpose

The goal of this system is to build an educational, clean, and robust distributed task execution system:
- **Decoupled Architecture**: Coordinators dispatch tasks without executing them; independent worker agents process tasks asynchronously.
- **Shared State**: Redis acts as the single source of truth for task metadata, lifecycle states, results, worker attribution, and agent liveness heartbeats.
- **Health Registry**: Maintains formal records for each worker agent with statuses:
  - `STARTING`: Agent initialized but heartbeat not yet established.
  - `HEALTHY`: Agent actively emitting heartbeats.
  - `SUSPECTED`: Agent heartbeat missed or overdue (future watchdog).
  - `FAILED`: Agent confirmed unreachable or crashed (future watchdog).
- **Durable Queueing**: RabbitMQ buffers tasks persistently in durable queues with manual message acknowledgements (`basic_ack` / `basic_nack`) and fair round-robin dispatching (`prefetch_count=1`).
- **Liveness Tracking**: Agents periodically refresh volatile heartbeat keys in Redis with time-to-live (TTL) expiration, enabling future watchdogs to identify crashed or stalled workers.

---

## Week 1 Architecture

```text
                Client / Caller
                       │
                       ▼
                  Coordinator
                       │
             ┌─────────┴─────────┐
             │                   │
             ▼                   ▼
     1. Store State       2. Dispatch ID
           Redis               RabbitMQ
   (status="PENDING")      (Queue: tasks_queue)
                                 │
                         ┌───────┴───────┐ (prefetch=1)
                         ▼               ▼
                      Agent A         Agent B
                 (id="agent_a")   (id="agent_b")
                         │               │
                         └───────┬───────┘
                                 │
                                 ▼
                     3. Update State in Redis
                    (PENDING → PROCESSING)
                                 │
                     4. Execute Deterministic Task
                               (a + b)
                                 │
                     5. Store Result in Redis
                    (PROCESSING → COMPLETED)
                                 │
                     6. Acknowledge to RabbitMQ
                            (basic_ack)
```

---

## Example Task Flow

1. **Task Submission**: Client calls `coordinator.create_task("calculate", {"a": 10, "b": 20})`.
2. **ID Generation**: Coordinator creates a unique UUID4 `task_id` (e.g., `5a306a31-e1a9-4a33-8ae9-a48311b1db73`).
3. **State Initialization**: Coordinator writes the task to Redis under key `task:<task_id>` with status `PENDING`.
4. **Queue Dispatch**: Coordinator publishes `{"task_id": "<task_id>"}` to RabbitMQ queue `tasks_queue` with persistent delivery.
5. **Worker Pickup**: RabbitMQ delivers the task message to an available worker (e.g., Agent A) governed by `prefetch_count=1`.
6. **Fetch State**: Agent reads the full payload from Redis using the received `task_id`.
7. **Transition to Processing**: Agent updates Redis status: `PENDING → PROCESSING` and records `agent_id = "agent_a"`.
8. **Deterministic Computation**: Agent executes the business logic: `10 + 20 = 30`.
9. **Store Result & Complete**: Agent writes `result = 30` and updates status: `PROCESSING → COMPLETED` in Redis.
10. **Acknowledge**: Agent sends manual `ACK` to RabbitMQ. The message is permanently removed from the queue.

---

## Week 2 Architecture: Liveness, Health & Distributed Leases

In Week 2, the system augments the Week 1 task processing pipeline with three distinct distributed primitives in Redis:

```text
                                  Redis
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
          ▼                         ▼                         ▼
  Heartbeats (TTL: 15s)    Health Registry           Task Leases (TTL: 30s)
  `heartbeat:<agent_id>`   `agent:health:<agent_id>`  `lease:<task_id>`
  "Is the agent alive?"    "What state is agent in?" "Which agent owns task?"
          │                         │                         │
     Agent A & B               Agent A & B                 Task 123
   Periodic refresh        STARTING / HEALTHY /       Atomic SET NX EX 30
   (interval: 5.0s)        SUSPECTED / FAILED         Renewed by worker (~10s)
                                                      Released on COMPLETED
```

### 1. Heartbeat Architecture
- **Purpose**: Answers *"Is the agent alive?"*
- **Mechanism**: Agents run a background daemon thread (`HeartbeatSender`) that periodically writes volatile keys to Redis:
  `heartbeat:<agent_id>` with a 15-second TTL (`HEARTBEAT_TTL=15`) refreshed every 5.0 seconds (`HEARTBEAT_INTERVAL=5.0`).
- **Failure Eviction**: If an agent process crashes or is killed, Redis automatically evicts the volatile key upon TTL expiration without requiring active cleanup.

### 2. Agent Health Registry
- **Purpose**: Tracks formal lifecycle records for registered worker agents.
- **States**:
  - `STARTING`: Agent registered upon process bootstrap; heartbeat not yet established.
  - `HEALTHY`: Agent actively emitting heartbeats.
  - `SUSPECTED`: Agent missed expected heartbeat (future watchdog).
  - `FAILED`: Agent confirmed unreachable or heartbeat expired.
- **Key Pattern**: `agent:health:<agent_id>` stores structured JSON records with `agent_id`, `status`, `last_heartbeat`, and `updated_at`.

### 3. Failure Detection Behavior
- **Component**: `FailureDetector` inspects Redis key existence (`heartbeat:<agent_id>`).
- **Rule**:
  - If `heartbeat:<agent_id>` exists in Redis -> `AgentStatus.HEALTHY`.
  - If `heartbeat:<agent_id>` has expired/missing -> `AgentStatus.FAILED`.
- **Registry Synchronization**: When evaluated, the detector updates the agent's persistent status in the health registry and emits the structured log marker `[FAILURE_DETECTOR]`.
- **Deterministic**: Relies strictly on Redis TTL expiration rather than local Python timers.

### 4. Task Lease Explanation
- **Purpose**: Answers *"Which agent currently owns this task?"*
- **Mechanism**:
  - Before transitioning a task to `PROCESSING`, the worker attempts an atomic lease acquisition:
    `SET lease:<task_id> <agent_id> NX EX 30`
  - **Mutual Exclusion**: Only one agent can acquire the lease. If Agent A owns the lease, Agent B cannot acquire it or overwrite Agent A's ownership.
  - **Safe Message Handling on Conflict**: If lease acquisition fails, the worker logs `[LEASE_CONFLICT]`, safely rejects the message via `msg.nack(requeue=False)` to prevent channel stalling and redelivery loops, and does NOT execute the task or overwrite task state.
  - **Periodic Renewal**: For long-running tasks, `LeaseRenewer` runs a background thread that periodically refreshes the 30-second TTL (every ~10 seconds) using an atomic Redis Lua script. Only the owning agent can renew the lease.
  - **Clean Release**: Upon task completion (`COMPLETED`) or failure (`FAILED`), the worker atomically deletes the lease using an owner-guarded Lua script (`RELEASE_LUA`).
  - **Post-Expiry Reacquisition**: If an agent crashes while processing, renewal ceases; after 30 seconds, Redis evicts `lease:<task_id>`, allowing future recovery to reassign or reacquire the lease.

### 5. Week 2 Limitations
> [!WARNING]
> **Week 2 Scope Boundaries & Intentional Limitations**:
> - **No Automatic Task Recovery**: Tasks orphaned by dead agents were not yet recovered in Week 2 (addressed in Week 3).
> - **No Automatic Requeueing**: Expired or failed task leases were not yet requeued back into RabbitMQ (addressed in Week 3).
> - **No Leader Election or Consensus**: The system does NOT yet elect coordinator leaders (e.g., Raft, Bully algorithm).
> - **No Idempotency Keys**: Duplicate task deduplication at coordinator level is deferred to Week 4.
> - **No AI / LLM Integrations**: Worker tasks remain simple and deterministic (`calculate`).

---

## WEEK 3: Automatic Failure Recovery

In Week 3, the distributed system introduces an end-to-end automatic failure detection and task recovery engine that seamlessly detects agent crashes, reclaims abandoned tasks, and safely routes them to replacement workers without data loss.

### Target Recovery Flow

```text
    Agent A (owns Task 123)
              │
              ▼
    Agent A Crashes / Fails
              │
              ▼
   Heartbeat Expires (TTL)
              │
              ▼
    Agent A Status = FAILED
              │
              ▼
    Lease Expires (TTL: 30s)
              │
              ▼
    Recovery Manager Scans
 (Task marked RECOVERABLE in Redis)
              │
              ▼
   Task Requeued to RabbitMQ
              │
              ▼
     Agent B (Replacement)
              │
              ▼
       Acquires New Lease
   (Task marked PROCESSING, Agent B)
              │
              ▼
    Executes Deterministic Task
              │
              ▼
    Task Marked COMPLETED
 (Result saved, duration recorded, lease released)
              │
              ▼
       Manual ACK to RabbitMQ
```

### 1. Failure Detection Flow
- The `FailureDetector` inspects `heartbeat:<agent_id>` in Redis using atomic TTL checks.
- When an agent process halts or crashes, its background heartbeat sender thread ceases emitting updates.
- Upon TTL expiration in Redis, the key is automatically evicted.
- `FailureDetector.is_healthy(agent_id)` returns `False`, and `check_agent(agent_id, update_registry=True)` updates the agent status in the `HealthRegistry` to `AgentStatus.FAILED`, emitting the `[AGENT_FAILED]` marker.

### 2. Lease Expiration
- While a task is `PROCESSING`, it is protected by an exclusive distributed lease: `lease:<task_id>` set to `<agent_id>`.
- While an agent is alive, `LeaseRenewer` periodically refreshes the TTL.
- When the agent crashes, renewal ceases immediately.
- Once the lease TTL expires (e.g., 30 seconds in production, 1.0s in tests), Redis automatically evicts `lease:<task_id>`.
- An orphaned task is only eligible for recovery if **both** conditions are met:
  1. The owning agent is confirmed `FAILED`.
  2. The task lease has strictly expired (`exists(task_id) == False`).
  This ensures that a slow or lagging agent whose lease is still active is **never** preempted or duplicate-processed.

### 3. Recoverable State
- `TaskRecoveryDetector` transitions orphaned `PROCESSING` tasks to the dedicated `TaskStatus.RECOVERABLE` state.
- **Recovery Metadata & Observability**:
  - `recovery_attempts`: Incremented by 1 upon each recovery attempt.
  - `previous_agent_id`: Stores the agent ID of the failed worker (e.g., `"agent_a"`).
  - `recovered_at`: Timestamp (ISO 8601) when the task entered `RECOVERABLE`.
  - `failure_detected_at`: Timestamp when the owning agent failure was confirmed.
  - `recovery_started_at`: Timestamp when the recovery pipeline commenced.
- **Loop Prevention**: Once a task enters `RECOVERABLE`, subsequent recovery scans ignore it, guaranteeing that tasks are never continuously or redundantly requeued while waiting in the broker.

### 4. Requeue Process
- `RecoveryManager` republishes the original `task_id` back into RabbitMQ (`tasks_queue`) with persistent delivery.
- The original `task_id` is strictly preserved—no duplicate tasks or alias IDs are generated.
- `RecoveryManager` acts purely as a coordinator: it never executes tasks directly and never pins tasks to a specific agent, allowing the queue's fair-dispatching mechanism (`prefetch_count=1`) to route the message to the next available healthy worker.
- Emits structured log markers: `[TASK_REQUEUED]` and `[TASK_RECOVERED]`.

### 5. Replacement Worker
- When an active, healthy replacement worker (e.g., Agent B) receives the requeued message:
  1. Reads the task payload and metadata from Redis.
  2. Calls `is_task_processable(task)` to confirm that the task is in `PENDING`, `RECOVERABLE`, or uncompleted state.
  3. Atomically acquires the distributed lease: `lease:<task_id>` -> `"agent_b"`.
  4. Emits `[TASK_RECOVERED_CLAIMED]`, attributing the new execution to `"agent_b"`.
  5. Updates task status to `PROCESSING` with `agent_id = "agent_b"`.
  6. Executes the task computation.
  7. Calculates `completed_at` and `recovery_duration` (`completed_at - recovery_started_at`).
  8. Persists the result and final `COMPLETED` status to Redis.
  9. Releases the lease and sends manual `ACK` to RabbitMQ.

### 6. Recovery Experiment
A repeatable 10-iteration chaos recovery experiment was executed via `scripts/run_week3_experiment.py` (and verified in `tests/test_week3_validation.py`), simulating controlled worker crashes under active lease ownership and validating end-to-end recovery by replacement workers.

#### Experiment Results Table

| Run | Task ID | Original Agent | Recovering Agent | Recovery Started At | Completed At | Recovery Duration | Status |
| :---: | :--- | :---: | :---: | :--- | :--- | :---: | :---: |
| 1 | `acda34f1...` | `agent_a` | `agent_b` | `09:41:00.714` | `09:41:00.736` | **0.0222s** | PASSED |
| 2 | `63806b58...` | `agent_a` | `agent_b` | `09:41:02.086` | `09:41:02.106` | **0.0199s** | PASSED |
| 3 | `7751438d...` | `agent_a` | `agent_b` | `09:41:03.458` | `09:41:03.476` | **0.0185s** | PASSED |
| 4 | `b6d55683...` | `agent_a` | `agent_b` | `09:41:04.827` | `09:41:04.853` | **0.0264s** | PASSED |
| 5 | `35bc28b0...` | `agent_a` | `agent_b` | `09:41:06.211` | `09:41:06.236` | **0.0254s** | PASSED |
| 6 | `7485814f...` | `agent_a` | `agent_b` | `09:41:07.592` | `09:41:07.617` | **0.0255s** | PASSED |
| 7 | `085e9425...` | `agent_a` | `agent_b` | `09:41:08.967` | `09:41:08.989` | **0.0222s** | PASSED |
| 8 | `9b694980...` | `agent_a` | `agent_b` | `09:41:10.344` | `09:41:10.391` | **0.0475s** | PASSED |
| 9 | `e4e18873...` | `agent_a` | `agent_b` | `09:41:11.757` | `09:41:11.780` | **0.0236s** | PASSED |
| 10 | `b97e3ca3...` | `agent_a` | `agent_b` | `09:41:13.140` | `09:41:13.166` | **0.0255s** | PASSED |

#### Summary Statistics
- **Total Recovery Runs**: 10
- **Successful Runs**: 10
- **Failed Runs**: 0
- **Recovery Success Rate**: **100.0%**
- **Average Recovery Duration**: **0.0257 seconds**
- **Min Recovery Duration**: **0.0185 seconds**
- **Max Recovery Duration**: **0.0475 seconds**
- **Average Total Run Time (including crash, TTL expiration & handoff)**: **1.38 seconds**

### 7. Limitations & Scope Boundaries
> [!IMPORTANT]
> **No Exactly-Once Execution Claim**:
> Distributed recovery guarantees *at-least-once* execution. Because a crashed worker may have partially executed code or reached external resources before halting, recovering tasks can result in duplicate attempts.
> 
> **Idempotency & Safe Retry Handling Deferred to Week 4**:
> Idempotency keys, deduplication caches, and deterministic side-effect suppression are scheduled specifically for the **Week 4** milestone.
> 
> Additional Week 3 boundaries maintained:
> - No consensus / distributed leader election (e.g. Raft).
> - No AI / LLM workflows.
> - No Kubernetes orchestration or external process supervisors.

---

## Structured Logging Markers

All core components emit structured log markers enabling observability across the lifecycle:

| Marker | Emitted By | Description |
| :--- | :--- | :--- |
| `[TASK_CREATED]` | Coordinator | Emitted when task is instantiated and saved to Redis |
| `[TASK_PUBLISHED]` | Coordinator | Emitted when task ID is published to RabbitMQ |
| `[TASK_RECEIVED]` | Agent | Emitted when worker receives message from RabbitMQ |
| `[TASK_PROCESSING]` | Agent | Emitted when task status is updated to `PROCESSING` |
| `[TASK_COMPLETED]` | Agent | Emitted when task calculation finishes and result is saved |
| `[TASK_ACKED]` | Agent | Emitted when manual `ACK` is sent to RabbitMQ |
| `[AGENT_REGISTERED]` | Health Registry | Emitted when an agent registers with the health registry |
| `[AGENT_HEALTHY]` | Health Registry | Emitted when an agent is marked `HEALTHY` upon heartbeat start |
| `[FAILURE_DETECTOR]` | Failure Detector | Emitted when failure detector evaluates and syncs agent health state |
| `[LEASE_ACQUIRED]` | Task Lease | Emitted when agent atomically acquires a lease on a task |
| `[LEASE_RENEWED]` | Task Lease | Emitted when agent owner renews lease expiration |
| `[LEASE_RENEWAL_STARTED]` | Lease Renewer | Emitted when periodic renewal background thread starts |
| `[LEASE_RENEWAL_STOPPED]` | Lease Renewer | Emitted when periodic renewal thread stops cleanly |
| `[LEASE_RELEASED]` | Task Lease | Emitted when agent owner releases lease upon task completion or failure |
| `[LEASE_CONFLICT]` | Worker | Emitted when worker skips processing because task is already leased |
| `[AGENT_FAILED]` | Recovery Manager / Failure Detector | Emitted when an agent is confirmed failed during health or task evaluation |
| `[TASK_RECOVERY_STARTED]` | Recovery Manager | Emitted when recovery process begins on an eligible orphaned task |
| `[TASK_RECOVERABLE]` | Task Recovery | Emitted when an orphaned PROCESSING task is identified and marked RECOVERABLE |
| `[TASK_REQUEUED]` | Recovery Manager | Emitted when a RECOVERABLE task ID is republished to RabbitMQ |
| `[TASK_RECOVERED]` | Recovery Manager | Emitted when a task is successfully transitioned and requeued |
| `[TASK_RECOVERED_CLAIMED]` | Worker | Emitted when a recovering worker claims a previously orphaned RECOVERABLE task |
| `[TASK_SKIPPED]` | Worker | Emitted when worker skips processing an unprocessable or already completed task |
| `[SIMULATED_FAILURE]` | Worker | Emitted when deterministic failure injection triggers during task execution |
| `[SIMULATED_CRASH]` | Worker | Emitted when a worker halts heartbeat and marks itself crashed |
| `[AGENT_CRASHED]` | Worker | Emitted when simulated crash halts worker leaving lease unreleased in Redis |
| `[RECOVERY_RUN_STARTED]` | Experiment | Emitted at the start of a recovery experiment iteration |
| `[RECOVERY_EXPERIMENT_RUN]` | Experiment | Emitted upon completion of a recovery run with duration and agent info |
| `[RECOVERY_EXPERIMENT_SUMMARY]` | Experiment | Emitted with aggregate statistics across multi-run recovery experiment |

---

## Directory Structure

```text
fault-tolerant-multi-agent/
├── coordinator/               # Task submission and coordinator logic
│   ├── __init__.py
│   ├── coordinator.py         # Coordinator class with structured logging
│   └── recovery.py            # RecoveryManager / TaskRecovery re-export
├── recovery/                  # Task recovery and automatic requeueing
│   ├── __init__.py
│   ├── recovery_manager.py    # RecoveryManager: detection & automatic requeueing to RabbitMQ
│   └── task_recovery.py       # TaskRecovery: failure & lease evaluation for RECOVERABLE state
├── agents/                    # Distributed worker agent implementations
│   ├── __init__.py
│   ├── agent_a.py             # Agent A independent worker entrypoint
│   ├── agent_b.py             # Agent B independent worker entrypoint
│   ├── executor.py            # Deterministic calculation engine
│   ├── failure_detector.py    # FailureDetector evaluating Redis heartbeat TTL expiration
│   ├── heartbeat.py           # HeartbeatSender background thread & liveness helpers
│   └── worker.py              # Base Worker consumer with manual ACK & heartbeat lifecycle
├── queue/                     # RabbitMQ broker abstraction
│   ├── __init__.py
│   ├── connection.py          # BlockingConnection factory
│   ├── message.py             # QueueMessage with encapsulated manual ack/nack
│   ├── publisher.py           # TaskPublisher (durable queue, persistent delivery)
│   └── consumer.py            # TaskConsumer (prefetch=1, manual ack)
├── state/                     # Redis state client & persistence logic
│   ├── __init__.py
│   ├── health_registry.py     # HealthRegistry (agent registration & health states)
│   ├── lease_renewer.py       # LeaseRenewer (background periodic lease TTL refresh)
│   ├── task_lease.py          # TaskLease (distributed task leases via SET NX EX & Lua)
│   └── task_store.py          # TaskStore (CRUD, status transitions, results/errors)
├── models/                    # Data schemas for tasks and messages
│   ├── __init__.py
│   ├── agent.py               # AgentHealth dataclass & AgentStatus enum
│   └── task.py                # Task dataclass & TaskStatus lifecycle enum
├── tests/                     # Test suite (112 unit & integration tests)
│   ├── __init__.py
│   ├── test_agent_a.py        # Agent A execution tests
│   ├── test_connectivity.py   # Infrastructure health & connectivity tests
│   ├── test_coordinator.py    # Coordinator dispatch tests
│   ├── test_failure_detector.py# Agent failure detection & heartbeat expiry tests
│   ├── test_failure_injection.py # Week 3 Batch 4 deterministic failure injection tests
│   ├── test_health_registry.py# Agent Health Registry tests
│   ├── test_heartbeat.py      # Heartbeat key, refresh, TTL, and expiry tests
│   ├── test_lease_renewal.py  # Periodic lease renewal tests
│   ├── test_multi_agent.py    # Multi-agent load distribution tests
│   ├── test_queue.py          # RabbitMQ queue abstraction tests
│   ├── test_recovery_manager.py# Week 3 Batch 2 automatic requeueing tests
│   ├── test_recovery_observability.py # Week 3 Batch 5 recovery safety & observability tests
│   ├── test_task_lease.py     # Distributed task lease unit tests
│   ├── test_task_lease_integration.py # Agent task lease flow integration tests
│   ├── test_task_model.py     # Task model serialization & UUID tests
│   ├── test_task_recovery.py  # Week 3 Batch 1 task recovery unit tests
│   ├── test_task_store.py     # Redis state store CRUD tests
│   ├── test_week1_pipeline.py # End-to-end integration tests (TEST 1 - 8)
│   ├── test_week2_validation.py # Final Week 2 validation test suite
│   ├── test_week3_validation.py # Final Week 3 validation & chaos recovery test suite
│   └── test_worker_recovery_integration.py # Week 3 Batch 3 recovered task worker integration tests
├── scripts/                   # Validation and experiment scripts
│   ├── manual_experiment.py   # Week 2 live multi-agent heartbeat failure experiment
│   └── run_week3_experiment.py# Week 3 10-iteration chaos recovery experiment
├── .env.example               # Example environment variables
├── .env                       # Local environment configuration
├── .gitignore                 # Git ignore rules
├── config.py                  # Centralized typed configuration module
├── docker-compose.yml         # Container configuration for RabbitMQ and Redis
├── requirements.txt           # Python dependencies (pika, redis, python-dotenv, pytest)
└── README.md                  # Project documentation
```

---

## Step-by-Step Setup Guide

### 1. Prerequisites

- **Python**: 3.11+ (tested on Python 3.14)
- **Docker & Docker Compose**: v2+

### 2. Configure Environment

Copy `.env.example` to `.env` (preconfigured with standard local defaults):

```bash
cp .env.example .env
```

### 3. Start RabbitMQ and Redis

Launch the infrastructure in background daemon mode:

```bash
docker compose up -d
```

Verify that both containers are running and healthy:

```bash
docker compose ps
```

- **RabbitMQ Management Dashboard**: [http://localhost:15672](http://localhost:15672) (Username: `guest`, Password: `guest`)
- **RabbitMQ AMQP Port**: `5672`
- **Redis Port**: `6379`

### 4. Setup Python Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Running the Components

Open separate terminal tabs for independent processes:

### Terminal 1: Run Agent A

```bash
source .venv/bin/activate
python agents/agent_a.py
```

### Terminal 2: Run Agent B

```bash
source .venv/bin/activate
python agents/agent_b.py
```

### Terminal 3: Dispatch Tasks via Coordinator

```bash
source .venv/bin/activate
python coordinator/coordinator.py
```

Or dispatch tasks interactively in Python:

```python
from coordinator import Coordinator

with Coordinator() as coord:
    task = coord.create_task("calculate", {"a": 15, "b": 25})
    print(f"Dispatched Task: {task.task_id}")
```

Watch Terminals 1 and 2 process tasks with structured log outputs:
```text
[TASK_RECEIVED] task_id=... agent_id=agent_a
[TASK_PROCESSING] task_id=... agent_id=agent_a
[TASK_COMPLETED] task_id=... agent_id=agent_a result=40
[TASK_ACKED] task_id=... agent_id=agent_a
```

---

## Running the Complete Test Suite

Execute the full pytest suite (112 unit and integration tests):

```bash
.venv/bin/pytest -v
```

To run the dedicated Week 3 final validation and chaos recovery test suite:

```bash
.venv/bin/pytest -v tests/test_week3_validation.py
```

To run all Week 3 recovery and observability tests:

```bash
.venv/bin/pytest -v tests/test_task_recovery.py tests/test_recovery_manager.py tests/test_worker_recovery_integration.py tests/test_failure_injection.py tests/test_recovery_observability.py tests/test_week3_validation.py
```

To run the dedicated Week 2 final validation suite:

```bash
.venv/bin/pytest -v tests/test_week2_validation.py
```

To run the dedicated task lease and renewal tests:

```bash
.venv/bin/pytest -v tests/test_task_lease.py tests/test_task_lease_integration.py tests/test_lease_renewal.py
```

To run the dedicated failure detector tests:

```bash
.venv/bin/pytest -v tests/test_failure_detector.py
```

To run the dedicated Week 1 pipeline integration tests:

```bash
.venv/bin/pytest -v tests/test_week1_pipeline.py
```

---

## Running the Week 3 Recovery Chaos Experiment

To execute the live 10-iteration automatic task recovery experiment (simulating crashes under active lease ownership, heartbeat expiration, failure detection, RECOVERABLE requeueing, and replacement worker completion):

```bash
python scripts/run_week3_experiment.py
```

---

## Running the Week 2 Live Manual Experiment

To execute the live multi-agent heartbeat failure and liveness validation experiment:

```bash
python scripts/manual_experiment.py
```

---

## Stopping Infrastructure

To stop and remove the Docker containers:

```bash
docker compose down
```
