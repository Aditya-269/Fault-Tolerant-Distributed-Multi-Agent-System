# Fault-Tolerant Distributed Multi-Agent System

A distributed task-processing system built with Python, RabbitMQ, Redis, and Docker Compose.

> [!NOTE]
> **Week 2 Milestone Scope (Batch 6 Complete)**:
> - **Week 1 (Complete)**: Foundation pipeline (Coordinator → RabbitMQ + Redis → Agent A & Agent B).
> - **Week 2 Batch 1 (Complete)**: Reusable agent liveness heartbeat mechanism via Redis (`heartbeat:<agent_id>`) with automatic TTL expiration.
> - **Week 2 Batch 2 (Complete)**: Agent Health Registry tracking agent status (`STARTING`, `HEALTHY`, `SUSPECTED`, `FAILED`) and heartbeat timestamps in Redis.
> - **Week 2 Batch 3 (Complete)**: Basic agent failure detection (`FailureDetector`) based purely on Redis heartbeat key existence / TTL expiration.
> - **Week 2 Batch 4 (Complete)**: Distributed task leases (`TaskLease`) managing task ownership (`lease:<task_id> = agent_id`) with atomic NX EX acquisition, Lua-based renewal, and release.
> - **Week 2 Batch 5 (Complete)**: Integrated task leases into Agent A & Agent B worker flow (Acquire Lease → PROCESSING → Execute → COMPLETED → Release Lease → ACK).
> - **Week 2 Batch 6 (Complete)**: Background lease renewal (`LeaseRenewer`) periodically refreshing TTL (~10s interval for 30s lease) for long-running tasks.
> - **Strict Boundaries**: Automatic task recovery, requeuing failed tasks, leader election, consensus, and AI/LLM components are intentionally omitted at this stage.

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
> - **No Automatic Task Recovery**: Tasks orphaned by dead agents are NOT automatically reassigned or recovered yet.
> - **No Automatic Requeueing**: Expired or failed task leases are NOT automatically requeued back into RabbitMQ.
> - **No Leader Election or Consensus**: The system does NOT yet elect coordinator leaders (e.g., Raft, Bully algorithm).
> - **No Idempotency Keys**: Duplicate task deduplication at coordinator level is deferred to future milestones.
> - **No AI / LLM Integrations**: Worker tasks remain simple and deterministic (`calculate`).

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

---

## Directory Structure

```text
fault-tolerant-multi-agent/
├── coordinator/               # Task submission and coordinator logic
│   ├── __init__.py
│   └── coordinator.py         # Coordinator class with structured logging
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
├── tests/                     # Test suite (80 unit & integration tests)
│   ├── __init__.py
│   ├── test_agent_a.py        # Agent A execution tests
│   ├── test_connectivity.py   # Infrastructure health & connectivity tests
│   ├── test_coordinator.py    # Coordinator dispatch tests
│   ├── test_failure_detector.py# Agent failure detection & heartbeat expiry tests
│   ├── test_health_registry.py# Agent Health Registry tests
│   ├── test_heartbeat.py      # Heartbeat key, refresh, TTL, and expiry tests
│   ├── test_lease_renewal.py  # Periodic lease renewal tests
│   ├── test_multi_agent.py    # Multi-agent load distribution tests
│   ├── test_queue.py          # RabbitMQ queue abstraction tests
│   ├── test_task_lease.py     # Distributed task lease unit tests
│   ├── test_task_lease_integration.py # Agent task lease flow integration tests
│   ├── test_task_model.py     # Task model serialization & UUID tests
│   ├── test_task_store.py     # Redis state store CRUD tests
│   ├── test_week1_pipeline.py # End-to-end integration tests (TEST 1 - 8)
│   └── test_week2_validation.py # Final Week 2 validation test suite
├── scripts/                   # Validation and experiment scripts
│   └── manual_experiment.py   # Live multi-agent heartbeat failure experiment
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

Execute the full pytest suite (80 unit and integration tests):

```bash
.venv/bin/pytest -v
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
