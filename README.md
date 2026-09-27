# Fault-Tolerant Distributed Multi-Agent System (Week 1 Foundation)

A distributed task-processing system built with Python, RabbitMQ, Redis, and Docker Compose.

> [!IMPORTANT]
> **Week 1 Milestone Scope**: This repository represents the clean, working architectural foundation of a distributed task-processing system. Advanced mechanisms such as heartbeats, leases, watchdogs, automatic failure recovery, leader election, consensus algorithms, idempotency keys, and AI/LLM agents are intentionally omitted at this stage and belong to subsequent weeks. We do **not** claim complete fault tolerance yet.

---

## Project Purpose

The goal of Week 1 is to build an educational, clean, and robust distributed task execution pipeline:
- **Decoupled Architecture**: Coordinators dispatch tasks without executing them; independent worker agents process tasks asynchronously.
- **Shared State**: Redis acts as the single source of truth for task metadata, lifecycle states, results, and worker attribution.
- **Durable Queueing**: RabbitMQ ensures tasks are buffered persistently in durable queues with manual message acknowledgements (`basic_ack` / `basic_nack`) and fair round-robin dispatching (`prefetch_count=1`).

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
│   └── worker.py              # Base Worker consumer with manual ACK & lifecycle
├── queue/                     # RabbitMQ broker abstraction
│   ├── __init__.py
│   ├── connection.py          # BlockingConnection factory
│   ├── message.py             # QueueMessage with encapsulated manual ack/nack
│   ├── publisher.py           # TaskPublisher (durable queue, persistent delivery)
│   └── consumer.py            # TaskConsumer (prefetch=1, manual ack)
├── state/                     # Redis state client & persistence logic
│   ├── __init__.py
│   └── task_store.py          # TaskStore (CRUD, status transitions, results/errors)
├── models/                    # Data schemas for tasks and messages
│   ├── __init__.py
│   └── task.py                # Task dataclass & TaskStatus lifecycle enum
├── tests/                     # Test suite (38 unit & integration tests)
│   ├── __init__.py
│   ├── test_agent_a.py        # Agent A execution tests
│   ├── test_connectivity.py   # Infrastructure health & connectivity tests
│   ├── test_coordinator.py    # Coordinator dispatch tests
│   ├── test_multi_agent.py    # Multi-agent load distribution tests
│   ├── test_queue.py          # RabbitMQ queue abstraction tests
│   ├── test_task_model.py     # Task model serialization & UUID tests
│   ├── test_task_store.py     # Redis state store CRUD tests
│   └── test_week1_pipeline.py # End-to-end integration tests (TEST 1 - 8)
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

Execute the full pytest suite (38 unit and integration tests):

```bash
.venv/bin/pytest -v
```

To run the dedicated Week 1 pipeline integration tests:

```bash
.venv/bin/pytest -v tests/test_week1_pipeline.py
```

---

## Stopping Infrastructure

To stop and remove the Docker containers:

```bash
docker compose down
```
