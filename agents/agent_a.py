"""Independent worker process entrypoint for Agent A."""

import logging
from pathlib import Path
import sys

# Ensure project root is in sys.path for direct execution
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from agents.worker import Worker


class AgentA(Worker):
    """Concrete worker process instance for Agent A."""

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("agent_id", "agent_a")
        super().__init__(**kwargs)


def main() -> None:
    """Run Agent A as an independent worker process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [Agent A] %(message)s",
    )
    print("========================================")
    print(" Starting Worker Process: Agent A       ")
    print(" Worker ID: agent_a                     ")
    print(" Consuming from: tasks_queue            ")
    print(" Press Ctrl+C to terminate cleanly      ")
    print("========================================")

    with AgentA() as agent:
        agent.run()


if __name__ == "__main__":
    main()
