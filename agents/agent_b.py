"""Independent worker process entrypoint for Agent B."""

import logging
from pathlib import Path
import sys

# Ensure project root is in sys.path for direct execution
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from agents.worker import Worker


class AgentB(Worker):
    """Concrete worker process instance for Agent B."""

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("agent_id", "agent_b")
        super().__init__(**kwargs)


def main() -> None:
    """Run Agent B as an independent worker process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [Agent B] %(message)s",
    )
    print("========================================")
    print(" Starting Worker Process: Agent B       ")
    print(" Worker ID: agent_b                     ")
    print(" Consuming from: tasks_queue            ")
    print(" Press Ctrl+C to terminate cleanly      ")
    print("========================================")

    with AgentB() as agent:
        agent.run()


if __name__ == "__main__":
    main()
