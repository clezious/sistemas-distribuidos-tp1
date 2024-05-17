import json
import os
import signal
from src.output_boundary import OutputBoundary
from common.logs import initialize_log

DEFAULT_PORT = 12345
DEFAULT_LISTEN_BACKLOG = 5


def main():
    port = os.getenv("PORT", DEFAULT_PORT)
    listen_backlog = os.getenv("LISTEN_BACKLOG", DEFAULT_LISTEN_BACKLOG)
    result_queues = json.loads(os.getenv("RESULT_QUEUES"))
    initialize_log(os.getenv("LOGGING_LEVEL", "INFO"))
    output_boundary = OutputBoundary(port,
                                     listen_backlog,
                                     result_queues)
    signal.signal(signal.SIGTERM, lambda signum,
                  frame: output_boundary.shutdown())
    output_boundary.run()


if __name__ == "__main__":
    main()
