import json
import logging
import os
import signal
import threading

from src.book_filter import BookFilter
from common.logs import initialize_log
from common.health_check import HealthCheck


def main():
    initialize_log(os.getenv("LOGGING_LEVEL") or "INFO")
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_queues = json.loads(os.getenv("OUTPUT_QUEUES")) or []
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []

    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    cluster_size = json.loads(os.getenv("CLUSTER_SIZE") or '0')

    healthcheck_port = json.loads(os.getenv("HEALTHCHECK_PORT") or '8888')

    book_filter = BookFilter(input_queues, output_queues, output_exchanges, instance_id, cluster_size)
    healthcheck = HealthCheck(port=healthcheck_port)
    healthcheck_thread = threading.Thread(target=healthcheck.start, daemon=True)
    healthcheck_thread.start()
    signal.signal(signal.SIGTERM, lambda signum, frame: [method()
                  for method in [book_filter.shutdown, healthcheck.shutdown, healthcheck_thread.join]])
    book_filter.start()
    logging.info("Book filter stopped")


if __name__ == "__main__":
    main()
